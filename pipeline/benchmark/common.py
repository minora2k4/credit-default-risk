"""Hàm dùng chung của bộ benchmark. Chạy trên máy host, chỉ cần thư viện chuẩn của Python."""
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime

PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RESULTS_DIR = os.path.join(PIPELINE_DIR, "work", "benchmark")
MIN_HOST_AVAILABLE_MB = 800     # máy còn ít RAM hơn mức này thì dừng đo, tránh kernel kill Docker VM
STALL_SECONDS = 1800            # quá lâu không có tiến triển thì coi như treo và thoát, không chờ mãi
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
UNITS = {"B": 1, "KIB": 1024, "MIB": 1024 ** 2, "GIB": 1024 ** 3, "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}


_last_activity = time.time()


def log(message):
    global _last_activity
    _last_activity = time.time()
    print(f"{datetime.now():%H:%M:%S} {message}", flush=True)


def _watch_stall():
    while True:
        time.sleep(30)
        if time.time() - _last_activity > STALL_SECONDS:
            print(f"LỖI: {STALL_SECONDS}s không có tiến triển, thoát", flush=True)
            os._exit(4)


def run(command, check=True, capture=True, timeout=None, env=None):
    """Chạy lệnh trong thư mục pipeline/, trả về stdout (gộp cả stderr)."""
    merged_env = {**os.environ, **(env or {})}
    result = subprocess.run(
        command, cwd=PIPELINE_DIR, shell=isinstance(command, str), text=True, errors="replace", timeout=timeout, env=merged_env,
        stdout=subprocess.PIPE if capture else None, stderr=subprocess.STDOUT if capture else None,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Lệnh lỗi (mã {result.returncode}): {command}\n{(result.stdout or '')[-3000:]}")
    return result.stdout or ""


def read_dotenv(name):
    with open(os.path.join(PIPELINE_DIR, ".env"), encoding="utf-8") as env_file:
        for line in env_file:
            if line.startswith(f"{name}="):
                return line.strip().split("=", 1)[1]
    raise KeyError(name)


def host_available_mb():
    with open("/proc/meminfo", encoding="ascii") as meminfo:
        for line in meminfo:
            if line.startswith("MemAvailable"):
                return int(line.split()[1]) / 1024
    return float("inf")


def cpu_temperature():
    for zone in sorted(os.listdir("/sys/class/thermal")):
        try:
            with open(f"/sys/class/thermal/{zone}/type", encoding="ascii") as zone_type:
                if zone_type.read().strip() != "x86_pkg_temp":
                    continue
            with open(f"/sys/class/thermal/{zone}/temp", encoding="ascii") as temperature:
                return int(temperature.read()) / 1000
        except OSError:
            continue
    return None


def throttle_count():
    """Số lần CPU bị giảm xung vì nhiệt (cấp package), tích lũy từ lúc bật máy."""
    try:
        with open("/sys/devices/system/cpu/cpu0/thermal_throttle/package_throttle_count", encoding="ascii") as counter:
            return int(counter.read())
    except OSError:
        return 0


def _to_bytes(text):
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    return float(match.group(1)) * UNITS[match.group(2).upper()] if match else 0.0


class ResourceSampler:
    """Lấy mẫu CPU và RAM từng container bằng docker stats, kèm RAM còn lại và nhiệt độ của máy host."""

    def __init__(self):
        self.containers = {}
        self.min_host_available_mb = float("inf")
        self.max_temperature = 0.0
        self._process = None
        self._stop = threading.Event()

    def __enter__(self):
        self._throttle_start = throttle_count()
        self._process = subprocess.Popen(
            ["docker", "stats", "--format", "{{json .}}"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        threading.Thread(target=self._read_stats, daemon=True).start()
        threading.Thread(target=self._watch_host, daemon=True).start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._process.kill()
        self._process.wait(timeout=10)

    def _read_stats(self):
        for line in self._process.stdout:
            line = ANSI_ESCAPE.sub("", line).strip()
            if not line.startswith("{"):
                continue
            # container đang khởi động hoặc đang dừng có giá trị "--"; bỏ qua mẫu đó, luồng này phải đọc đến hết
            # vì docker stats sẽ bị chặn nếu không ai đọc stdout của nó
            try:
                sample = json.loads(line)
                cpu = float(sample["CPUPerc"].rstrip("%"))
                memory = _to_bytes(sample["MemUsage"].split("/")[0]) / 1024 ** 2
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
            self.containers.setdefault(sample["Name"], {"cpu": [], "mem": []})
            self.containers[sample["Name"]]["cpu"].append(cpu)
            self.containers[sample["Name"]]["mem"].append(memory)

    def _watch_host(self):
        while not self._stop.wait(1):
            available = host_available_mb()
            self.min_host_available_mb = min(self.min_host_available_mb, available)
            self.max_temperature = max(self.max_temperature, cpu_temperature() or 0)
            if available < MIN_HOST_AVAILABLE_MB:
                log(f"LỖI: máy chỉ còn {available:.0f} MB RAM, dừng benchmark để tránh hết RAM")
                subprocess.run("docker ps -q --filter name=bench- | xargs -r docker kill; "
                               "docker ps -q --filter name=spark-client | xargs -r docker kill", shell=True)
                os._exit(3)

    def summary(self):
        """Với mỗi container đang hoạt động: CPU trung bình, CPU cao nhất (100 = một nhân), RAM cao nhất (MB)."""
        containers = {}
        for name, samples in self.containers.items():
            cpu = samples["cpu"][1:] or samples["cpu"]     # mẫu đầu của docker stats là 0
            containers[name.replace("credit-", "").removesuffix("-1")] = {
                "cpu_mean": round(statistics.fmean(cpu), 1), "cpu_max": round(max(cpu), 1),
                "mem_max_mb": round(max(samples["mem"])),
            }
        return {"containers": containers, "host_available_min_mb": round(self.min_host_available_mb),
                "temperature_max_c": self.max_temperature, "throttle_events": throttle_count() - self._throttle_start}


class Results:
    """Kết quả lưu từng dòng JSON. Chạy lại thì bỏ qua các lượt đã có nên tiếp tục được sau khi bị ngắt."""

    def __init__(self, suite):
        os.makedirs(RESULTS_DIR, exist_ok=True)
        self.path = os.path.join(RESULTS_DIR, f"{suite}.jsonl")
        self.rows = []
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as results_file:
                self.rows = [json.loads(line) for line in results_file if line.strip()]
        self.done = {(row["experiment"], row["label"], row["repeat"]) for row in self.rows}

    def has(self, experiment, label, repeat):
        return (experiment, label, repeat) in self.done

    def add(self, experiment, label, repeat, **fields):
        row = {"experiment": experiment, "label": label, "repeat": repeat, "time": datetime.now().isoformat(timespec="seconds"), **fields}
        with open(self.path, "a", encoding="utf-8") as results_file:
            results_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.rows.append(row)
        self.done.add((experiment, label, repeat))
        return row

    def median(self, experiment, label, field):
        values = [row[field] for row in self.rows if row["experiment"] == experiment and row["label"] == label]
        return statistics.median(values) if values else None


def require_stack():
    threading.Thread(target=_watch_stall, daemon=True).start()
    if "credit-kafka-1" not in run("docker ps --format '{{.Names}}'"):
        sys.exit("LỖI: cụm chưa chạy, cần `make up` trước")
