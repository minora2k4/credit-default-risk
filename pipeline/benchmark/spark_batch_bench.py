"""Benchmark các job batch của Spark: bronze_to_silver, build_features, batch_score.

Mỗi lượt chạy đúng job của pipeline qua spark-submit, chỉ đổi cấu hình bằng --conf. Job tự kiểm tra số dòng và ROC-AUC,
nên lượt nào cho kết quả sai sẽ báo lỗi chứ không được tính. Số liệu lấy từ event log của Spark: thời gian ứng dụng,
tổng thời gian task, GC, shuffle, spill.

Chạy: make bench-batch  hoặc  python3 benchmark/spark_batch_bench.py --experiment cores
"""
import argparse
import json
import os
import time

from common import ResourceSampler, Results, log, require_stack, run

EVENT_LOG_DIR = "/benchmark/eventlogs"
JOBS = {"silver": "jobs/bronze_to_silver.py", "features": "jobs/build_features.py", "score": "jobs/batch_score.py"}
REPEATS = 3         # cấu hình mặc định
SWEEP_REPEATS = 2   # các điểm sweep; chênh lệch giữa các lượt ở mặc định chỉ dưới 5%
DEFAULT_WORKER = {"SPARK_WORKER_CORES": "4", "SPARK_WORKER_MEMORY": "3g"}

# (nhãn, cấu hình worker, cấu hình Spark). Thứ tự job theo luồng dữ liệu vì job sau đọc kết quả của job trước
CORE_POINTS = [
    ("1 core", DEFAULT_WORKER, {"spark.cores.max": "1"}),
    ("2 core", DEFAULT_WORKER, {"spark.cores.max": "2"}),
    ("4 core", DEFAULT_WORKER, {}),
    ("6 core", {"SPARK_WORKER_CORES": "6", "SPARK_WORKER_MEMORY": "3g"}, {}),
    ("8 core", {"SPARK_WORKER_CORES": "8", "SPARK_WORKER_MEMORY": "3g"}, {}),
]
MEMORY_POINTS = [
    ("executor 1g", DEFAULT_WORKER, {"spark.executor.memory": "1g"}),
    ("executor 3g", DEFAULT_WORKER, {"spark.executor.memory": "3g"}),
]
SHUFFLE_POINTS = [(f"shuffle {count}", DEFAULT_WORKER, {"spark.sql.shuffle.partitions": str(count)}) for count in (8, 24, 48, 96, 200)]
LAYOUT_POINTS = [("2 executor 2 core 1.5g", DEFAULT_WORKER, {"spark.executor.cores": "2", "spark.executor.memory": "1536m"})]
AQE_POINTS = [("AQE tắt", DEFAULT_WORKER, {"spark.sql.adaptive.enabled": "false"})]
ARROW_POINTS = [(f"arrow {size}", DEFAULT_WORKER, {"spark.sql.execution.arrow.maxRecordsPerBatch": str(size)}) for size in (1000, 50000)]

EXPERIMENTS = {
    "baseline": (["silver", "features", "score"], [("mặc định", DEFAULT_WORKER, {})]),
    "cores": (["silver", "features", "score"], CORE_POINTS),
    "memory": (["silver", "features", "score"], MEMORY_POINTS),
    "shuffle": (["features"], SHUFFLE_POINTS),
    "layout": (["silver", "features", "score"], LAYOUT_POINTS),
    "aqe": (["features"], AQE_POINTS),
    "arrow": (["score"], ARROW_POINTS),
}


def namenode(*args):
    return run(["docker", "compose", "exec", "-T", "namenode", "hdfs", *args])


def set_worker(worker):
    """Khởi động lại worker nếu cấu hình cần khác cấu hình đang chạy; lệnh up không làm gì nếu không đổi."""
    run(["docker", "compose", "up", "-d", "--wait", "spark-worker-1"], env=worker, timeout=180)
    time.sleep(5)     # chờ worker đăng ký với master


def latest_event_log():
    listing = namenode("dfs", "-ls", "-t", EVENT_LOG_DIR)
    paths = [line.split()[-1] for line in listing.splitlines() if EVENT_LOG_DIR in line]
    return paths[0] if paths else None


def parse_event_log(path):
    """Tổng hợp event log của một ứng dụng: thời gian và các chỉ số của toàn bộ task."""
    # Spark 4 mặc định rolling event log: mỗi ứng dụng là một thư mục chứa các file events_*
    text = run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-cat", f"{path}/events_*"])
    started = ended = None
    totals = {"run_ms": 0, "gc_ms": 0, "cpu_ns": 0, "spill_memory": 0, "spill_disk": 0, "shuffle_read": 0, "shuffle_write": 0,
              "input_bytes": 0, "output_bytes": 0, "tasks": 0, "failed_tasks": 0, "peak_jvm": 0}
    for line in text.splitlines():
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        kind = event.get("Event")
        if kind == "SparkListenerApplicationStart":
            started = event["Timestamp"]
        elif kind == "SparkListenerApplicationEnd":
            ended = event["Timestamp"]
        elif kind == "SparkListenerTaskEnd":
            totals["tasks"] += 1
            if event["Task Info"].get("Failed"):
                totals["failed_tasks"] += 1
            metrics = event.get("Task Metrics") or {}
            totals["run_ms"] += metrics.get("Executor Run Time", 0)
            totals["gc_ms"] += metrics.get("JVM GC Time", 0)
            totals["cpu_ns"] += metrics.get("Executor CPU Time", 0)
            totals["spill_memory"] += metrics.get("Memory Bytes Spilled", 0)
            totals["spill_disk"] += metrics.get("Disk Bytes Spilled", 0)
            read = metrics.get("Shuffle Read Metrics", {})
            totals["shuffle_read"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
            totals["shuffle_write"] += metrics.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
            totals["input_bytes"] += metrics.get("Input Metrics", {}).get("Bytes Read", 0)
            totals["output_bytes"] += metrics.get("Output Metrics", {}).get("Bytes Written", 0)
    return {
        "application_seconds": round((ended - started) / 1000, 1) if started and ended else None,
        "task_seconds": round(totals["run_ms"] / 1000, 1), "cpu_seconds": round(totals["cpu_ns"] / 1e9, 1), "gc_seconds": round(totals["gc_ms"] / 1000, 1),
        "spill_mb": round(totals["spill_disk"] / 1024 ** 2, 1), "shuffle_read_mb": round(totals["shuffle_read"] / 1024 ** 2, 1),
        "shuffle_write_mb": round(totals["shuffle_write"] / 1024 ** 2, 1), "input_mb": round(totals["input_bytes"] / 1024 ** 2, 1),
        "output_mb": round(totals["output_bytes"] / 1024 ** 2, 1), "tasks": totals["tasks"], "failed_tasks": totals["failed_tasks"],
    }


def run_job(job, conf):
    """Một lượt spark-submit; trả về số liệu. Job lỗi (kể cả không qua cổng kiểm tra ROC-AUC) sẽ ném ngoại lệ."""
    arguments = ["--conf", "spark.eventLog.enabled=true", "--conf", f"spark.eventLog.dir=hdfs://namenode:8020{EVENT_LOG_DIR}",
                 "--conf", "spark.eventLog.compress=false"]
    for key, value in conf.items():
        arguments += ["--conf", f"{key}={value}"]
    started = time.time()
    with ResourceSampler() as sampler:
        run(["docker", "compose", "--profile", "tools", "run", "--rm", "spark-client", "spark-submit", *arguments, JOBS[job]], timeout=5400)
    wall_seconds = round(time.time() - started, 1)
    resources = sampler.summary()
    worker = resources["containers"].get("spark-worker-1", {})
    return {"wall_seconds": wall_seconds, **parse_event_log(latest_event_log()),
            "worker_cpu_mean": worker.get("cpu_mean"), "worker_cpu_max": worker.get("cpu_max"), "worker_mem_max_mb": worker.get("mem_max_mb"),
            "host_available_min_mb": resources["host_available_min_mb"], "temperature_max_c": resources["temperature_max_c"], "throttle_events": resources["throttle_events"]}


def run_experiment(results, experiment, repeats):
    jobs, points = EXPERIMENTS[experiment]
    warmup = experiment == "baseline"
    for label, worker, conf in points:
        set_worker(worker)
        for job in jobs:
            key = f"{job} | {label}"
            pending = [repeat for repeat in range(repeats) if not results.has(experiment, key, repeat)]
            if not pending:
                continue
            if warmup:
                # lượt khởi động: nạp page cache của HDFS để các lượt đo so sánh được với nhau; không tính
                log(f"{experiment} {key}: lượt khởi động")
                run_job(job, conf)
            for repeat in pending:
                row = results.add(experiment, key, repeat, job=job, point=label, worker=worker, conf=conf, **run_job(job, conf))
                log(f"{experiment} {key} lượt {repeat}: {row['application_seconds']}s, task {row['task_seconds']}s, CPU {row['cpu_seconds']}s, GC {row['gc_seconds']}s, "
                    f"spill {row['spill_mb']} MB, shuffle {row['shuffle_write_mb']} MB, worker RAM {row['worker_mem_max_mb']} MB")
    set_worker(DEFAULT_WORKER)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", choices=[*EXPERIMENTS, "all"], default="all")
    parser.add_argument("--repeats", type=int, default=SWEEP_REPEATS)
    args = parser.parse_args()
    require_stack()
    namenode("dfs", "-mkdir", "-p", EVENT_LOG_DIR)
    results = Results("spark_batch")
    for experiment in (EXPERIMENTS if args.experiment == "all" else [args.experiment]):
        run_experiment(results, experiment, REPEATS if experiment == "baseline" else args.repeats)
    log("Hoàn tất benchmark Spark batch")


if __name__ == "__main__":
    os.environ.setdefault("COMPOSE_PROGRESS", "quiet")
    main()
