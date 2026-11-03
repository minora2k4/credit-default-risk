"""Benchmark Kafka bằng các công cụ chuẩn đi kèm Kafka, với payload là hồ sơ vay thật.

  sweep     đổi từng tham số của producer quanh cấu hình gốc, chạy hết công suất
  scale     1 đến 4 producer đồng thời, để tìm giới hạn của broker
  broker    đổi số luồng network và IO của broker
  latency   độ trễ producer theo tải (10% đến 90% công suất, cộng các mức tải thực tế của pipeline)
  consumer  thông lượng đọc của 1 và 3 consumer
  e2e       độ trễ từ lúc gửi đến lúc đọc được, khi không có tải
  app       producer Python thật của pipeline (simulator/producer.py) chạy hết công suất

Chạy: make bench-kafka  hoặc  python3 benchmark/kafka_bench.py --phase sweep
Mọi thứ ghi vào topic bench_*, xóa sau khi đo; topic của pipeline không bị đụng tới.
"""
import argparse
import gzip
import json
import os
import random
import re
import subprocess
import time

from common import PIPELINE_DIR, RESULTS_DIR, ResourceSampler, Results, log, read_dotenv, require_stack, run

BENCH_DIR = RESULTS_DIR
KAFKA_IMAGE = read_dotenv("KAFKA_IMAGE")
TOPIC = "bench_kafka"
CLIENT = "bench-kafka-client"
PAYLOAD_IN_CONTAINER = "/bench/payload_real.txt"
MEAN_PAYLOAD_BYTES = 7198
TARGET_SECONDS = 10
MAX_DISK_MB_PER_RUN = 2500      # giới hạn lượng ghi mỗi lượt để không đầy bộ nhớ Docker VM
MAX_RECORDS_PER_RUN = 1_500_000
REPEATS = 3

# cấu hình gốc tương đương producer của pipeline (librdkafka mặc định batch 1 MB; Java mặc định chỉ 16 KB)
BASE = {"acks": "all", "enable.idempotence": "true", "compression.type": "zstd", "linger.ms": "20", "batch.size": "1000000", "partitions": 3}
SWEEP = [
    ("gốc", {}),
    ("acks=1", {"acks": "1", "enable.idempotence": "false"}),
    ("acks=0", {"acks": "0", "enable.idempotence": "false"}),
    ("compression=none", {"compression.type": "none"}),
    ("compression=lz4", {"compression.type": "lz4"}),
    ("compression=snappy", {"compression.type": "snappy"}),
    ("compression=gzip", {"compression.type": "gzip"}),
    ("linger=0", {"linger.ms": "0"}),
    ("linger=5", {"linger.ms": "5"}),
    ("linger=50", {"linger.ms": "50"}),
    ("linger=100", {"linger.ms": "100"}),
    ("batch=16k", {"batch.size": "16384"}),
    ("batch=64k", {"batch.size": "65536"}),
    ("batch=256k", {"batch.size": "262144"}),
    ("partitions=1", {"partitions": 1}),
    ("partitions=6", {"partitions": 6}),
    ("partitions=12", {"partitions": 12}),
]
BROKER_THREADS = [("net=3,io=8", 3, 8), ("net=6,io=8", 6, 8), ("net=3,io=16", 3, 16), ("net=6,io=16", 6, 16), ("net=2,io=4", 2, 4)]
LOAD_FRACTIONS = [0.10, 0.25, 0.50, 0.75, 0.90]
PIPELINE_RATES = [150, 1000]    # hồ sơ/giây: tốc độ phát lại của pipeline và một mức cao hơn nhiều

PRODUCER_RE = re.compile(
    r"(\d+) (?:steady state )?records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, "
    r"([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th")


def ensure_payload():
    """Lấy mẫu 5,000 hồ sơ thật làm payload cho perf-test; lấy mẫu cố định seed nên chạy lại vẫn ra đúng file đó."""
    path = os.path.join(RESULTS_DIR, "payload_real.txt")
    if os.path.exists(path):
        return
    with gzip.open(os.path.join(PIPELINE_DIR, "work", "sim_pool", "real_test", "applications.jsonl.gz"), "rt", encoding="utf-8") as pool_file:
        lines = pool_file.readlines()
    with open(path, "w", encoding="utf-8") as payload_file:
        payload_file.writelines(random.Random(42).sample(lines, 5000))


def kafka_tool(tool, *args):
    return run(["docker", "compose", "exec", "-T", "kafka", f"/opt/kafka/bin/{tool}.sh", "--bootstrap-server", "kafka:9092", *map(str, args)])


def recreate_topic(partitions, topic=TOPIC):
    delete_topic(topic)
    kafka_tool("kafka-topics", "--create", "--topic", topic, "--partitions", partitions, "--replication-factor", 1,
               "--config", "max.message.bytes=5242880")


def delete_topic(topic=TOPIC):
    if topic in kafka_tool("kafka-topics", "--list").split():
        kafka_tool("kafka-topics", "--delete", "--topic", topic)
        for _ in range(30):
            if topic not in kafka_tool("kafka-topics", "--list").split():
                break
            time.sleep(1)
        time.sleep(1)


def disk_bytes(topic=TOPIC):
    """Tổng dung lượng file .log của topic; bỏ qua thư mục *-delete đang chờ xóa và file index cấp phát sẵn."""
    script = f"ls -d /var/lib/kafka/data/{topic}-* 2>/dev/null | grep -v delete | while read d; do du -b $d/*.log; done | cut -f1"
    output = run(["docker", "compose", "exec", "-T", "kafka", "sh", "-c", script], check=False)
    return sum(int(value) for value in output.split())


def client_command(entrypoint, *args):
    return ["docker", "run", "--rm", "--name", CLIENT, "--network", "credit-net", "--memory", "1500m",
            "-e", "KAFKA_HEAP_OPTS=-Xms512m -Xmx1g", "-v", f"{BENCH_DIR}:/bench:ro",
            "--entrypoint", f"/opt/kafka/bin/{entrypoint}.sh", KAFKA_IMAGE, *map(str, args)]


def producer_props(config):
    return [f"{key}={value}" for key, value in config.items() if key != "partitions"] + ["max.request.size=5242880"]


def run_producer(config, records, throughput=-1):
    """Một lượt kafka-producer-perf-test trên topic mới; 10% số record đầu là khởi động, không tính vào kết quả."""
    recreate_topic(config["partitions"])
    with ResourceSampler() as sampler:
        output = run(client_command(
            "kafka-producer-perf-test", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--num-records", records,
            "--warmup-records", max(records // 10, 1), "--throughput", throughput, "--payload-file", PAYLOAD_IN_CONTAINER,
            "--command-property", *producer_props(config)), timeout=900)
    matches = PRODUCER_RE.findall(output)
    if not matches:
        raise RuntimeError(f"Không đọc được kết quả:\n{output[-2000:]}")
    sent, rate, mb, avg, peak, p50, p95, p99, p999 = matches[-1]     # dòng cuối là kết quả ở trạng thái ổn định
    on_disk = disk_bytes()
    resources = sampler.summary()
    return {
        "records": int(records), "records_per_s": float(rate), "mb_per_s": float(mb), "latency_avg_ms": float(avg), "latency_max_ms": float(peak),
        "latency_p50_ms": int(p50), "latency_p95_ms": int(p95), "latency_p99_ms": int(p99), "latency_p999_ms": int(p999),
        "compression_ratio": round(records * MEAN_PAYLOAD_BYTES / on_disk, 2) if on_disk else None,
        "kafka_cpu_mean": resources["containers"].get("kafka", {}).get("cpu_mean"),
        "kafka_mem_max_mb": resources["containers"].get("kafka", {}).get("mem_max_mb"),
        "client_cpu_mean": resources["containers"].get(CLIENT, {}).get("cpu_mean"),
        "host_available_min_mb": resources["host_available_min_mb"], "temperature_max_c": resources["temperature_max_c"], "throttle_events": resources["throttle_events"],
    }


def records_for_full_load(config):
    """Đo thử ngắn để chọn số record sao cho lượt đo dài khoảng TARGET_SECONDS mà không ghi quá nhiều xuống đĩa."""
    trial = run_producer(config, 100_000)
    disk_mb_per_record = MEAN_PAYLOAD_BYTES / trial["compression_ratio"] / 1024 ** 2
    return int(min(trial["records_per_s"] * TARGET_SECONDS, MAX_DISK_MB_PER_RUN / disk_mb_per_record, MAX_RECORDS_PER_RUN)), trial


def phase_sweep(results):
    for label, change in SWEEP:
        config = {**BASE, **change}
        if all(results.has("sweep", label, repeat) for repeat in range(REPEATS)):
            continue
        records, trial = records_for_full_load(config)
        log(f"sweep {label}: đo thử {trial['records_per_s']:,.0f} record/s, nén {trial['compression_ratio']}x; mỗi lượt {records:,} record")
        for repeat in range(REPEATS):
            if results.has("sweep", label, repeat):
                continue
            row = results.add("sweep", label, repeat, config=config, **run_producer(config, records))
            log(f"  lượt {repeat}: {row['records_per_s']:,.0f} record/s, {row['mb_per_s']:.0f} MB/s, p99 {row['latency_p99_ms']} ms, "
                f"Kafka CPU {row['kafka_cpu_mean']}%, client CPU {row['client_cpu_mean']}%")
    delete_topic()


def phase_scale(results):
    """Nhiều producer chạy đồng thời để tìm giới hạn của broker: một tiến trình perf-test bị giới hạn bởi luồng nén của nó."""
    for clients in (1, 2, 3, 4):
        label = f"{clients} producer"
        for repeat in range(REPEATS):
            if results.has("scale", label, repeat):
                continue
            recreate_topic(BASE["partitions"])
            records = 1_000_000 // clients
            started = time.time()
            with ResourceSampler() as sampler:
                processes = []
                for index in range(clients):
                    command = client_command(
                        "kafka-producer-perf-test", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--num-records", records,
                        "--warmup-records", records // 10, "--throughput", -1, "--payload-file", PAYLOAD_IN_CONTAINER,
                        "--command-property", *producer_props(BASE))
                    command[command.index(CLIENT)] = f"{CLIENT}-{index}"
                    processes.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
                outputs = [process.communicate(timeout=600)[0] for process in processes]
            parsed = [PRODUCER_RE.findall(output)[-1] for output in outputs]
            on_disk = disk_bytes()
            resources = sampler.summary()
            kafka = resources["containers"].get("kafka", {})
            row = results.add(
                "scale", label, repeat, clients=clients, records_per_s=sum(float(item[1]) for item in parsed), mb_per_s=sum(float(item[2]) for item in parsed),
                latency_p50_ms=max(int(item[5]) for item in parsed), latency_p99_ms=max(int(item[7]) for item in parsed), wall_seconds=round(time.time() - started, 1),
                on_disk_mb=round(on_disk / 1024 ** 2), kafka_cpu_mean=kafka.get("cpu_mean"), kafka_cpu_max=kafka.get("cpu_max"), kafka_mem_max_mb=kafka.get("mem_max_mb"),
                host_available_min_mb=resources["host_available_min_mb"], temperature_max_c=resources["temperature_max_c"])
            log(f"scale {label} lượt {repeat}: tổng {row['records_per_s']:,.0f} record/s, {row['mb_per_s']:.0f} MB/s, p99 {row['latency_p99_ms']} ms, "
                f"Kafka CPU {row['kafka_cpu_mean']}% (đỉnh {row['kafka_cpu_max']}%)")
    delete_topic()


def reset_broker_threads():
    """Đưa số luồng broker về mặc định; lệnh báo lỗi nếu chưa từng đặt, trường hợp đó bỏ qua."""
    run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", "kafka:9092", "--alter", "--entity-type", "brokers",
         "--entity-default", "--delete-config", "num.network.threads,num.io.threads"], check=False)


def phase_broker(results):
    records, _ = records_for_full_load(BASE)
    try:
        for label, network_threads, io_threads in BROKER_THREADS:
            # Kafka chỉ cho đổi số luồng trong khoảng từ một nửa đến gấp đôi giá trị hiện tại, nên về mặc định trước
            reset_broker_threads()
            time.sleep(2)
            kafka_tool("kafka-configs", "--alter", "--entity-type", "brokers", "--entity-default", "--add-config",
                       f"num.network.threads={network_threads},num.io.threads={io_threads}")
            time.sleep(3)
            for repeat in range(REPEATS):
                if results.has("broker", label, repeat):
                    continue
                row = results.add("broker", label, repeat, config=BASE, **run_producer(BASE, records))
                log(f"broker {label} lượt {repeat}: {row['records_per_s']:,.0f} record/s, p99 {row['latency_p99_ms']} ms, Kafka CPU {row['kafka_cpu_mean']}%")
    finally:
        reset_broker_threads()
        delete_topic()


def phase_latency(results, experiments):
    """Độ trễ phải đo ở tải cố định: chạy hết công suất thì độ trễ chủ yếu là thời gian xếp hàng."""
    for experiment, config in experiments:
        capacity = results.median("sweep", "gốc", "records_per_s") if experiment == "latency_gốc" else None
        if capacity is None:
            records, _ = records_for_full_load(config)
            capacity = run_producer(config, records)["records_per_s"]
        log(f"{experiment}: công suất {capacity:,.0f} record/s")
        targets = [(f"{rate}/s", rate, 60) for rate in PIPELINE_RATES]
        targets += [(f"{int(fraction * 100)}%", int(capacity * fraction), TARGET_SECONDS) for fraction in LOAD_FRACTIONS]
        for label, rate, seconds in targets:
            for repeat in range(REPEATS):
                if results.has(experiment, label, repeat):
                    continue
                records = int(min(rate * seconds, MAX_RECORDS_PER_RUN))
                row = results.add(experiment, label, repeat, config=config, target_rate=rate, **run_producer(config, records, throughput=rate))
                log(f"{experiment} {label} lượt {repeat}: {row['records_per_s']:,.0f} record/s, p50 {row['latency_p50_ms']} ms, "
                    f"p95 {row['latency_p95_ms']} ms, p99 {row['latency_p99_ms']} ms")
    delete_topic()


def consumer_once(group, records, extra):
    return client_command(
        "kafka-consumer-perf-test", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--num-records", records, "--group", group,
        "--timeout", 120000, *extra)


def parse_consumer(output):
    line = [text for text in output.strip().splitlines() if re.match(r"\d{4}-\d{2}-\d{2}", text)][-1]
    fields = [field.strip() for field in line.split(",")]
    return {"mb": float(fields[2]), "mb_per_s": float(fields[3]), "records": int(fields[4]), "records_per_s": float(fields[5]),
            "rebalance_ms": int(fields[6]), "fetch_ms": int(fields[7]), "fetch_mb_per_s": float(fields[8]), "fetch_records_per_s": float(fields[9])}


def phase_consumer(results):
    records = 1_000_000
    recreate_topic(3)
    fill = run_producer_keep(BASE, records)
    log(f"Đã nạp {records:,} record ({fill['mb_per_s']:.0f} MB/s), trên đĩa {disk_bytes() / 1024 ** 2:,.0f} MB")
    large_fetch = ["--command-property", "fetch.max.bytes=104857600", "max.partition.fetch.bytes=10485760", "max.poll.records=2000"]
    for label, extra in [("mặc định", []), ("fetch_lớn", large_fetch)]:
        for repeat in range(REPEATS):
            if results.has("consumer_1", label, repeat):
                continue
            with ResourceSampler() as sampler:
                output = run(consumer_once(f"bench-{label}-{repeat}-{time.time_ns()}", records, extra), timeout=300)
            row = results.add("consumer_1", label, repeat, **parse_consumer(output), resources=sampler.summary())
            log(f"consumer_1 {label} lượt {repeat}: {row['fetch_records_per_s']:,.0f} record/s, {row['fetch_mb_per_s']:.0f} MB/s (không tính rebalance)")
    # 3 consumer cùng group, mỗi consumer một partition
    for repeat in range(REPEATS):
        if results.has("consumer_3", "mặc định", repeat):
            continue
        group = f"bench-three-{repeat}-{time.time_ns()}"
        share = records // 3 - 1000     # hơi ít hơn phần của mỗi partition để cả 3 cùng kết thúc, không chờ timeout
        started = time.time()
        with ResourceSampler() as sampler:
            processes = []
            for index in range(3):
                command = consumer_once(group, share, [])
                command[command.index(CLIENT)] = f"{CLIENT}-{index}"
                processes.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
            outputs = [process.communicate(timeout=300)[0] for process in processes]
        parsed = [parse_consumer(output) for output in outputs]
        row = results.add("consumer_3", "mặc định", repeat, wall_seconds=round(time.time() - started, 1),
                          fetch_records_per_s=sum(item["fetch_records_per_s"] for item in parsed),
                          fetch_mb_per_s=sum(item["fetch_mb_per_s"] for item in parsed), resources=sampler.summary())
        log(f"consumer_3 lượt {repeat}: tổng {row['fetch_records_per_s']:,.0f} record/s, {row['fetch_mb_per_s']:.0f} MB/s")
    delete_topic()


def run_producer_keep(config, records):
    """Nạp dữ liệu vào topic đang có, không tạo lại."""
    output = run(client_command(
        "kafka-producer-perf-test", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--num-records", records, "--throughput", -1,
        "--payload-file", PAYLOAD_IN_CONTAINER, "--command-property", *producer_props(config)), timeout=900)
    _, rate, mb, *_ = PRODUCER_RE.findall(output)[-1]
    return {"records_per_s": float(rate), "mb_per_s": float(mb)}


def phase_e2e(results):
    recreate_topic(1)
    for label, acks in [("acks=all", "all"), ("acks=1", "1")]:
        for repeat in range(REPEATS):
            if results.has("e2e", label, repeat):
                continue
            output = run(client_command("kafka-e2e-latency", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--num-records", 20000,
                                        "--producer-acks", acks, "--record-size", MEAN_PAYLOAD_BYTES), timeout=600)
            average = float(re.search(r"Avg latency: ([\d.]+)", output).group(1))
            p50, p99, p999 = re.search(r"50th = (\d+), 99th = (\d+), 99\.9th = (\d+)", output).groups()
            row = results.add("e2e", label, repeat, latency_avg_ms=average, latency_p50_ms=int(p50), latency_p99_ms=int(p99), latency_p999_ms=int(p999))
            log(f"e2e {label} lượt {repeat}: trung bình {row['latency_avg_ms']} ms, p50 {p50}, p99 {p99}, p99.9 {p999}")
    delete_topic()


def phase_app(results):
    """Producer Python thật gửi cả pool 55,644 hồ sơ, cộng nhãn, với tốc độ tối đa."""
    for source in ("real-test", "synthetic"):
        for repeat in range(REPEATS):
            if results.has("app", source, repeat):
                continue
            recreate_topic(3, "bench_applications")
            recreate_topic(3, "bench_outcomes")
            with ResourceSampler() as sampler:
                output = run(["docker", "compose", "--profile", "sim", "run", "--rm", "--name", "bench-app-producer",
                              "-e", "BENCH_APPLICATIONS_TOPIC=bench_applications", "-e", "BENCH_OUTCOMES_TOPIC=bench_outcomes",
                              "simulator", "python", "benchmark/producer_job.py", "--source", source, "--rate", "1000000", "--limit", "0", "--outcome-delay", "0"], timeout=600)
            match = re.search(r"Hoàn tất: ([\d,]+) hồ sơ và ([\d,]+) nhãn trong ([\d.]+)s \(gửi hồ sơ ([\d.]+)s, (\d+) hồ sơ/s\), message lớn nhất ([\d.]+) KB, Kafka xác nhận ([\d,]+), lỗi (\d+)", output)
            if not match:
                raise RuntimeError(f"Không đọc được kết quả:\n{output[-1500:]}")
            applications = int(match.group(1).replace(",", ""))
            row = results.add("app", source, repeat, applications=applications, total_seconds=float(match.group(3)), records_per_s=int(match.group(5)),
                              confirmed=int(match.group(7).replace(",", "")), errors=int(match.group(8)), largest_kb=float(match.group(6)),
                              on_disk_mb=round(disk_bytes("bench_applications") / 1024 ** 2), resources=sampler.summary())
            log(f"app {source} lượt {repeat}: {row['records_per_s']:,} hồ sơ/s, tổng {row['total_seconds']}s, trên đĩa {row['on_disk_mb']} MB, lỗi {row['errors']}")
    delete_topic("bench_applications")
    delete_topic("bench_outcomes")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["sweep", "scale", "broker", "latency", "consumer", "e2e", "app", "all"], default="all")
    parser.add_argument("--latency-config", help="JSON cấu hình thêm cho đường cong độ trễ, ví dụ thử cấu hình đã tối ưu")
    args = parser.parse_args()
    require_stack()
    ensure_payload()
    results = Results("kafka")
    if args.phase in ("sweep", "all"):
        phase_sweep(results)
    if args.phase in ("scale", "all"):
        phase_scale(results)
    if args.phase in ("broker", "all"):
        phase_broker(results)
    if args.phase in ("latency", "all"):
        experiments = [("latency_gốc", BASE)]
        if args.latency_config:
            experiments.append(("latency_tối_ưu", {**BASE, **json.loads(args.latency_config)}))
        phase_latency(results, experiments)
    if args.phase in ("consumer", "all"):
        phase_consumer(results)
    if args.phase in ("e2e", "all"):
        phase_e2e(results)
    if args.phase in ("app", "all"):
        phase_app(results)
    log("Hoàn tất benchmark Kafka")


if __name__ == "__main__":
    main()
