"""Benchmark Spark Structured Streaming trên đúng logic của jobs/stream_score.py.

  capacity   topic đã có sẵn nhiều hồ sơ, job chạy hết tốc độ: đo số hồ sơ/giây tối đa theo kích thước micro-batch và tài nguyên
  sustained  producer gửi đều ở một tốc độ cố định: đo độ trễ end-to-end và kiểm tra pipeline có theo kịp không

Độ trễ end-to-end tính từ event_time (producer đóng dấu ngay trước khi gửi) đến lúc dòng được ghi vào Postgres. Thời điểm
ghi lấy từ cột committed_at chỉ có trong database benchmark (DEFAULT clock_timestamp()), vì cột scored_at của pipeline
được Spark chốt lúc bắt đầu tính toán, trước khi đặc trưng và điểm được tính xong.

Dùng topic bench_*, database credit_bench và HDFS /benchmark/stream; không đụng tới dữ liệu của pipeline.
Chạy: make bench-stream  hoặc  python3 benchmark/stream_bench.py --experiment capacity
"""
import argparse
import json
import statistics
import subprocess
import threading
import time

from common import PIPELINE_DIR, ResourceSampler, Results, log, require_stack, run

APPLICATIONS_TOPIC = "bench_applications"
OUTCOMES_TOPIC = "bench_outcomes"
BENCH_ROOT = "/benchmark/stream"
BENCH_DB = "credit_bench"
POOL_ROWS = 55_644
CAPACITY_ROWS = 16_000
REPEATS = 2

# cấu hình trước khi tối ưu, giữ cố định để so sánh; cấu hình đang chạy thật nằm trong jobs/stream_score.py
BASELINE = {"trigger": "10 seconds", "max_per_batch": 2000, "cores": 2, "executor_memory": "1g", "shuffle_partitions": 8}
PLAN_STEPS = ("clean_application", "clean_bureau", "clean_bureau_balance", "application_features", "bureau_balance_loan_features",
              "bureau_customer_features", "merge_and_fill", "add_scores")
AS_FAST_AS_POSSIBLE = "1 second"     # batch nào cũng lâu hơn 1 giây nên trigger không phải giới hạn; hết hàng thì chỉ hỏi Kafka mỗi giây một lần
SWEEP_BATCH = [500, 1000, 2000, 4000, 8000, 16000]
SWEEP_CORES = [1, 2, 3, 4]
SWEEP_SHUFFLE = [4, 8, 16]
SWEEP_MEMORY = ["1g", "2g"]


AQE_OFF = {"spark.sql.adaptive.enabled": "false"}
TUNE_POINTS = [
    ("đối chứng", {}),
    ("AQE tắt", {"spark_conf": AQE_OFF}),
    ("AQE tắt, shuffle 4", {"shuffle_partitions": 4, "spark_conf": AQE_OFF}),
    ("locality 0", {"spark_conf": {"spark.locality.wait": "0"}}),
    ("codegen tắt", {"spark_conf": {"spark.sql.codegen.wholeStage": "false"}}),
    ("batch 8000, core 2", {"max_per_batch": 8000}),
    ("batch 8000, core 4", {"max_per_batch": 8000, "cores": 4}),
    ("batch 16000, core 4", {"max_per_batch": 16000, "cores": 4}),
]
TUNED = {"max_per_batch": 8000, "shuffle_partitions": 4}
# (tên thí nghiệm, cấu hình, các tốc độ đo một lượt, các tốc độ đo lặp ba lượt)
SUSTAINED_PLANS = [
    ("sustained_gốc", {}, [50, 100, 190, 250], [150]),
    ("sustained_t5_m8000", {**TUNED, "trigger": "5 seconds"}, [300], [150]),
    ("sustained_t6_m8000", {**TUNED, "trigger": "6 seconds"}, [50, 450, 550, 650], [150, 300]),
    ("sustained_t8_m8000", {**TUNED, "trigger": "8 seconds"}, [300], [150]),
    ("sustained_t10_m16000", {"max_per_batch": 16000, "shuffle_partitions": 4}, [150, 600, 900, 1100, 1300], []),
]
SUSTAINED_SECONDS = 120


def kafka_tool(tool, *args):
    return run(["docker", "compose", "exec", "-T", "kafka", f"/opt/kafka/bin/{tool}.sh", "--bootstrap-server", "kafka:9092", *map(str, args)])


def recreate_topics():
    existing = kafka_tool("kafka-topics", "--list").split()
    for topic in (APPLICATIONS_TOPIC, OUTCOMES_TOPIC):
        if topic in existing:
            kafka_tool("kafka-topics", "--delete", "--topic", topic)
    for _ in range(30):
        existing = kafka_tool("kafka-topics", "--list").split()
        if APPLICATIONS_TOPIC not in existing and OUTCOMES_TOPIC not in existing:
            break
        time.sleep(1)
    time.sleep(1)
    kafka_tool("kafka-topics", "--create", "--topic", APPLICATIONS_TOPIC, "--partitions", 3, "--replication-factor", 1, "--config", "max.message.bytes=5242880")
    kafka_tool("kafka-topics", "--create", "--topic", OUTCOMES_TOPIC, "--partitions", 3, "--replication-factor", 1)


def psql(sql, database=BENCH_DB):
    return run(["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "credit", "-d", database, "-t", "-A", "-F", ",", "-c", sql]).strip()


def prepare_database():
    if BENCH_DB not in psql("SELECT datname FROM pg_database", database="credit").split():
        psql(f"CREATE DATABASE {BENCH_DB}", database="credit")
        run(f"docker compose exec -T postgres psql -q -U credit -d {BENCH_DB} -v ON_ERROR_STOP=1 < docker/postgres/init/01_schema.sql")
        psql("ALTER TABLE loan_predictions ADD COLUMN committed_at timestamptz DEFAULT clock_timestamp()")


def reset_run_state(checkpoint):
    """Xóa kết quả của lượt trước; topic giữ nguyên nếu dùng lại được."""
    psql("TRUNCATE loan_predictions, loan_outcomes")
    run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash",
         f"{BENCH_ROOT}/bronze", f"{BENCH_ROOT}/gold", f"{BENCH_ROOT}/stop", checkpoint], check=False)


def signal_stop():
    run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-touchz", f"{BENCH_ROOT}/stop"])


def producer_command(rate, limit, outcome_delay):
    # pool thật có 55,644 hồ sơ; cần nhiều hơn thì dùng pool sinh, phát lại với ID mới (message nhỏ hơn khoảng 25%)
    source = ["--source", "real-test"] if limit <= POOL_ROWS else ["--source", "synthetic", "--forever"]
    return ["docker", "compose", "--profile", "sim", "run", "--rm", "--name", "bench-stream-producer",
            "-e", f"BENCH_APPLICATIONS_TOPIC={APPLICATIONS_TOPIC}", "-e", f"BENCH_OUTCOMES_TOPIC={OUTCOMES_TOPIC}",
            "simulator", "python", "benchmark/producer_job.py", *source, "--rate", str(rate), "--limit", str(limit), "--outcome-delay", str(outcome_delay)]


def start_job(config, checkpoint, stop_after_rows=0):
    command = ["docker", "compose", "--profile", "stream", "run", "--rm", "--no-deps", "--name", "bench-stream-job", "-e", f"POSTGRES_DB={BENCH_DB}",
               "spark-stream", "spark-submit", "/opt/pipeline/benchmark/stream_job.py",
               "--trigger", config["trigger"], "--max-per-batch", str(config["max_per_batch"]), "--cores", str(config["cores"]),
               "--executor-memory", config["executor_memory"], "--shuffle-partitions", str(config["shuffle_partitions"]),
               "--spark-conf", json.dumps(config.get("spark_conf", {})),
               "--checkpoint", checkpoint, "--stop-after-rows", str(stop_after_rows)]
    process = subprocess.Popen(command, cwd=PIPELINE_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    state = {"progress": [], "steps": [], "ready": threading.Event(), "done": threading.Event(), "tail": []}

    def read_output():
        for line in process.stdout:
            if line.startswith("BENCH_PROGRESS "):
                state["progress"].append(json.loads(line.split(" ", 1)[1]))
            elif line.startswith("BENCH_STEP "):
                state["steps"].append(json.loads(line.split(" ", 1)[1]))
            elif line.startswith("BENCH_READY "):
                state["ready"].set()
            elif line.startswith("BENCH_DONE "):
                state["done"].set()
            else:
                state["tail"] = (state["tail"] + [line.rstrip()])[-40:]
    thread = threading.Thread(target=read_output, daemon=True)
    thread.start()
    return process, state, thread


def batch_stats(state, skip_batches=2):
    """Thống kê các micro-batch của query hồ sơ, bỏ vài batch đầu vì JVM và session ONNX còn đang khởi động."""
    batches = [item for item in state["progress"] if item["query"] == "applications" and item["rows"] > 0][skip_batches:]
    if not batches:
        return {}
    durations = [item["duration"]["triggerExecution"] / 1000 for item in batches]
    rows = sum(item["rows"] for item in batches)
    return {
        "batches": len(batches), "rows_measured": rows, "rows_per_batch_mean": round(rows / len(batches), 1),
        "batch_seconds_mean": round(statistics.fmean(durations), 2), "batch_seconds_median": round(statistics.median(durations), 2),
        "batch_seconds_max": round(max(durations), 2),
        "rows_per_s": round(rows / sum(durations), 1),
        "planning_seconds_mean": round(statistics.fmean(item["duration"].get("queryPlanning", 0) for item in batches) / 1000, 3),
        "add_batch_seconds_mean": round(statistics.fmean(item["duration"].get("addBatch", 0) for item in batches) / 1000, 2),
        "wal_seconds_mean": round(statistics.fmean(item["duration"].get("walCommit", 0) + item["duration"].get("commit", 0) for item in batches) / 1000, 3),
    }


def step_stats(state, skip=2):
    summary = {}
    plan_seconds = 0.0
    for name in ("bronze", "upsert", *PLAN_STEPS):
        values = [item["seconds"] for item in state["steps"] if item["step"] == name][skip:]
        if values:
            summary[f"{name}_seconds_mean"] = round(statistics.fmean(values), 3)
            plan_seconds += statistics.fmean(values) if name in PLAN_STEPS else 0
    summary["plan_build_seconds_mean"] = round(plan_seconds, 3)
    return summary


def finish_job(process, state, thread, timeout=120):
    signal_stop()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        run("docker kill bench-stream-job", check=False)
        process.wait(timeout=30)
    thread.join(timeout=5)


def check_alive(process, state):
    if process.poll() is not None and not state["done"].is_set():
        raise RuntimeError("Job benchmark dừng bất thường:\n" + "\n".join(state["tail"]))


def capacity_run(config, label, stop_after_rows=CAPACITY_ROWS):
    checkpoint = f"{BENCH_ROOT}/checkpoints/{label.replace(' ', '_')}"
    reset_run_state(checkpoint)
    with ResourceSampler() as sampler:
        started = time.time()
        process, state, thread = start_job(config, checkpoint, stop_after_rows=stop_after_rows)
        while not state["done"].is_set():
            check_alive(process, state)
            if time.time() - started > 1500:
                raise RuntimeError("Quá thời gian chờ:\n" + "\n".join(state["tail"]))
            time.sleep(1)
        wall_seconds = round(time.time() - started, 1)
        process.wait(timeout=60)
        thread.join(timeout=5)
    written = int(psql("SELECT count(*) FROM loan_predictions"))
    resources = sampler.summary()
    worker = resources["containers"].get("spark-worker-1", {})
    driver = resources["containers"].get("bench-stream-job", {})
    return {
        "config": config, "wall_seconds": wall_seconds, "rows_written": written, **batch_stats(state), **step_stats(state),
        "worker_cpu_mean": worker.get("cpu_mean"), "worker_mem_max_mb": worker.get("mem_max_mb"), "driver_mem_max_mb": driver.get("mem_max_mb"),
        "driver_cpu_mean": driver.get("cpu_mean"), "host_available_min_mb": resources["host_available_min_mb"], "temperature_max_c": resources["temperature_max_c"], "throttle_events": resources["throttle_events"],
    }


def capacity_points():
    points = [("gốc", {})]
    points += [(f"batch {size}", {"max_per_batch": size}) for size in SWEEP_BATCH if size != BASELINE["max_per_batch"]]
    points += [(f"core {cores}", {"cores": cores}) for cores in SWEEP_CORES if cores != BASELINE["cores"]]
    points += [(f"shuffle {count}", {"shuffle_partitions": count}) for count in SWEEP_SHUFFLE if count != BASELINE["shuffle_partitions"]]
    points += [(f"executor {memory}", {"executor_memory": memory}) for memory in SWEEP_MEMORY if memory != BASELINE["executor_memory"]]
    return points


def fill_topics():
    """Nạp sẵn toàn bộ pool hồ sơ thật vào topic để job capacity đọc tồn đọng."""
    recreate_topics()
    run(producer_command(1_000_000, 0, 0), timeout=900)


def experiment_capacity(results, repeats, points=None, experiment="capacity"):
    fill_topics()
    for label, change in (points or capacity_points()):
        # capacity luôn chạy hết tốc độ: khoảng trigger không được là giới hạn
        config = {**BASELINE, "trigger": AS_FAST_AS_POSSIBLE, **change}
        for repeat in range(repeats):
            if results.has(experiment, label, repeat):
                continue
            # batch càng lớn thì càng cần nhiều dòng để có đủ batch đo
            rows = min(max(CAPACITY_ROWS, config["max_per_batch"] * 8), POOL_ROWS)
            row = results.add(experiment, label, repeat, **capacity_run(config, f"{experiment}_{label}_{repeat}", stop_after_rows=rows))
            log(f"{experiment} {label} lượt {repeat}: {row.get('rows_per_s')} hồ sơ/s, batch {row.get('rows_per_batch_mean')} dòng mất {row.get('batch_seconds_mean')}s "
                f"(dựng kế hoạch {row.get('plan_build_seconds_mean')}s, bronze {row.get('bronze_seconds_mean')}s, upsert {row.get('upsert_seconds_mean')}s), "
                f"worker CPU {row.get('worker_cpu_mean')}%, RAM {row.get('worker_mem_max_mb')} MB")


def latency_stats(window_start, window_end):
    """Độ trễ của các hồ sơ gửi trong cửa sổ đo; chia ba đoạn thời gian để phát hiện độ trễ tăng dần."""
    length = window_end - window_start
    def query(start, end):
        return psql(
            "SELECT count(*), "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY latency), percentile_cont(0.95) WITHIN GROUP (ORDER BY latency), "
            "percentile_cont(0.99) WITHIN GROUP (ORDER BY latency), max(latency), avg(scored_lag) FROM ("
            "SELECT EXTRACT(EPOCH FROM committed_at - event_time) AS latency, EXTRACT(EPOCH FROM committed_at - scored_at) AS scored_lag "
            f"FROM loan_predictions WHERE event_time >= to_timestamp({start}) AND event_time < to_timestamp({end})) t")
    values = query(window_start, window_end).split(",")
    first = query(window_start, window_start + length / 3).split(",")
    last = query(window_end - length / 3, window_end).split(",")
    number = lambda text: round(float(text), 3) if text else None   # noqa: E731
    return {
        "rows_in_window": int(values[0]), "latency_p50_s": number(values[1]), "latency_p95_s": number(values[2]), "latency_p99_s": number(values[3]),
        "latency_max_s": number(values[4]), "committed_minus_scored_s": number(values[5]),
        "p95_first_third_s": number(first[2]), "p95_last_third_s": number(last[2]),
    }


def sustained_run(config, rate, seconds, label, warmup_seconds=30, outcome_delay=10):
    """Gửi đều ở tốc độ rate; warmup_seconds đầu bỏ qua, phần còn lại là cửa sổ đo."""
    checkpoint = f"{BENCH_ROOT}/checkpoints/{label.replace(' ', '_')}"
    recreate_topics()
    reset_run_state(checkpoint)
    total_rows = int(rate * (warmup_seconds + seconds))
    with ResourceSampler() as sampler:
        process, state, thread = start_job(config, checkpoint)
        try:
            deadline = time.time() + 180
            while not state["ready"].is_set():
                check_alive(process, state)
                if time.time() > deadline:
                    raise RuntimeError("Job không sẵn sàng:\n" + "\n".join(state["tail"]))
                time.sleep(0.5)
            producer_started = time.time()
            producer = subprocess.Popen(producer_command(rate, total_rows, outcome_delay), cwd=PIPELINE_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            window_start = producer_started + warmup_seconds + 3     # thêm vài giây cho container producer khởi động
            window_end = window_start + seconds - 3
            producer_output = producer.communicate(timeout=seconds + warmup_seconds + outcome_delay + 300)[0]
            if producer.returncode != 0:
                raise RuntimeError(f"Producer lỗi:\n{producer_output[-1500:]}")
            sent_at = time.time()
            # chờ pipeline xử lý hết hồ sơ đã gửi; thời gian chờ này cho biết tồn đọng lúc producer dừng
            drained_at = None
            while time.time() - sent_at < 600:
                check_alive(process, state)
                if int(psql("SELECT count(*) FROM loan_predictions")) >= total_rows:
                    drained_at = time.time()
                    break
                time.sleep(1)
        except Exception:
            run("docker kill bench-stream-job", check=False)    # không để job sót lại làm nhiễu lượt sau
            raise
        finish_job(process, state, thread)
    resources = sampler.summary()
    worker = resources["containers"].get("spark-worker-1", {})
    stats = latency_stats(window_start, window_end)
    batches = batch_stats(state, skip_batches=3)
    return {
        "config": config, "rate": rate, "seconds": seconds, "total_rows": total_rows,
        "drain_seconds_after_outcomes": round(drained_at - sent_at, 1) if drained_at else None, **stats, **batches, **step_stats(state, skip=3),
        "worker_cpu_mean": worker.get("cpu_mean"), "worker_mem_max_mb": worker.get("mem_max_mb"),
        "kafka_cpu_mean": resources["containers"].get("kafka", {}).get("cpu_mean"), "postgres_cpu_mean": resources["containers"].get("postgres", {}).get("cpu_mean"),
        "host_available_min_mb": resources["host_available_min_mb"], "temperature_max_c": resources["temperature_max_c"], "throttle_events": resources["throttle_events"],
    }


def experiment_sustained(results, experiment, config, rates, repeats, seconds=150):
    for rate in rates:
        label = f"{rate}/s"
        for repeat in range(repeats):
            if results.has(experiment, label, repeat):
                continue
            row = results.add(experiment, label, repeat, **sustained_run(config, rate, seconds, f"{experiment}_{rate}_{repeat}"))
            log(f"{experiment} {label} lượt {repeat}: p50 {row['latency_p50_s']}s, p95 {row['latency_p95_s']}s, p99 {row['latency_p99_s']}s; "
                f"p95 đầu/cuối {row['p95_first_third_s']}/{row['p95_last_third_s']}s; batch {row.get('rows_per_batch_mean')} dòng {row.get('batch_seconds_mean')}s; "
                f"xử lý hết sau {row['drain_seconds_after_outcomes']}s; worker CPU {row.get('worker_cpu_mean')}%")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", choices=["all", "capacity", "custom-capacity", "sustained"], default="all")
    parser.add_argument("--name", help="tên thí nghiệm khi dùng custom-capacity hoặc sustained")
    parser.add_argument("--config", default="{}", help="JSON ghi đè cấu hình gốc")
    parser.add_argument("--points", default="[]", help='custom-capacity: JSON [["nhãn", {cấu hình}], ...]')
    parser.add_argument("--rates", default="", help="sustained: danh sách tốc độ, ví dụ 50,100,150")
    parser.add_argument("--seconds", type=int, default=150)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    args = parser.parse_args()
    require_stack()
    prepare_database()
    results = Results("spark_stream")
    run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-mkdir", "-p", BENCH_ROOT])
    if args.experiment == "all":
        experiment_capacity(results, args.repeats)
        experiment_capacity(results, args.repeats, points=TUNE_POINTS, experiment="capacity_tune")
        for name, change, single_rates, repeated_rates in SUSTAINED_PLANS:
            config = {**BASELINE, **change}
            experiment_sustained(results, name, config, single_rates, 1, SUSTAINED_SECONDS)
            experiment_sustained(results, name, config, repeated_rates, 3, SUSTAINED_SECONDS)
    elif args.experiment == "capacity":
        experiment_capacity(results, args.repeats)
    elif args.experiment == "custom-capacity":
        experiment_capacity(results, args.repeats, points=json.loads(args.points), experiment=args.name)
    else:
        config = {**BASELINE, **json.loads(args.config)}
        experiment_sustained(results, args.name, config, [float(rate) if "." in rate else int(rate) for rate in args.rates.split(",")], args.repeats, args.seconds)
    log("Hoàn tất benchmark streaming")


if __name__ == "__main__":
    main()
