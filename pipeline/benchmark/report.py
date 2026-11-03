"""In các bảng tổng hợp từ kết quả benchmark trong work/benchmark/*.jsonl. Mỗi ô là trung vị các lượt đo.

Chạy: make bench-report
"""
import json
import os
import statistics

from common import RESULTS_DIR


def load(suite):
    path = os.path.join(RESULTS_DIR, f"{suite}.jsonl")
    if not os.path.exists(path):
        return {}
    grouped = {}
    with open(path, encoding="utf-8") as results_file:
        for line in results_file:
            row = json.loads(line)
            grouped.setdefault((row["experiment"], row["label"]), []).append(row)
    return grouped


def median(rows, field):
    values = [row[field] for row in rows if row.get(field) is not None]
    return statistics.median(values) if values else None


def show(value, digits=0):
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def spread(rows, field, digits=0):
    values = [row[field] for row in rows if row.get(field) is not None]
    if not values:
        return "-"
    return f"{statistics.median(values):,.{digits}f} ({min(values):,.{digits}f} - {max(values):,.{digits}f})"


def table(title, headers, lines):
    if not lines:
        return
    print(f"\n### {title}\n")
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join(["---"] + ["---:"] * (len(headers) - 1)) + "|")
    for line in lines:
        print("| " + " | ".join(line) + " |")


def kafka():
    data = load("kafka")

    def producer_table(title, experiment):
        lines = [[label, spread(rows, "records_per_s"), show(median(rows, "mb_per_s")), show(median(rows, "latency_p50_ms")),
                  show(median(rows, "latency_p99_ms")), show(median(rows, "compression_ratio"), 1), show(median(rows, "kafka_cpu_mean"))]
                 for (name, label), rows in data.items() if name == experiment]
        table(title, ["Cấu hình", "record/s (min - max)", "MB/s", "p50 ms", "p99 ms", "Nén (lần)", "CPU Kafka %"], lines)

    producer_table("Kafka: producer theo từng tham số", "sweep")
    producer_table("Kafka: số luồng broker", "broker")
    producer_table("Kafka: độ trễ theo tải", "latency_gốc")
    table("Kafka: nhiều producer đồng thời", ["Số producer", "record/s (min - max)", "MB/s", "p50 ms", "p99 ms", "CPU Kafka %"],
          [[label, spread(rows, "records_per_s"), show(median(rows, "mb_per_s")), show(median(rows, "latency_p50_ms")), show(median(rows, "latency_p99_ms")),
            show(median(rows, "kafka_cpu_mean"))] for (name, label), rows in data.items() if name == "scale"])
    table("Kafka: consumer", ["Thí nghiệm", "record/s (min - max)", "MB/s"],
          [[f"{name} {label}", spread(rows, "fetch_records_per_s"), show(median(rows, "fetch_mb_per_s"))]
           for (name, label), rows in data.items() if name.startswith("consumer")])
    table("Kafka: độ trễ đầu-cuối khi không có tải", ["acks", "trung bình ms", "p50 ms", "p99 ms", "p99.9 ms"],
          [[label, show(median(rows, "latency_avg_ms"), 2), show(median(rows, "latency_p50_ms")), show(median(rows, "latency_p99_ms")), show(median(rows, "latency_p999_ms"))]
           for (name, label), rows in data.items() if name == "e2e"])
    table("Kafka: producer Python của pipeline", ["Nguồn", "hồ sơ/s (min - max)", "Tổng giây", "Trên đĩa MB"],
          [[label, spread(rows, "records_per_s"), show(median(rows, "total_seconds"), 1), show(median(rows, "on_disk_mb"))]
           for (name, label), rows in data.items() if name == "app"])


def spark_batch():
    data = load("spark_batch")
    lines = [[name, label, spread(rows, "application_seconds", 1), show(median(rows, "task_seconds")), show(median(rows, "gc_seconds"), 1),
              show(median(rows, "spill_mb")), show(median(rows, "shuffle_write_mb")), show(median(rows, "worker_mem_max_mb"))]
             for (name, label), rows in data.items()]
    table("Spark batch", ["Thí nghiệm", "Job | cấu hình", "Giây (min - max)", "Tổng task giây", "GC giây", "Spill MB", "Shuffle MB", "RAM worker MB"], lines)


def spark_stream():
    data = load("spark_stream")
    capacity = [[name, label, spread(rows, "rows_per_s"), show(median(rows, "rows_per_batch_mean")), show(median(rows, "batch_seconds_mean"), 2),
                 show(median(rows, "plan_build_seconds_mean"), 2), show(median(rows, "worker_mem_max_mb")), show(median(rows, "driver_mem_max_mb"))]
                for (name, label), rows in data.items() if name.startswith("capacity")]
    table("Streaming: năng lực khi đọc tồn đọng", ["Thí nghiệm", "Cấu hình", "hồ sơ/s (min - max)", "Dòng/batch", "Giây/batch", "Dựng kế hoạch giây", "RAM worker MB", "RAM driver MB"], capacity)
    sustained = [[name, label, show(median(rows, "latency_p50_s"), 1), show(median(rows, "latency_p95_s"), 1), show(median(rows, "latency_p99_s"), 1),
                  show(median(rows, "p95_first_third_s"), 1), show(median(rows, "p95_last_third_s"), 1), show(median(rows, "batch_seconds_mean"), 2),
                  show(median(rows, "worker_cpu_mean")), show(median(rows, "drain_seconds_after_outcomes"), 1)]
                 for (name, label), rows in data.items() if name.startswith("sustained")]
    table("Streaming: tải đều", ["Cấu hình", "Tốc độ", "p50 s", "p95 s", "p99 s", "p95 đầu s", "p95 cuối s", "Giây/batch", "CPU worker %", "Xử lý hết sau s"], sustained)


if __name__ == "__main__":
    kafka()
    spark_batch()
    spark_stream()
