"""Chạy đúng các handler của jobs/stream_score.py, nhưng với topic, đường dẫn HDFS và database riêng của benchmark.

Khác job thật ở chỗ cấu hình lấy từ tham số, in tiến độ từng micro-batch ra stdout để máy host thu thập, và tự dừng
khi đã xử lý đủ số hồ sơ yêu cầu. Chạy trong container spark-stream, xem benchmark/stream_bench.py.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs"))

import stream_score  # noqa: E402
from credit_pipeline.config import load_model_metadata  # noqa: E402
from credit_pipeline.logs import make_logger, start_spark  # noqa: E402

BENCH_ROOT = "/benchmark/stream"
APPLICATIONS_TOPIC = "bench_applications"
OUTCOMES_TOPIC = "bench_outcomes"


def emit(kind, **fields):
    print(f"BENCH_{kind} {json.dumps(fields)}", flush=True)


def timed(name, function, keep=lambda *_: True):
    """Bọc hàm để đo thời gian từng bước trong micro-batch."""
    def wrapper(*args, **kwargs):
        started = time.time()
        try:
            return function(*args, **kwargs)
        finally:
            if keep(*args):
                emit("STEP", step=name, seconds=round(time.time() - started, 3))
    return wrapper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-per-batch", type=int, default=2000)
    parser.add_argument("--cores", default="2")
    parser.add_argument("--executor-memory", default="1g")
    parser.add_argument("--shuffle-partitions", default="8")
    parser.add_argument("--spark-conf", default="{}", help="JSON cấu hình Spark thêm")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--stop-after-rows", type=int, default=0, help="dừng khi query hồ sơ đã xử lý đủ số dòng này")
    parser.add_argument("--stop-file", default="/benchmark/stream/stop", help="file HDFS: có file này thì dừng")
    args = parser.parse_args()

    log = make_logger("bench_stream")
    # trỏ các tên mà handler dùng sang vùng của benchmark
    stream_score.APPLICATIONS_TOPIC = APPLICATIONS_TOPIC
    stream_score.OUTCOMES_TOPIC = OUTCOMES_TOPIC
    stream_score.BRONZE_STREAM = f"{BENCH_ROOT}/bronze"
    stream_score.GOLD = f"{BENCH_ROOT}/gold"
    is_applications = lambda _frame, topic, _batch: topic == APPLICATIONS_TOPIC   # noqa: E731
    stream_score.write_raw_bronze = timed("bronze", stream_score.write_raw_bronze, is_applications)
    stream_score.upsert_predictions = timed("upsert", stream_score.upsert_predictions)
    # các hàm dựng kế hoạch: DataFrame lười nên thời gian ở đây là thời gian Spark phân tích kế hoạch, chưa tính toán
    for name in ("clean_application", "clean_bureau", "clean_bureau_balance", "application_features", "bureau_balance_loan_features",
                 "bureau_customer_features", "merge_and_fill", "add_scores"):
        setattr(stream_score, name, timed(name, getattr(stream_score, name)))

    spark = start_spark("bench_stream", log, {
        "spark.cores.max": args.cores, "spark.executor.memory": args.executor_memory, "spark.sql.shuffle.partitions": args.shuffle_partitions,
        **json.loads(args.spark_conf)})
    metadata = load_model_metadata(spark)
    feature_names, threshold = metadata["feature_names"], metadata["decision_threshold"]

    (
        stream_score.read_kafka(spark, APPLICATIONS_TOPIC, args.max_per_batch).writeStream
        .queryName("applications").foreachBatch(stream_score.make_applications_handler(log, feature_names, threshold))
        .trigger(processingTime=args.trigger).option("checkpointLocation", f"{args.checkpoint}/applications").start()
    )
    (
        stream_score.read_kafka(spark, OUTCOMES_TOPIC, stream_score.MAX_OUTCOMES_PER_BATCH).writeStream
        .queryName("outcomes").foreachBatch(stream_score.make_outcomes_handler(log))
        .trigger(processingTime=args.trigger).option("checkpointLocation", f"{args.checkpoint}/outcomes").start()
    )
    emit("READY", time=time.time())

    file_system = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    stop_path = spark._jvm.org.apache.hadoop.fs.Path(args.stop_file)
    seen = set()
    processed = 0
    while True:
        spark.streams.awaitAnyTermination(1)
        for query in spark.streams.active:
            for progress in query.recentProgress:
                key = (query.name, progress.batchId)
                if key in seen:
                    continue
                seen.add(key)
                if query.name == "applications":
                    processed += progress.numInputRows
                emit("PROGRESS", query=query.name, batch=progress.batchId, rows=progress.numInputRows, duration=progress.durationMs,
                     input_rate=progress.inputRowsPerSecond, processed_rate=progress.processedRowsPerSecond, timestamp=progress.timestamp)
        if (args.stop_after_rows and processed >= args.stop_after_rows) or file_system.exists(stop_path):
            break
    for query in spark.streams.active:
        query.stop()
    emit("DONE", processed=processed)
    spark.stop()


if __name__ == "__main__":
    main()
