"""Chấm điểm hồ sơ vay mới từ Kafka theo thời gian thực bằng Spark Structured Streaming.

  loan_applications ─► mỗi 10 giây: làm sạch, tạo đặc trưng, chấm điểm bằng ONNX (cùng hàm với batch)
                       ├─► HDFS: message thô (bronze) và kết quả (gold)
                       └─► PostgreSQL: bảng loan_predictions (upsert)
  loan_outcomes      ─► nhãn vỡ nợ đến sau, ghi vào HDFS và bảng loan_outcomes

Checkpoint trên HDFS giúp chạy tiếp đúng chỗ dừng sau khi khởi động lại; ghi vào HDFS (ghi đè theo batch_id) và
Postgres (upsert) đều idempotent nên một batch chạy lại không sinh dữ liệu trùng.

Chạy: make stream-up
"""
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from credit_pipeline.cleaning import clean_application, clean_bureau, clean_bureau_balance
from credit_pipeline.config import (
    APPLICATIONS_TOPIC, BRONZE_STREAM, CHECKPOINTS, GOLD, KAFKA_BOOTSTRAP, OUTCOMES_TOPIC, load_model_metadata,
)
from credit_pipeline.features import (
    application_features, bureau_balance_loan_features, bureau_customer_features, merge_and_fill,
)
from credit_pipeline.logs import run, start_spark
from credit_pipeline.schemas import MESSAGE_SCHEMA, OUTCOME_SCHEMA
from credit_pipeline.scoring import add_scores
from credit_pipeline.sinks import upsert_outcomes, upsert_predictions

TRIGGER = "10 seconds"
MAX_APPLICATIONS_PER_BATCH = 2000
MAX_OUTCOMES_PER_BATCH = 5000
HEARTBEAT_SECONDS = 60

totals = {"applications": 0, "outcomes": 0, "started": time.time()}


def read_kafka(spark, topic, max_offsets):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")      # chỉ dùng lần chạy đầu; sau đó đọc tiếp từ checkpoint
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )


def write_raw_bronze(batch_df, topic, batch_id):
    """Lưu message thô kèm vị trí Kafka; ghi đè theo batch_id nên chạy lại không nhân đôi."""
    (
        batch_df.select(
            F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
            "topic", "partition", "offset", "timestamp")
        .coalesce(1).write.mode("overwrite").parquet(f"{BRONZE_STREAM}/{topic}/batch_id={batch_id}")
    )


def latest_per_key(frame, key_column):
    """Giữ message mới nhất (offset lớn nhất) của mỗi khóa. Hồ sơ gửi lặp trong một batch sẽ làm nhân đôi khoản vay."""
    newest_first = Window.partitionBy(key_column).orderBy(F.col("kafka_offset").desc())
    return frame.withColumn("_rank", F.row_number().over(newest_first)).filter("_rank = 1").drop("_rank")


def rate_text():
    elapsed = max(time.time() - totals["started"], 1e-9)
    return f"tổng {totals['applications']:,} hồ sơ, {totals['outcomes']:,} nhãn ({totals['applications'] / elapsed:.1f} hồ sơ/s từ lúc khởi động)"


def make_applications_handler(log, feature_names, threshold):
    def handle(batch_df, batch_id):
        started = time.time()
        batch_df = batch_df.persist()
        try:
            total_messages = batch_df.count()
            if total_messages == 0:
                return
            spark = batch_df.sparkSession
            write_raw_bronze(batch_df, APPLICATIONS_TOPIC, batch_id)

            # parse JSON; message hỏng (không parse được, thiếu id) bị loại, bản thô vẫn nằm ở bronze
            parsed = batch_df.select(
                F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"),
            ).select(
                "kafka_partition", "kafka_offset", F.col("m.application_id").alias("application_id"),
                F.col("m.event_time").cast("timestamp").alias("event_time"),
                F.col("m.application").alias("application"), F.col("m.bureau").alias("bureau"),
            ).filter(F.col("application_id").isNotNull() & F.col("application.SK_ID_CURR").isNotNull())
            messages = latest_per_key(parsed, "application_id").persist()

            # tách thành ba bảng cùng cấu trúc với dữ liệu batch
            application = messages.select("application.*")
            loans = messages.select(F.col("application.SK_ID_CURR").alias("SK_ID_CURR"), F.explode("bureau").alias("loan"))
            bureau = loans.select("SK_ID_CURR", "loan.*").drop("balance")
            balance = loans.select(F.col("loan.SK_ID_BUREAU").alias("SK_ID_BUREAU"),
                                   F.explode("loan.balance").alias("month")).select("SK_ID_BUREAU", "month.*")

            application_clean = clean_application(application)
            bureau_loan_features = bureau_balance_loan_features(clean_bureau_balance(balance))
            features = merge_and_fill(
                application_features(application_clean, feature_names),
                bureau_customer_features(clean_bureau(bureau), bureau_loan_features),
                feature_names,
            )
            scored = add_scores(spark, features, feature_names, threshold, ["SK_ID_CURR"])

            # mọi hồ sơ đều có một dòng kết quả; chỉ Cash loans đủ số tiền mới được chấm điểm
            status = (
                F.when(F.col("NAME_CONTRACT_TYPE") != "Cash loans", "OUT_OF_SCOPE")
                .when(~F.col("HAS_REQUIRED_AMOUNTS"), "INVALID_INPUT")
                .otherwise("SCORED")
            )
            predictions = (
                application_clean.select("SK_ID_CURR", status.alias("status"))
                .join(messages.select(F.col("application.SK_ID_CURR").alias("SK_ID_CURR"), "application_id", "event_time",
                                      "kafka_partition", "kafka_offset"), on="SK_ID_CURR")
                .join(scored.select("SK_ID_CURR", "probability", "decision", "model_version"), on="SK_ID_CURR", how="left")
                .select(
                    "application_id", F.col("SK_ID_CURR").alias("sk_id_curr"), "event_time",
                    F.current_timestamp().alias("scored_at"), "probability", "decision",
                    F.lit(threshold).alias("threshold"), "status", "model_version", "kafka_partition", "kafka_offset")
                .persist()   # dùng cho cả HDFS lẫn Postgres: tính đặc trưng và chấm điểm đúng một lần
            )

            predictions.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/predictions_stream/batch_id={batch_id}")
            rows = predictions.collect()
            written = upsert_predictions(rows)

            counts = {}
            for row in rows:
                counts[row["status"]] = counts.get(row["status"], 0) + 1
            totals["applications"] += written
            log(f"applications batch {batch_id}: {total_messages} message -> {written} hồ sơ {counts} "
                f"({time.time() - started:.1f}s) | {rate_text()}")
            predictions.unpersist()
            messages.unpersist()
        except Exception as error:
            log(f"LỖI applications batch {batch_id}: {type(error).__name__}: {error}")
            raise     # để Spark dừng job; Docker khởi động lại và chạy lại đúng batch này từ checkpoint
        finally:
            batch_df.unpersist()
    return handle


def make_outcomes_handler(log):
    def handle(batch_df, batch_id):
        started = time.time()
        batch_df = batch_df.persist()
        try:
            if batch_df.count() == 0:
                return
            write_raw_bronze(batch_df, OUTCOMES_TOPIC, batch_id)
            parsed = batch_df.select(
                F.col("offset").alias("kafka_offset"), F.from_json(F.col("value").cast("string"), OUTCOME_SCHEMA).alias("o")
            ).select("kafka_offset", "o.*").filter(F.col("application_id").isNotNull() & F.col("TARGET").isNotNull())
            outcomes = latest_per_key(parsed, "application_id").select(
                "application_id", F.col("TARGET").cast("smallint").alias("target"),
                F.col("outcome_time").cast("timestamp").alias("outcome_time"))
            written = upsert_outcomes(outcomes.collect())
            totals["outcomes"] += written
            log(f"outcomes batch {batch_id}: {written} nhãn ({time.time() - started:.1f}s) | {rate_text()}")
        except Exception as error:
            log(f"LỖI outcomes batch {batch_id}: {type(error).__name__}: {error}")
            raise
        finally:
            batch_df.unpersist()
    return handle


def main(log):
    # job chạy thường trực nên dùng ít tài nguyên (2 core, executor 1 GB) để chừa chỗ cho các dịch vụ khác
    spark = start_spark("stream_score", log, {
        "spark.cores.max": "2", "spark.executor.memory": "1g", "spark.sql.shuffle.partitions": "8"})
    metadata = load_model_metadata(spark)
    feature_names, threshold = metadata["feature_names"], metadata["decision_threshold"]
    log(f"Đã đọc mô hình: {len(feature_names)} đặc trưng, ngưỡng quyết định {threshold:.6f}")

    (
        read_kafka(spark, APPLICATIONS_TOPIC, MAX_APPLICATIONS_PER_BATCH).writeStream
        .queryName("applications").foreachBatch(make_applications_handler(log, feature_names, threshold))
        .trigger(processingTime=TRIGGER).option("checkpointLocation", f"{CHECKPOINTS}/stream_applications").start()
    )
    (
        read_kafka(spark, OUTCOMES_TOPIC, MAX_OUTCOMES_PER_BATCH).writeStream
        .queryName("outcomes").foreachBatch(make_outcomes_handler(log))
        .trigger(processingTime=TRIGGER).option("checkpointLocation", f"{CHECKPOINTS}/stream_outcomes").start()
    )
    log(f"Đang chạy: đọc topic {APPLICATIONS_TOPIC} và {OUTCOMES_TOPIC} từ {KAFKA_BOOTSTRAP}, mỗi {TRIGGER}")

    # một query lỗi thì awaitAnyTermination ném lỗi và cả job dừng; trong lúc chờ, cứ mỗi phút báo còn sống
    while not spark.streams.awaitAnyTermination(HEARTBEAT_SECONDS):
        log(f"Vẫn đang chạy | {rate_text()}")


if __name__ == "__main__":
    run("stream_score", main)
