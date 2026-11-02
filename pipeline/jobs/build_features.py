"""Tạo bảng đặc trưng Cash loans (221 đặc trưng) kèm nhãn và cột split train/test, ghi vào tầng gold.

Chạy: make features
"""
import time

from pyspark.sql import functions as F

from credit_pipeline.config import GOLD, REFERENCE_HOLDOUT, SILVER, load_model_metadata
from credit_pipeline.features import (
    application_features, bureau_balance_loan_features, bureau_customer_features, merge_and_fill,
)
from credit_pipeline.logs import run, start_spark

EXPECTED_ROWS = {"train": 222_576, "test": 55_644}
OUTPUT_PATH = f"{GOLD}/features_cash"


def main(log):
    spark = start_spark("build_features", log)
    started = time.time()
    feature_names = load_model_metadata(spark)["feature_names"]
    log(f"Đã đọc metadata mô hình: {len(feature_names)} đặc trưng")

    log("Đang tạo đặc trưng từ silver (application, bureau, bureau_balance)")
    application = application_features(spark.read.parquet(f"{SILVER}/application"), feature_names)
    bb_loan = bureau_balance_loan_features(spark.read.parquet(f"{SILVER}/bureau_balance"))
    bureau = bureau_customer_features(spark.read.parquet(f"{SILVER}/bureau"), bb_loan)
    features = merge_and_fill(application, bureau, feature_names)

    # holdout_ids tái tạo đúng phép chia train/test của notebook huấn luyện
    holdout = spark.read.parquet(REFERENCE_HOLDOUT).select(
        "SK_ID_CURR", "split", F.col("TARGET").alias("HOLDOUT_TARGET"))
    gold = features.join(holdout, on="SK_ID_CURR", how="left")
    gold = gold.select("SK_ID_CURR", "TARGET", "split", *[F.col(f"`{name}`") for name in feature_names])

    gold.coalesce(4).write.mode("overwrite").parquet(OUTPUT_PATH)     # vài chục MB nên gộp thành 4 file
    log(f"Đã ghi {OUTPUT_PATH} ({time.time() - started:.0f}s), đang kiểm tra")

    written = spark.read.parquet(OUTPUT_PATH)
    total = written.count()
    split_counts = {row["split"]: row["count"] for row in written.groupBy("split").count().collect()}
    no_split = split_counts.get(None, 0)
    test_default_rate = written.filter(F.col("split") == "test").agg(F.avg("TARGET")).collect()[0][0]
    target_mismatch = (   # nhãn trong application phải trùng nhãn trong holdout
        features.join(holdout, on="SK_ID_CURR", how="left")
        .filter(F.col("TARGET") != F.col("HOLDOUT_TARGET")).count()
    )
    log(f"{total:,} dòng, {len(written.columns) - 3} đặc trưng | split {split_counts} | "
        f"tỉ lệ vỡ nợ tập test {test_default_rate:.4f}")

    problems = []
    if total != sum(EXPECTED_ROWS.values()):
        problems.append(f"số dòng {total:,}, cần {sum(EXPECTED_ROWS.values()):,}")
    if no_split or target_mismatch:
        problems.append(f"{no_split} hồ sơ thiếu split, {target_mismatch} hồ sơ lệch nhãn")
    if split_counts != EXPECTED_ROWS:
        problems.append(f"số dòng train/test {split_counts}, cần {EXPECTED_ROWS}")
    if written.columns[3:] != feature_names:
        problems.append("thứ tự đặc trưng khác metadata.json")
    if problems:
        raise RuntimeError("; ".join(problems))
    log("Hoàn tất: gold/features_cash khớp số dòng, split và thứ tự đặc trưng của notebook")
    spark.stop()


if __name__ == "__main__":
    run("build_features", main)
