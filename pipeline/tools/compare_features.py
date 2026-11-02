"""So sánh đặc trưng do Spark tạo với bảng tham chiếu xuất từ notebook huấn luyện, từng cột một.

  make compare STAGE=application|bureau_balance|bureau|cash

Hai giá trị khớp khi cùng null, hoặc bằng nhau (kể cả inf), hoặc lệch tương đối nhỏ hơn --tolerance.
Thoát với mã khác 0 nếu có cột lệch.
"""
import argparse

from pyspark.sql import functions as F

from credit_pipeline.config import REFERENCE_NOTEBOOK, SILVER, load_model_metadata
from credit_pipeline.features import (
    application_features, bureau_balance_loan_features, bureau_customer_features, merge_and_fill,
)
from credit_pipeline.logs import run, start_spark
from credit_pipeline.naming import sanitize_name

KEY = "SK_ID_CURR"


def compare(ours, reference, key, tolerance, label, log):
    """So hai bảng theo khóa `key`; trả về True nếu mọi cột khớp."""
    ours_columns = [name for name in ours.columns if name != key]
    reference_columns = [name for name in reference.columns if name != key]
    only_ours = sorted(set(ours_columns) - set(reference_columns))
    only_reference = sorted(set(reference_columns) - set(ours_columns))
    common = [name for name in ours_columns if name in set(reference_columns)]

    # đặt lại tên cột thành s0, r0, s1, r1... để tránh ký tự đặc biệt (CREDIT/TOTAL, dấu cách) trong biểu thức
    ours_renamed = ours.select(key, F.lit(1).alias("_in_s"), *[F.col(f"`{name}`").cast("double").alias(f"s{i}") for i, name in enumerate(common)])
    reference_renamed = reference.select(key, F.lit(1).alias("_in_r"), *[F.col(f"`{name}`").cast("double").alias(f"r{i}") for i, name in enumerate(common)])
    joined = ours_renamed.join(reference_renamed, on=key, how="full_outer")

    aggregations = [
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("_in_r").isNull(), 1).otherwise(0)).alias("only_spark"),
        F.sum(F.when(F.col("_in_s").isNull(), 1).otherwise(0)).alias("only_reference"),
    ]
    for i in range(len(common)):
        s, r = F.col(f"s{i}"), F.col(f"r{i}")
        null_differs = s.isNull() != r.isNull()
        both = s.isNotNull() & r.isNotNull()
        relative = F.abs(s - r) / F.greatest(F.lit(1.0), F.abs(s), F.abs(r))
        differs = null_differs | (both & (s != r) & ~(F.isnan(s) & F.isnan(r)) & ~(relative <= tolerance))
        aggregations += [
            F.sum(F.when(null_differs, 1).otherwise(0)).alias(f"n{i}"),
            F.sum(F.when(differs, 1).otherwise(0)).alias(f"d{i}"),
            F.max(F.when(both & (s != r), relative)).alias(f"m{i}"),
        ]
    row = joined.agg(*aggregations).collect()[0]

    bad = [(name, row[f"n{i}"], row[f"d{i}"], row[f"m{i}"]) for i, name in enumerate(common) if row[f"d{i}"]]
    largest = max((row[f"m{i}"] or 0.0 for i in range(len(common))), default=0.0)

    log(f"{label}: {row['rows']:,} dòng, {len(common)} cột so sánh | chỉ có ở Spark {row['only_spark']:,} dòng, "
        f"chỉ có ở tham chiếu {row['only_reference']:,} dòng | lệch tương đối lớn nhất {largest:.2e}")
    if only_ours or only_reference:
        log(f"Cột chỉ có ở Spark: {only_ours[:5]} | chỉ có ở tham chiếu: {only_reference[:5]}")
    for name, nulls, diffs, rel in sorted(bad, key=lambda item: -item[2])[:25]:
        log(f"LỆCH {name}: {diffs:,} ô lệch giá trị, {nulls:,} ô lệch vị trí null, lệch tương đối lớn nhất {rel if rel is not None else float('nan'):.2e}")
    return not bad and not only_ours and not only_reference and not row["only_spark"] and not row["only_reference"]


def read_reference(spark, name):
    """Bảng tham chiếu có tên cột chưa thay ký tự lạ; thay cùng cách với mô hình để so được."""
    reference = spark.read.parquet(f"{REFERENCE_NOTEBOOK}/{name}.parquet")
    return reference.toDF(*[sanitize_name(column) for column in reference.columns])


def compare_application(spark, tolerance, log):
    names = load_model_metadata(spark)["feature_names"]
    ours = application_features(spark.read.parquet(f"{SILVER}/application"), names)
    return compare(ours, read_reference(spark, "application_features"), KEY, tolerance, "application_features", log)


def compare_bureau_balance(spark, tolerance, log):
    ours = bureau_balance_loan_features(spark.read.parquet(f"{SILVER}/bureau_balance"))
    return compare(ours, read_reference(spark, "bureau_balance_features"), "SK_ID_BUREAU", tolerance, "bureau_balance_features", log)


def compare_bureau(spark, tolerance, log):
    bb_loan = bureau_balance_loan_features(spark.read.parquet(f"{SILVER}/bureau_balance"))
    ours = bureau_customer_features(spark.read.parquet(f"{SILVER}/bureau"), bb_loan)
    return compare(ours, read_reference(spark, "bureau_features"), KEY, tolerance, "bureau_features", log)


def compare_cash(spark, tolerance, log):
    names = load_model_metadata(spark)["feature_names"]
    application = application_features(spark.read.parquet(f"{SILVER}/application"), names)
    bb_loan = bureau_balance_loan_features(spark.read.parquet(f"{SILVER}/bureau_balance"))
    bureau = bureau_customer_features(spark.read.parquet(f"{SILVER}/bureau"), bb_loan)
    ours = merge_and_fill(application, bureau, names)
    return compare(ours, read_reference(spark, "application_train_cash"), KEY, tolerance, "application_train_cash", log)


STAGES = {
    "application": compare_application,
    "bureau_balance": compare_bureau_balance,
    "bureau": compare_bureau,
    "cash": compare_cash,
}


def main(log):
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=sorted(STAGES), required=True)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    args = parser.parse_args()
    spark = start_spark(f"compare_{args.stage}", log)
    log(f"Đang so sánh {args.stage} với bảng tham chiếu của notebook (dung sai {args.tolerance:.0e})")
    ok = STAGES[args.stage](spark, args.tolerance, log)
    spark.stop()
    if not ok:
        raise RuntimeError(f"{args.stage}: đặc trưng chưa khớp notebook")
    log(f"Hoàn tất: {args.stage} khớp hoàn toàn với notebook")


if __name__ == "__main__":
    run("compare_features", main)
