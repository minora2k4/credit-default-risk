"""Làm sạch dữ liệu: đọc CSV gốc trên HDFS, ghi Parquet có kiểu dữ liệu vào tầng silver.

Chạy: make silver
"""
import time

from credit_pipeline.cleaning import clean_application, clean_bureau, clean_bureau_balance
from credit_pipeline.logs import run, start_spark
from credit_pipeline.schemas import APPLICATION_SCHEMA, BUREAU_BALANCE_SCHEMA, BUREAU_SCHEMA

BRONZE = "/datalake/bronze/batch"
SILVER = "/datalake/silver"

# (thư mục bronze, schema, hàm làm sạch, thư mục silver, số dòng của file CSV gốc)
TABLES = [
    ("application_train", APPLICATION_SCHEMA, clean_application, "application", 307_511),
    ("bureau", BUREAU_SCHEMA, clean_bureau, "bureau", 1_716_428),
    ("bureau_balance", BUREAU_BALANCE_SCHEMA, clean_bureau_balance, "bureau_balance", 27_299_925),
]


def main(log):
    spark = start_spark("bronze_to_silver", log)
    for bronze_name, schema, clean, silver_name, expected_rows in TABLES:
        started = time.time()
        log(f"Đang làm sạch {bronze_name}")
        # enforceSchema=False: báo lỗi nếu tên cột trong header không khớp schema (mặc định Spark chỉ khớp theo vị trí)
        raw = spark.read.csv(f"{BRONZE}/{bronze_name}/", header=True, schema=schema, enforceSchema=False)
        silver_path = f"{SILVER}/{silver_name}"
        clean(raw).write.mode("overwrite").parquet(silver_path)

        written_rows = spark.read.parquet(silver_path).count()
        if written_rows != expected_rows:
            raise RuntimeError(f"{silver_name}: ghi {written_rows:,} dòng nhưng CSV gốc có {expected_rows:,}")
        log(f"{silver_name}: {written_rows:,} dòng ({time.time() - started:.0f}s) -> {silver_path}")
    log("Hoàn tất: 3 bảng silver khớp số dòng CSV gốc")
    spark.stop()


if __name__ == "__main__":
    run("bronze_to_silver", main)
