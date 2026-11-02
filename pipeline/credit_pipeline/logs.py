import sys
import traceback
from datetime import datetime


def make_logger(job):
    def log(message):
        print(f"{datetime.now():%H:%M:%S} [{job}] {message}", flush=True)
    return log


def run(job, main):
    """Chạy main(log); gặp lỗi thì in dòng LỖI cùng traceback rồi thoát mã 1."""
    log = make_logger(job)
    try:
        main(log)
    except SystemExit:
        raise
    except Exception as error:
        print(f"{datetime.now():%H:%M:%S} [{job}] LỖI: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        traceback.print_exc()
        sys.exit(1)


def start_spark(app_name, log, config=None):
    """Tạo SparkSession; chỉ in cảnh báo và lỗi của Spark để log của job không bị chìm."""
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(app_name)
    for key, value in (config or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log(f"Đã kết nối Spark ({spark.sparkContext.master}), ứng dụng {app_name}")
    return spark
