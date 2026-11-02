"""Làm sạch dữ liệu thô của ba bảng. Batch và streaming dùng chung các hàm này."""

from pyspark.sql import functions as F
from credit_pipeline.columns import APPLICATION_COLUMNS, REQUIRED_AMOUNT_COLUMNS

RETIREE_EMPLOYED_CODE = 365243   # mã DAYS_EMPLOYED của người hưu trí, không phải số ngày đi làm thật


def clean_application(application):
    """Giữ các cột cần dùng, gắn cờ hồ sơ đủ số tiền, đổi mã hưu trí thành null.
    Không xóa hồ sơ thiếu số tiền mà gắn cờ HAS_REQUIRED_AMOUNTS, để streaming vẫn ghi nhận hồ sơ lỗi.
    """
    columns = [name for name in APPLICATION_COLUMNS if name in application.columns]   # hồ sơ từ Kafka không có TARGET
    has_required_amounts = F.lit(True)
    for name in REQUIRED_AMOUNT_COLUMNS:
        has_required_amounts = has_required_amounts & F.col(name).isNotNull()

    return (
        application.select(*columns)
        .withColumn("HAS_REQUIRED_AMOUNTS", has_required_amounts)
        .withColumn(
            "DAYS_EMPLOYED",
            F.when(F.col("DAYS_EMPLOYED") == RETIREE_EMPLOYED_CODE, F.lit(None).cast("double"))
            .otherwise(F.col("DAYS_EMPLOYED").cast("double")),
        )
        # thiếu nghề nghiệp phần lớn là người hưu trí, không phải thất nghiệp
        .withColumn("OCCUPATION_TYPE", F.coalesce(F.col("OCCUPATION_TYPE"), F.lit("Not declared")))
    )


def clean_bureau(bureau):
    """Chuyển giá trị sai logic thành null; không xóa dòng, không điền giá trị."""
    no_value = F.lit(None).cast("double")
    return (
        bureau
        # ngày kết thúc trước ngày mở khoản vay là sai
        .withColumn("DAYS_CREDIT_ENDDATE",
                    F.when(F.col("DAYS_CREDIT_ENDDATE") < F.col("DAYS_CREDIT"), no_value).otherwise(F.col("DAYS_CREDIT_ENDDATE")))
        .withColumn("DAYS_ENDDATE_FACT",
                    F.when(F.col("DAYS_ENDDATE_FACT") < F.col("DAYS_CREDIT"), no_value).otherwise(F.col("DAYS_ENDDATE_FACT")))
        .withColumn("AMT_CREDIT_SUM_DEBT",
                    F.when(F.col("AMT_CREDIT_SUM_DEBT") < 0, F.lit(0.0)).otherwise(F.col("AMT_CREDIT_SUM_DEBT")))
        .withColumn("AMT_CREDIT_SUM_LIMIT",
                    F.when(F.col("AMT_CREDIT_SUM_LIMIT") <= 0, no_value).otherwise(F.col("AMT_CREDIT_SUM_LIMIT")))
    )


def clean_bureau_balance(bureau_balance):
    """Đổi STATUS thành mức quá hạn DPD (0 đến 5; C và X thành null) và hai cờ IS_X, IS_C."""
    status = F.col("STATUS")
    return (
        bureau_balance.select("SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS")
        .withColumn("DPD", F.when(status.isin("0", "1", "2", "3", "4", "5"), status.cast("double")))
        .withColumn("IS_X", F.when(status == "X", 1).otherwise(0).cast("tinyint"))
        .withColumn("IS_C", F.when(status == "C", 1).otherwise(0).cast("tinyint"))
    )
