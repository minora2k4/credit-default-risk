"""Schema của 3 bảng CSV gốc và của message Kafka.

Khai báo sẵn schema khi đọc CSV nhanh hơn inferSchema (không phải quét file hai lần) và kiểu khớp với pandas.
Spark đọc CSV theo vị trí cột nên phải liệt kê đủ và đúng thứ tự.
"""
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

from credit_pipeline.columns import APPLICATION_COLUMNS as MODEL_APPLICATION_COLUMNS

_TYPES = {"long": LongType(), "double": DoubleType(), "string": StringType()}


def _struct(columns):
    return StructType([StructField(name, _TYPES[kind], True) for name, kind in columns])

APPLICATION_COLUMNS = [
    ("SK_ID_CURR", "long"), ("TARGET", "long"), ("NAME_CONTRACT_TYPE", "string"), ("CODE_GENDER", "string"),
    ("FLAG_OWN_CAR", "string"), ("FLAG_OWN_REALTY", "string"), ("CNT_CHILDREN", "long"),
    ("AMT_INCOME_TOTAL", "double"), ("AMT_CREDIT", "double"), ("AMT_ANNUITY", "double"),
    ("AMT_GOODS_PRICE", "double"), ("NAME_TYPE_SUITE", "string"), ("NAME_INCOME_TYPE", "string"),
    ("NAME_EDUCATION_TYPE", "string"), ("NAME_FAMILY_STATUS", "string"), ("NAME_HOUSING_TYPE", "string"),
    ("REGION_POPULATION_RELATIVE", "double"), ("DAYS_BIRTH", "long"), ("DAYS_EMPLOYED", "long"),
    ("DAYS_REGISTRATION", "double"), ("DAYS_ID_PUBLISH", "long"), ("OWN_CAR_AGE", "double"), ("FLAG_MOBIL", "long"),
    ("FLAG_EMP_PHONE", "long"), ("FLAG_WORK_PHONE", "long"), ("FLAG_CONT_MOBILE", "long"), ("FLAG_PHONE", "long"),
    ("FLAG_EMAIL", "long"), ("OCCUPATION_TYPE", "string"), ("CNT_FAM_MEMBERS", "double"),
    ("REGION_RATING_CLIENT", "long"), ("REGION_RATING_CLIENT_W_CITY", "long"),
    ("WEEKDAY_APPR_PROCESS_START", "string"), ("HOUR_APPR_PROCESS_START", "long"),
    ("REG_REGION_NOT_LIVE_REGION", "long"), ("REG_REGION_NOT_WORK_REGION", "long"),
    ("LIVE_REGION_NOT_WORK_REGION", "long"), ("REG_CITY_NOT_LIVE_CITY", "long"), ("REG_CITY_NOT_WORK_CITY", "long"),
    ("LIVE_CITY_NOT_WORK_CITY", "long"), ("ORGANIZATION_TYPE", "string"), ("EXT_SOURCE_1", "double"),
    ("EXT_SOURCE_2", "double"), ("EXT_SOURCE_3", "double"), ("APARTMENTS_AVG", "double"),
    ("BASEMENTAREA_AVG", "double"), ("YEARS_BEGINEXPLUATATION_AVG", "double"), ("YEARS_BUILD_AVG", "double"),
    ("COMMONAREA_AVG", "double"), ("ELEVATORS_AVG", "double"), ("ENTRANCES_AVG", "double"),
    ("FLOORSMAX_AVG", "double"), ("FLOORSMIN_AVG", "double"), ("LANDAREA_AVG", "double"),
    ("LIVINGAPARTMENTS_AVG", "double"), ("LIVINGAREA_AVG", "double"), ("NONLIVINGAPARTMENTS_AVG", "double"),
    ("NONLIVINGAREA_AVG", "double"), ("APARTMENTS_MODE", "double"), ("BASEMENTAREA_MODE", "double"),
    ("YEARS_BEGINEXPLUATATION_MODE", "double"), ("YEARS_BUILD_MODE", "double"), ("COMMONAREA_MODE", "double"),
    ("ELEVATORS_MODE", "double"), ("ENTRANCES_MODE", "double"), ("FLOORSMAX_MODE", "double"),
    ("FLOORSMIN_MODE", "double"), ("LANDAREA_MODE", "double"), ("LIVINGAPARTMENTS_MODE", "double"),
    ("LIVINGAREA_MODE", "double"), ("NONLIVINGAPARTMENTS_MODE", "double"), ("NONLIVINGAREA_MODE", "double"),
    ("APARTMENTS_MEDI", "double"), ("BASEMENTAREA_MEDI", "double"), ("YEARS_BEGINEXPLUATATION_MEDI", "double"),
    ("YEARS_BUILD_MEDI", "double"), ("COMMONAREA_MEDI", "double"), ("ELEVATORS_MEDI", "double"),
    ("ENTRANCES_MEDI", "double"), ("FLOORSMAX_MEDI", "double"), ("FLOORSMIN_MEDI", "double"),
    ("LANDAREA_MEDI", "double"), ("LIVINGAPARTMENTS_MEDI", "double"), ("LIVINGAREA_MEDI", "double"),
    ("NONLIVINGAPARTMENTS_MEDI", "double"), ("NONLIVINGAREA_MEDI", "double"), ("FONDKAPREMONT_MODE", "string"),
    ("HOUSETYPE_MODE", "string"), ("TOTALAREA_MODE", "double"), ("WALLSMATERIAL_MODE", "string"),
    ("EMERGENCYSTATE_MODE", "string"), ("OBS_30_CNT_SOCIAL_CIRCLE", "double"), ("DEF_30_CNT_SOCIAL_CIRCLE", "double"),
    ("OBS_60_CNT_SOCIAL_CIRCLE", "double"), ("DEF_60_CNT_SOCIAL_CIRCLE", "double"),
    ("DAYS_LAST_PHONE_CHANGE", "double"), ("FLAG_DOCUMENT_2", "long"), ("FLAG_DOCUMENT_3", "long"),
    ("FLAG_DOCUMENT_4", "long"), ("FLAG_DOCUMENT_5", "long"), ("FLAG_DOCUMENT_6", "long"),
    ("FLAG_DOCUMENT_7", "long"), ("FLAG_DOCUMENT_8", "long"), ("FLAG_DOCUMENT_9", "long"),
    ("FLAG_DOCUMENT_10", "long"), ("FLAG_DOCUMENT_11", "long"), ("FLAG_DOCUMENT_12", "long"),
    ("FLAG_DOCUMENT_13", "long"), ("FLAG_DOCUMENT_14", "long"), ("FLAG_DOCUMENT_15", "long"),
    ("FLAG_DOCUMENT_16", "long"), ("FLAG_DOCUMENT_17", "long"), ("FLAG_DOCUMENT_18", "long"),
    ("FLAG_DOCUMENT_19", "long"), ("FLAG_DOCUMENT_20", "long"), ("FLAG_DOCUMENT_21", "long"),
    ("AMT_REQ_CREDIT_BUREAU_HOUR", "double"), ("AMT_REQ_CREDIT_BUREAU_DAY", "double"),
    ("AMT_REQ_CREDIT_BUREAU_WEEK", "double"), ("AMT_REQ_CREDIT_BUREAU_MON", "double"),
    ("AMT_REQ_CREDIT_BUREAU_QRT", "double"), ("AMT_REQ_CREDIT_BUREAU_YEAR", "double"),
]
APPLICATION_SCHEMA = _struct(APPLICATION_COLUMNS)

BUREAU_COLUMNS = [
    ("SK_ID_CURR", "long"), ("SK_ID_BUREAU", "long"), ("CREDIT_ACTIVE", "string"), ("CREDIT_CURRENCY", "string"),
    ("DAYS_CREDIT", "long"), ("CREDIT_DAY_OVERDUE", "long"), ("DAYS_CREDIT_ENDDATE", "double"),
    ("DAYS_ENDDATE_FACT", "double"), ("AMT_CREDIT_MAX_OVERDUE", "double"), ("CNT_CREDIT_PROLONG", "long"),
    ("AMT_CREDIT_SUM", "double"), ("AMT_CREDIT_SUM_DEBT", "double"), ("AMT_CREDIT_SUM_LIMIT", "double"),
    ("AMT_CREDIT_SUM_OVERDUE", "double"), ("CREDIT_TYPE", "string"), ("DAYS_CREDIT_UPDATE", "long"),
    ("AMT_ANNUITY", "double"),
]
BUREAU_SCHEMA = _struct(BUREAU_COLUMNS)

BUREAU_BALANCE_COLUMNS = [
    ("SK_ID_BUREAU", "long"), ("MONTHS_BALANCE", "long"), ("STATUS", "string"),
]
BUREAU_BALANCE_SCHEMA = _struct(BUREAU_BALANCE_COLUMNS)


# Message Kafka. Cột số được đọc là double (trừ khóa SK_ID_*) vì JSON của dữ liệu sinh có thể ghi 3.0 cho cột
# nguyên, mà from_json đọc "3.0" vào cột long sẽ ra null. Số nguyên dưới 2^53 biểu diễn chính xác bằng double.
_KEYS = ("SK_ID_CURR", "SK_ID_BUREAU")


def _message_fields(columns, keep=None, drop=()):
    fields = []
    for name, kind in columns:
        if name in drop or (keep is not None and name not in keep):
            continue
        data_type = StringType() if kind == "string" else LongType() if name in _KEYS else DoubleType()
        fields.append(StructField(name, data_type, True))
    return fields


# chỉ khai báo cột pipeline dùng; from_json bỏ qua các trường thừa trong message
_APPLICATION_MESSAGE = StructType(_message_fields(
    APPLICATION_COLUMNS, keep=set(MODEL_APPLICATION_COLUMNS), drop=("TARGET",)))
_BALANCE_MESSAGE = StructType([StructField("MONTHS_BALANCE", LongType(), True), StructField("STATUS", StringType(), True)])
_LOAN_MESSAGE = StructType(
    _message_fields(BUREAU_COLUMNS, drop=("SK_ID_CURR",)) + [StructField("balance", ArrayType(_BALANCE_MESSAGE), True)])

MESSAGE_SCHEMA = StructType([
    StructField("schema_version", LongType(), True),
    StructField("application_id", StringType(), True),
    StructField("event_time", StringType(), True),       # ISO 8601, UTC, ghi lúc gửi
    StructField("application", _APPLICATION_MESSAGE, True),
    StructField("bureau", ArrayType(_LOAN_MESSAGE), True),
])

OUTCOME_SCHEMA = StructType([
    StructField("application_id", StringType(), True),
    StructField("TARGET", LongType(), True),
    StructField("outcome_time", StringType(), True),
])
