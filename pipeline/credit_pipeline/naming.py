"""Quy ước đặt tên cột và danh sách cột của các bảng đặc trưng (không phụ thuộc Spark)."""
import re

# 9 cột phân loại được one-hot
ONE_HOT_SOURCES = [
    "NAME_CONTRACT_TYPE", "NAME_FAMILY_STATUS", "NAME_EDUCATION_TYPE", "FLAG_OWN_CAR", "FLAG_OWN_REALTY",
    "NAME_HOUSING_TYPE", "NAME_INCOME_TYPE", "OCCUPATION_TYPE", "ORGANIZATION_TYPE",
]

# Các ký tự này được thay bằng "_" trong tên cột lúc huấn luyện (XGBoost/LightGBM không chấp nhận),
# nên giá trị phân loại cũng phải thay theo cách đó khi so khớp với tên đặc trưng.
SANITIZE_PATTERN = r'[\[\]{}":,]'

# one-hot loại hợp đồng: không phải đặc trưng của mô hình, chỉ dùng để tách Cash / Revolving
CONTRACT_DUMMIES = {
    "NAME_CONTRACT_TYPE_Cash loans": ("NAME_CONTRACT_TYPE", "Cash loans"),
    "NAME_CONTRACT_TYPE_Revolving loans": ("NAME_CONTRACT_TYPE", "Revolving loans"),
}


def sanitize_name(name):
    return re.sub(SANITIZE_PATTERN, "_", name)


def one_hot_specs(feature_names):
    """Trả về {tên cột one-hot: (cột nguồn, giá trị đã thay ký tự lạ)} từ danh sách đặc trưng của mô hình.

    Ví dụ "ORGANIZATION_TYPE_Industry_ type 1" cho ("ORGANIZATION_TYPE", "Industry_ type 1").
    Giá trị lạ (không có lúc huấn luyện) không khớp cột nào nên cho ra toàn 0.
    """
    specs = {}
    sources_longest_first = sorted(ONE_HOT_SOURCES, key=len, reverse=True)
    for name in feature_names:
        for source in sources_longest_first:
            if name.startswith(source + "_"):
                specs[name] = (source, name[len(source) + 1:])
                break
    return specs

# bureau_balance gom về cấp khoản vay
BB_LOAN_COLUMNS = [
    "SK_ID_BUREAU", "BB_N_MONTHS", "BB_WORST_12M", "BB_WORST_24M", "BB_WORST_EVER", "BB_N_DPD1_12M",
    "BB_N_DPD2_24M", "BB_N_DPD1_EVER", "BB_SHARE_X", "BB_SHARE_C", "BB_MONTHS_SINCE_DPD", "BB_EVER_DPD5",
    "BB_SHARE_DPD1",
]

# bureau gom về cấp khách hàng, trước khi điền giá trị thiếu
BUREAU_CUSTOMER_COLUMNS = [
    "SK_ID_CURR", "BUR_N_LOANS", "BUR_N_ACTIVE", "BUR_N_CLOSED", "BUR_EVER_BAD", "BUR_N_NEW_6M",
    "BUR_N_NEW_12M", "BUR_N_ACT_PAST_END", "BUR_N_CLOSED_LATE", "BUR_N_OVERDUE_NOW", "BUR_MAX_DPD_NOW",
    "BUR_EVER_PROLONG", "BUR_HAS_MORTGAGE", "BUR_HAS_CAR", "BUR_N_MICRO", "BUR_N_CARD", "BUR_MAX_OVD_AMT",
    "BB_WORST_12M", "BB_WORST_24M", "BB_WORST_EVER", "BB_EVER_DPD5", "BB_MONTHS_SINCE_DPD", "BB_SHARE_X",
    "BB_N_DPD1_12M", "BB_N_DPD2_24M", "HAS_BB", "BUR_ACTIVE_DEBT", "BUR_ACTIVE_CREDIT", "BUR_ACTIVE_ANNUITY",
    "BUR_ACTIVE_OVD_AMT", "BUR_CARD_DEBT", "BUR_CARD_LIMIT", "BUR_HIST_MONTHS", "BUR_MONTHS_SINCE_NEW",
    "BUR_MONTHS_SINCE_CLOSED", "BUR_ACTIVE_SHARE", "BUR_CLOSED_LATE_RATE", "BUR_DEBT_TO_CREDIT",
    "BUR_CARD_UTIL", "BUR_OVD_TO_DEBT", "BUR_DAYS_CREDIT_MEAN", "BUR_DAYS_CREDIT_ENDDATE_MEAN",
    "BUR_DAYS_CREDIT_ENDDATE_MAX", "BUR_DAYS_CREDIT_UPDATE_MEAN", "BUR_DAYS_ENDDATE_FACT_MEAN",
    "BUR_AMT_CREDIT_SUM_MEAN", "BUR_AMT_CREDIT_SUM_MAX", "BUR_AMT_CREDIT_SUM_TOTAL", "BUR_AMT_DEBT_MEAN",
    "BUR_AMT_DEBT_MAX", "BUR_AMT_MAX_OVERDUE_MEAN", "BUR_CNT_PROLONG_SUM", "BUR_BB_N_MONTHS_MEAN",
    "BUR_BB_SHARE_C_MEAN", "BUR_BB_SHARE_DPD1_MEAN", "BUR_ACT_DAYS_CREDIT_MEAN",
    "BUR_ACT_DAYS_CREDIT_ENDDATE_MEAN", "BUR_ACT_DAYS_UPDATE_MEAN", "BUR_ACT_AMT_CREDIT_SUM_MEAN",
    "BUR_CLS_DAYS_CREDIT_MEAN", "BUR_CLS_DAYS_CREDIT_MAX", "BUR_CLS_DAYS_ENDDATE_FACT_MAX",
    "BUR_CLS_AMT_CREDIT_SUM_MEAN",
]

# 23 cột thống kê mean/max của bureau: giữ null khi khách hàng không có lịch sử CIC
NUMERIC_STATS_COLUMNS = BUREAU_CUSTOMER_COLUMNS[-23:]
