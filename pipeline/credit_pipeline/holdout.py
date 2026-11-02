"""Tái tạo tập test hold-out của notebook huấn luyện, không cần chạy lại notebook."""
import pandas as pd
from sklearn.model_selection import train_test_split

from credit_pipeline.columns import REQUIRED_AMOUNT_COLUMNS  # noqa: F401  (module khác import tên này từ đây)


def build_holdout_ids(application_csv_path):
    """Trả về DataFrame [SK_ID_CURR, TARGET, split] cho mọi hồ sơ Cash loans.
    Phép chia 80/20 chỉ phụ thuộc số dòng và nhãn, nên lọc đúng thứ tự như notebook thì tập test giống hệt notebook.
    """
    application = pd.read_csv(application_csv_path, usecols=["SK_ID_CURR", "TARGET", "NAME_CONTRACT_TYPE"] + REQUIRED_AMOUNT_COLUMNS)
    application = application.dropna(subset=REQUIRED_AMOUNT_COLUMNS)
    cash = application[application["NAME_CONTRACT_TYPE"] == "Cash loans"].reset_index(drop=True)

    train_index, test_index = train_test_split(
        cash.index, test_size=0.2, random_state=42, stratify=cash["TARGET"]
    )
    holdout_ids = cash[["SK_ID_CURR", "TARGET"]].copy()
    holdout_ids["split"] = "train"
    holdout_ids.loc[test_index, "split"] = "test"
    return holdout_ids
