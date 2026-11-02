"""Hàm dùng chung để tạo pool hồ sơ giả lập (phát lại tập test thật hoặc sinh dữ liệu tổng hợp)."""
import pandas as pd

BUREAU_BALANCE_CHUNK_ROWS = 2_000_000
MAX_MESSAGE_BYTES = 4 * 1024 * 1024   # topic cho phép 5 MB, chừa dư địa


def to_native(value):
    """NaN thành None (JSON null); kiểu numpy thành kiểu Python thường."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return value.item() if hasattr(value, "item") else value


def records_without_nan(frame):
    return [{key: to_native(value) for key, value in record.items()} for record in frame.to_dict("records")]


def read_balance_for_loans(bureau_balance_csv_path, loan_ids):
    """Đọc bureau_balance (27 triệu dòng) theo chunk, chỉ giữ các khoản vay trong loan_ids để tiết kiệm RAM."""
    loan_id_set = set(loan_ids)
    parts = []
    for chunk in pd.read_csv(
        bureau_balance_csv_path,
        dtype={"SK_ID_BUREAU": "int32", "MONTHS_BALANCE": "int16", "STATUS": "str"},
        chunksize=BUREAU_BALANCE_CHUNK_ROWS,
    ):
        parts.append(chunk[chunk["SK_ID_BUREAU"].isin(loan_id_set)])
    return pd.concat(parts, ignore_index=True)


def balance_by_loan(balance):
    """DataFrame balance thành {SK_ID_BUREAU: [(MONTHS_BALANCE, STATUS), ...]}."""
    return {
        loan_id: list(zip(group["MONTHS_BALANCE"].tolist(), group["STATUS"].tolist()))
        for loan_id, group in balance.groupby("SK_ID_BUREAU")
    }
