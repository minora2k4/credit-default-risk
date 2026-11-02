"""Tạo pool phát lại 55,644 hồ sơ thật của tập test hold-out, mỗi hồ sơ là một JSON lồng nhau.

Một message gồm hồ sơ, các khoản vay tại tổ chức tín dụng khác và lịch sử tháng của từng khoản vay.
Dữ liệu giữ nguyên dạng thô, không có TARGET; việc làm sạch thuộc về Spark.

Chạy: make sim-pool-real
Đầu ra trong work/:
  holdout_ids.parquet                        SK_ID_CURR, TARGET, split của mọi hồ sơ Cash
  sim_pool/real_test/applications.jsonl.gz   mỗi dòng là một message gửi vào Kafka
  sim_pool/real_test/outcomes.parquet        application_id, TARGET của tập test (nhãn gửi trễ)
"""
import argparse
import gzip
import json
import os

import pandas as pd

from credit_pipeline.holdout import build_holdout_ids
from credit_pipeline.logs import run
from pool_common import MAX_MESSAGE_BYTES, balance_by_loan, read_balance_for_loans, records_without_nan


def main(log):
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--out-dir", default="work")
    args = parser.parse_args()
    pool_dir = os.path.join(args.out_dir, "sim_pool", "real_test")
    os.makedirs(pool_dir, exist_ok=True)

    log("Đang tái tạo tập test của notebook")
    holdout_ids = build_holdout_ids(os.path.join(args.data_dir, "application_train.csv"))
    holdout_ids.to_parquet(os.path.join(args.out_dir, "holdout_ids.parquet"), index=False)
    test = holdout_ids[holdout_ids["split"] == "test"]
    test_ids = test["SK_ID_CURR"].tolist()
    test_id_set = set(test_ids)
    log(f"{len(holdout_ids):,} hồ sơ Cash: train {(holdout_ids['split'] == 'train').sum():,}, test {len(test_ids):,}, "
        f"tỉ lệ vỡ nợ test {test['TARGET'].mean():.4f}")

    log("Đang đọc hồ sơ và các khoản vay của tập test")
    application = pd.read_csv(os.path.join(args.data_dir, "application_train.csv"))
    application = application[application["SK_ID_CURR"].isin(test_id_set)].set_index("SK_ID_CURR", drop=False)
    application = application.loc[test_ids].drop(columns=["TARGET"])      # giữ thứ tự của tập test
    bureau = pd.read_csv(os.path.join(args.data_dir, "bureau.csv"))
    bureau = bureau[bureau["SK_ID_CURR"].isin(test_id_set)]
    log(f"application {application.shape}, bureau {len(bureau):,} khoản vay của {bureau['SK_ID_CURR'].nunique():,} hồ sơ")

    log("Đang đọc lịch sử tháng (bureau_balance, 27 triệu dòng, đọc theo chunk)")
    balance = read_balance_for_loans(os.path.join(args.data_dir, "bureau_balance.csv"), set(bureau["SK_ID_BUREAU"]))
    log(f"bureau_balance: {len(balance):,} dòng của {balance['SK_ID_BUREAU'].nunique():,} khoản vay")
    balance_by_loan_id = balance_by_loan(balance)
    del balance

    bureau_by_application = {}
    for application_id, group in bureau.groupby("SK_ID_CURR"):
        loans = records_without_nan(group.drop(columns=["SK_ID_CURR"]))
        for loan in loans:
            loan["balance"] = [{"MONTHS_BALANCE": m, "STATUS": st} for m, st in balance_by_loan_id.get(loan["SK_ID_BUREAU"], [])]
        bureau_by_application[application_id] = loans

    sizes = []
    out_path = os.path.join(pool_dir, "applications.jsonl.gz")
    with gzip.open(out_path, "wt", encoding="utf-8") as out_file:
        for record in records_without_nan(application):
            message = {
                "schema_version": 1,
                "application_id": str(record["SK_ID_CURR"]),
                "application": record,
                "bureau": bureau_by_application.get(record["SK_ID_CURR"], []),
            }
            line = json.dumps(message, allow_nan=False, separators=(",", ":"))
            sizes.append(len(line.encode("utf-8")))
            out_file.write(line + "\n")
    if max(sizes) >= MAX_MESSAGE_BYTES:
        raise RuntimeError(f"Có message {max(sizes) / 1024 / 1024:.1f} MB, vượt giới hạn 4 MB của topic")
    log(f"Đã ghi {len(sizes):,} message vào {out_path} | kích thước trung bình {sum(sizes) / len(sizes) / 1024:.1f} KB, "
        f"lớn nhất {max(sizes) / 1024:.1f} KB")

    outcomes = test[["SK_ID_CURR", "TARGET"]].copy()
    outcomes["application_id"] = outcomes["SK_ID_CURR"].astype(str)
    outcomes[["application_id", "TARGET"]].to_parquet(os.path.join(pool_dir, "outcomes.parquet"), index=False)
    log(f"Hoàn tất: {len(outcomes):,} nhãn trong outcomes.parquet")


if __name__ == "__main__":
    run("build_pool", main)
