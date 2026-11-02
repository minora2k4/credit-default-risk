"""Kiểm tra kết quả streaming trong PostgreSQL.

  make verify SOURCE=real-test|synthetic

real-test: sau khi phát lại 55,644 hồ sơ thật của tập test, xác suất từng hồ sơ trong Postgres phải bằng xác suất của job
  batch (gold/predictions_batch) và ROC-AUC online phải bằng ROC-AUC của notebook huấn luyện.
synthetic: dữ liệu sinh không có điểm batch để so, nên chỉ báo cáo số hồ sơ, trạng thái, phân bố xác suất, tỉ lệ REJECT.
"""
import argparse

import pandas as pd
from pyspark.sql import functions as F
from sklearn.metrics import roc_auc_score

from credit_pipeline.config import GOLD, load_model_metadata
from credit_pipeline.logs import run, start_spark
from credit_pipeline.sinks import connect

SYNTHETIC_ID_OFFSET = 900_000_000   # hồ sơ sinh có SK_ID_CURR từ mức này trở lên
PROBABILITY_TOLERANCE = 1e-6
AUC_TOLERANCE = 1e-5


def read_postgres():
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT application_id, probability, decision, status FROM loan_predictions")
        predictions = pd.DataFrame(cursor.fetchall(), columns=["application_id", "probability", "decision", "status"])
        cursor.execute("SELECT application_id, target FROM loan_outcomes")
        outcomes = pd.DataFrame(cursor.fetchall(), columns=["application_id", "target"])
    for frame in (predictions, outcomes):
        frame["SK_ID_CURR"] = frame["application_id"].astype("int64")
    return predictions, outcomes


def verify_real_test(spark, metadata, log):
    batch = (
        spark.read.parquet(f"{GOLD}/predictions_batch").filter(F.col("split") == "test")
        .select("SK_ID_CURR", "TARGET", F.col("probability").alias("p_batch"), F.col("decision").alias("decision_batch"))
        .toPandas()
    )
    predictions, outcomes = read_postgres()
    streamed = predictions[predictions["SK_ID_CURR"].isin(set(batch["SK_ID_CURR"]))]   # bỏ hồ sơ sinh nằm cùng bảng

    merged = batch.merge(streamed.rename(columns={"probability": "p_stream", "decision": "decision_stream"}),
                         on="SK_ID_CURR", how="left")
    missing = int(merged["p_stream"].isna().sum())
    not_scored = int((streamed["status"] != "SCORED").sum())
    compared = merged.dropna(subset=["p_stream"])
    difference = (compared["p_stream"] - compared["p_batch"]).abs()
    decision_mismatch = int((compared["decision_stream"] != compared["decision_batch"]).sum())

    # ROC-AUC online chỉ tính cho hồ sơ đã có nhãn (nhãn đến trễ qua topic loan_outcomes)
    labelled = compared.merge(outcomes[["SK_ID_CURR", "target"]], on="SK_ID_CURR", how="inner")
    has_both_classes = labelled["target"].nunique() == 2
    online_auc = roc_auc_score(labelled["target"], labelled["p_stream"]) if has_both_classes else float("nan")
    batch_auc = roc_auc_score(labelled["TARGET"], labelled["p_batch"]) if has_both_classes else float("nan")
    expected_auc = metadata["onnx_roc_auc_test"]
    label_mismatch = int((labelled["target"] != labelled["TARGET"]).sum())

    total = len(batch)
    log(f"Tập test thật {total:,} hồ sơ: đã có trong Postgres {total - missing:,}, chưa có {missing:,}, không SCORED {not_scored:,}")
    log(f"Xác suất stream so với batch: lệch lớn nhất {difference.max():.2e}, "
        f"{int((difference > PROBABILITY_TOLERANCE).sum()):,} hồ sơ lệch quá {PROBABILITY_TOLERANCE:.0e}, "
        f"{decision_mismatch:,} quyết định khác")
    log(f"Tỉ lệ REJECT: stream {(compared['decision_stream'] == 'REJECT').mean():.4f}, batch {(batch['decision_batch'] == 'REJECT').mean():.4f}")
    log(f"Hồ sơ đã có nhãn {len(labelled):,}/{total:,}, nhãn lệch holdout {label_mismatch}")
    log(f"ROC-AUC online {online_auc:.7f} | batch cùng tập {batch_auc:.7f} | notebook {expected_auc:.7f}")

    problems = []
    if missing:
        problems.append(f"thiếu {missing:,} hồ sơ (stream còn đang xử lý thì chờ rồi chạy lại)")
    if not_scored:
        problems.append(f"{not_scored:,} hồ sơ không SCORED")
    if len(compared) and difference.max() > PROBABILITY_TOLERANCE:
        problems.append("xác suất stream khác batch")
    if decision_mismatch:
        problems.append("quyết định khác batch")
    if label_mismatch:
        problems.append("nhãn không khớp holdout")
    if len(labelled) < total:
        problems.append(f"mới có {len(labelled):,} nhãn, AUC chưa phải của toàn tập test")
    elif abs(online_auc - expected_auc) > AUC_TOLERANCE:
        problems.append(f"ROC-AUC online lệch notebook {abs(online_auc - expected_auc):.2e}")
    return problems


def verify_synthetic(log):
    predictions, _ = read_postgres()
    synthetic = predictions[predictions["SK_ID_CURR"] >= SYNTHETIC_ID_OFFSET]
    log(f"Hồ sơ sinh trong Postgres: {len(synthetic):,} | trạng thái {synthetic['status'].value_counts().to_dict()}")
    scored = synthetic[synthetic["status"] == "SCORED"]
    if len(scored):
        log(f"Xác suất: nhỏ nhất {scored['probability'].min():.4f}, trung vị {scored['probability'].median():.4f}, "
            f"lớn nhất {scored['probability'].max():.4f} | tỉ lệ REJECT {(scored['decision'] == 'REJECT').mean():.4f} (tập test thật: 0.1391)")
    return [] if len(synthetic) else ["chưa có hồ sơ sinh nào trong Postgres"]


def main(log):
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["real-test", "synthetic"], default="real-test")
    args = parser.parse_args()
    spark = start_spark("verify_stream", log)
    metadata = load_model_metadata(spark)
    log(f"Đang kiểm tra kết quả streaming ({args.source})")
    problems = verify_real_test(spark, metadata, log) if args.source == "real-test" else verify_synthetic(log)
    spark.stop()
    if problems:
        raise RuntimeError("; ".join(problems))
    log("Hoàn tất: streaming khớp batch và notebook" if args.source == "real-test" else "Hoàn tất")


if __name__ == "__main__":
    run("verify_stream", main)
