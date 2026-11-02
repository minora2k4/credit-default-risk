"""Chấm điểm toàn bộ bảng đặc trưng gold bằng mô hình ONNX và kiểm tra ROC-AUC tập test có khớp notebook không.

Khớp nghĩa là đặc trưng Spark và cách chia tập test đều giống notebook huấn luyện.
Chạy: make score
"""

from pyspark.sql import functions as F
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support, roc_auc_score

from credit_pipeline.config import GOLD, load_model_metadata
from credit_pipeline.logs import run, start_spark
from credit_pipeline.scoring import add_scores

AUC_TOLERANCE = 1e-5
FEATURES_PATH = f"{GOLD}/features_cash"
PREDICTIONS_PATH = f"{GOLD}/predictions_batch"
IDENTIFIERS = ["SK_ID_CURR", "TARGET", "split"]


def main(log):
    spark = start_spark("batch_score", log)
    metadata = load_model_metadata(spark)
    feature_names = metadata["feature_names"]
    threshold = metadata["decision_threshold"]
    log(f"Đã đọc mô hình: {len(feature_names)} đặc trưng, ngưỡng quyết định {threshold:.6f}")

    log("Đang chấm điểm bằng ONNX")
    features = spark.read.parquet(FEATURES_PATH)
    add_scores(spark, features, feature_names, threshold, IDENTIFIERS).coalesce(4).write.mode("overwrite").parquet(PREDICTIONS_PATH)
    predictions = spark.read.parquet(PREDICTIONS_PATH)
    decisions = {row["decision"]: row["count"] for row in predictions.groupBy("decision").count().collect()}
    log(f"Đã ghi {predictions.count():,} dòng vào {PREDICTIONS_PATH} | quyết định {decisions}")

    test = predictions.filter(F.col("split") == "test").select("TARGET", "probability").toPandas()
    y_true = test["TARGET"].to_numpy()
    probabilities = test["probability"].to_numpy()
    y_pred = (probabilities >= threshold).astype(int)

    auc = roc_auc_score(y_true, probabilities)
    expected_auc = metadata["onnx_roc_auc_test"]        # AUC của chính file ONNX, tính lúc huấn luyện
    expected = metadata["metrics_test"]
    precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, labels=[1], zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    log(f"Tập test {len(test):,} hồ sơ, tỉ lệ vỡ nợ {y_true.mean():.4f}")
    log(f"ROC-AUC {auc:.7f} (notebook {expected_auc:.7f}) | Precision lớp 1 {precision[0]:.4f} (notebook {expected['precision']:.4f}) | "
        f"Recall {recall[0]:.4f} ({expected['recall']:.4f}) | F1 {f1[0]:.4f} ({expected['f1']:.4f})")
    log(f"Confusion matrix: TN={tn:,} FP={fp:,} FN={fn:,} TP={tp:,}")

    difference = abs(auc - expected_auc)
    spark.stop()
    if difference > AUC_TOLERANCE:
        raise RuntimeError(f"ROC-AUC lệch notebook {difference:.2e} (cho phép {AUC_TOLERANCE:.0e}): "
                           "đặc trưng hoặc tập test chưa khớp")
    log(f"Hoàn tất: ROC-AUC khớp notebook (lệch {difference:.2e})")


if __name__ == "__main__":
    run("batch_score", main)
