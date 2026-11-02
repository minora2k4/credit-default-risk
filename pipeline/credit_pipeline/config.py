"""Đường dẫn HDFS, tên topic Kafka và thông tin mô hình dùng chung giữa các job."""
import json

DATALAKE = "/datalake"
SILVER = f"{DATALAKE}/silver"
GOLD = f"{DATALAKE}/gold"
BRONZE_STREAM = f"{DATALAKE}/bronze/stream"
REFERENCE_NOTEBOOK = f"{DATALAKE}/reference/notebook"
REFERENCE_HOLDOUT = f"{DATALAKE}/reference/holdout_ids"
CHECKPOINTS = "/checkpoints"

KAFKA_BOOTSTRAP = "kafka:9092"
APPLICATIONS_TOPIC = "loan_applications"
OUTCOMES_TOPIC = "loan_outcomes"

MODEL_DIR = "/models/credit_default/lightgbm/v1"
MODEL_METADATA_PATH = f"{MODEL_DIR}/metadata.json"
MODEL_ONNX_PATH = f"{MODEL_DIR}/model.onnx"
MODEL_VERSION = "lightgbm-v1"


def load_model_metadata(spark, path=MODEL_METADATA_PATH):
    """Đọc metadata.json của mô hình từ HDFS: 221 đặc trưng theo đúng thứ tự, ngưỡng quyết định, chỉ số tham chiếu."""
    return json.loads("\n".join(spark.sparkContext.textFile(path).collect()))
