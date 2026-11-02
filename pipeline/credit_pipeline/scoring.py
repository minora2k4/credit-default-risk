"""Chấm điểm bằng mô hình ONNX trong Spark (mapInPandas + onnxruntime). Batch và streaming dùng chung."""
import os

import numpy as np
from pyspark import SparkFiles
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from credit_pipeline.config import MODEL_ONNX_PATH, MODEL_VERSION

# Mỗi Python worker giữ một session ONNX và tái dùng giữa các task. Chỉ truy cập qua _get_session
# (hàm cấp module, được pickle theo tham chiếu), nếu không dict bị sao chép theo closure và mất tác dụng cache.
_SESSIONS = {}
_REGISTERED_MODEL_PATHS = set()   # chỉ dùng ở driver: các mô hình đã addFile


def _get_session(model_path):
    session = _SESSIONS.get(model_path)
    if session is None:
        import onnxruntime as ort
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1    # Spark đã chạy song song nhiều task, nhiều luồng trong một task chỉ tranh CPU
        options.inter_op_num_threads = 1
        session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
        _SESSIONS[model_path] = session
    return session


def _probabilities_of_default(outputs):
    """Lấy xác suất lớp 1 (vỡ nợ) từ đầu ra của ONNX."""
    probabilities = outputs[1]
    if isinstance(probabilities, list):   # định dạng zipmap: mỗi dòng là dict {lớp: xác suất}
        probabilities = np.array([row[1] for row in probabilities])
    return np.asarray(probabilities)[:, 1]


def add_scores(spark, features, feature_names, threshold, passthrough_columns, model_version=MODEL_VERSION):
    """Thêm cột probability, decision (REJECT nếu >= ngưỡng, ngược lại APPROVE) và model_version.

    features cần có đủ feature_names; passthrough_columns là các cột định danh giữ lại ở kết quả.
    """
    # addFile tải model từ HDFS về từng executor; cần đủ scheme hdfs://, và chỉ gọi một lần cho mỗi đường dẫn
    model_uri = spark.conf.get("spark.hadoop.fs.defaultFS") + MODEL_ONNX_PATH
    if model_uri not in _REGISTERED_MODEL_PATHS:
        spark.sparkContext.addFile(model_uri)
        _REGISTERED_MODEL_PATHS.add(model_uri)
    model_file = os.path.basename(MODEL_ONNX_PATH)

    schema = StructType(
        [features.schema[name] for name in passthrough_columns]
        + [StructField("probability", DoubleType()), StructField("decision", StringType()),
           StructField("model_version", StringType())]
    )
    feature_names = list(feature_names)
    passthrough_columns = list(passthrough_columns)

    def score_batches(batches):
        session = _get_session(SparkFiles.get(model_file))
        input_name = session.get_inputs()[0].name
        for frame in batches:
            if frame.empty:
                continue
            matrix = frame[feature_names].to_numpy(dtype=np.float32)   # float32, giữ NaN, như lúc huấn luyện
            probabilities = _probabilities_of_default(session.run(None, {input_name: matrix}))
            result = frame[passthrough_columns].copy()
            result["probability"] = probabilities.astype("float64")
            result["decision"] = np.where(probabilities >= threshold, "REJECT", "APPROVE")
            result["model_version"] = model_version
            yield result

    return features.mapInPandas(score_batches, schema)
