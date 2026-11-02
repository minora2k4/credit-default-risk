"""Ghi kết quả vào PostgreSQL bằng upsert, nên ghi lại cùng một hồ sơ không sinh dòng trùng."""
import os
from datetime import timezone

PREDICTION_COLUMNS = [
    "application_id", "sk_id_curr", "event_time", "scored_at", "probability", "decision", "threshold",
    "status", "model_version", "kafka_partition", "kafka_offset",
]
OUTCOME_COLUMNS = ["application_id", "target", "outcome_time"]


def connect():
    import psycopg   # import muộn vì chỉ các job ghi Postgres mới cần
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT_INTERNAL", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "credit"),
        user=os.environ.get("POSTGRES_USER", "credit"),
        password=os.environ["POSTGRES_PASSWORD"],   # lấy từ .env.local
    )


def _upsert_statement(table, columns, key):
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != key)
    return (f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({key}) DO UPDATE SET {updates}")


def _as_utc(value):
    """Spark trả datetime không múi giờ (phiên chạy ở UTC); gắn UTC để Postgres không hiểu nhầm."""
    return value.replace(tzinfo=timezone.utc) if value is not None and value.tzinfo is None else value


def _write(table, columns, rows):
    if not rows:
        return 0
    statement = _upsert_statement(table, columns, "application_id")
    with connect() as connection:       # một transaction cho cả micro-batch: ghi hết hoặc không ghi gì
        with connection.cursor() as cursor:
            cursor.executemany(statement, rows)
    return len(rows)


def upsert_predictions(rows):
    prepared = [
        tuple(_as_utc(row[column]) if column in ("event_time", "scored_at") else row[column] for column in PREDICTION_COLUMNS)
        for row in rows
    ]
    return _write("loan_predictions", PREDICTION_COLUMNS, prepared)


def upsert_outcomes(rows):
    prepared = [tuple(_as_utc(row[column]) if column == "outcome_time" else row[column] for column in OUTCOME_COLUMNS) for row in rows]
    return _write("loan_outcomes", OUTCOME_COLUMNS, prepared)
