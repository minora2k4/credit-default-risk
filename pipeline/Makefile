# Điều khiển toàn bộ pipeline. Chạy `make help` để xem các lệnh.
.DEFAULT_GOAL := help

NN := docker compose exec -T namenode
PG := docker compose exec -T postgres psql -U credit -d credit -t -A
PROFILES := --profile stream --profile ui --profile live --profile sim --profile tools
TABLES := application_train bureau bureau_balance
MODEL_DIR := /models/credit_default/lightgbm/v1
CORE_SERVICES := namenode datanode1 kafka postgres spark-master spark-worker-1
# môi trường có pandas, scikit-learn, IPython để chạy notebook (dùng cho make reference)
HOST_PYTHON ?= python3

# tham số của make simulate, ví dụ: make simulate SOURCE=synthetic RATE=300 LIMIT=0
SOURCE ?= real-test
RATE ?= 100
LIMIT ?= 1000
OUTCOME_DELAY ?= 15
STAGE ?= cash

.PHONY: help build up stop ps clean all all-replay \
        hdfs-ingest hdfs-holdout hdfs-reference hdfs-verify topics \
        sim-pool-real sim-pool-synth reference \
        silver features score compare \
        stream-up stream-down stream-logs simulate stream-wait verify \
        db-monitoring grafana-up ui-up live-start live-stop live-logs

help: ## Liệt kê các lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

# --- Hạ tầng ---------------------------------------------------------------------------------------------------

build: ## Build image Spark, simulator và synthesizer
	docker compose --profile sim --profile tools build

up: ## Bật HDFS, Kafka, PostgreSQL, Spark và chờ chúng sẵn sàng
	docker compose up -d --wait $(CORE_SERVICES)

ui-up: ## Bật các giao diện: Kafka UI (8089), Adminer (8090), Grafana (3000)
	docker compose --profile ui up -d kafka-ui adminer grafana

stop: ## Dừng mọi container, giữ nguyên dữ liệu trong volume
	docker compose $(PROFILES) stop

ps: ## Trạng thái các container
	docker compose $(PROFILES) ps

clean: ## XÓA mọi container và dữ liệu (HDFS, Kafka, PostgreSQL, Grafana); hỏi xác nhận
	@read -p "Xóa toàn bộ container và dữ liệu trong volume? Gõ 'xoa' để xác nhận: " answer; \
	[ "$$answer" = "xoa" ] || { echo "Đã hủy"; exit 1; }
	docker compose $(PROFILES) down -v

# --- HDFS ------------------------------------------------------------------------------------------------------

hdfs-ingest: ## Đưa CSV gốc và mô hình ONNX lên HDFS (chạy lại an toàn)
	@echo "Đang đưa dữ liệu gốc và mô hình lên HDFS"
	$(NN) hdfs dfs -mkdir -p /datalake/silver /datalake/gold /datalake/reference /checkpoints $(MODEL_DIR)
	@for t in $(TABLES); do \
		$(NN) hdfs dfs -mkdir -p /datalake/bronze/batch/$$t && \
		$(NN) hdfs dfs -put -f /raw/$$t.csv /datalake/bronze/batch/$$t/ && echo "  đã đưa $$t.csv" || exit 1; \
	done
	$(NN) hdfs dfs -put -f /artifacts/LightGBM/model.onnx /artifacts/LightGBM/metadata.json $(MODEL_DIR)/
	@echo "  đã đưa model.onnx và metadata.json"

hdfs-holdout: ## Đưa holdout_ids (tập test của notebook) lên HDFS
	$(NN) hdfs dfs -mkdir -p /datalake/reference/holdout_ids
	$(NN) hdfs dfs -put -f /work/holdout_ids.parquet /datalake/reference/holdout_ids/
	@echo "Đã đưa holdout_ids lên HDFS"

hdfs-reference: ## Đưa các bảng tham chiếu của notebook (make reference) lên HDFS
	$(NN) hdfs dfs -mkdir -p /datalake/reference/notebook
	$(NN) hdfs dfs -put -f /work/reference/application_features.parquet /work/reference/bureau_balance_features.parquet /work/reference/bureau_features.parquet /work/reference/application_train_cash.parquet /datalake/reference/notebook/
	@echo "Đã đưa 4 bảng tham chiếu lên HDFS"

hdfs-verify: ## So md5 dữ liệu trên HDFS với file gốc và kiểm tra fsck
	$(NN) hdfs dfsadmin -safemode wait
	@for t in $(TABLES); do \
		a=$$($(NN) bash -c "hdfs dfs -cat /datalake/bronze/batch/$$t/$$t.csv | md5sum" | cut -d' ' -f1); \
		b=$$(md5sum ../data/$$t.csv | cut -d' ' -f1); \
		[ "$$a" = "$$b" ] && echo "$$t: md5 khớp" || { echo "LỖI $$t: md5 lệch"; exit 1; }; \
	done
	@for m in model.onnx metadata.json; do \
		a=$$($(NN) bash -c "hdfs dfs -cat $(MODEL_DIR)/$$m | md5sum" | cut -d' ' -f1); \
		b=$$(md5sum ../mlops_artifacts/LightGBM/$$m | cut -d' ' -f1); \
		[ "$$a" = "$$b" ] && echo "$$m: md5 khớp" || { echo "LỖI $$m: md5 lệch"; exit 1; }; \
	done
	@$(NN) hdfs fsck /datalake | grep -E 'Total blocks|Status'
	@$(NN) hdfs fsck /models | grep -E 'Total blocks|Status'

# --- Kafka và dữ liệu giả lập ----------------------------------------------------------------------------------

topics: ## Tạo topic loan_applications và loan_outcomes (chạy lại an toàn)
	docker compose run --rm kafka-init

sim-pool-real: ## Tạo pool 55,644 hồ sơ thật của tập test và holdout_ids
	docker compose --profile sim run --rm simulator python simulator/build_pool.py

sim-pool-synth: ## Sinh pool dữ liệu tổng hợp bằng SDV (khoảng 5 phút)
	docker compose --profile sim run --rm synthesizer python simulator/synthesize_pool.py

simulate: ## Gửi hồ sơ vào Kafka. Tham số: SOURCE=real-test|synthetic RATE= LIMIT= (0 là hết pool) OUTCOME_DELAY=
	docker compose --profile sim run --rm simulator python simulator/producer.py --source $(SOURCE) --rate $(RATE) --limit $(LIMIT) --outcome-delay $(OUTCOME_DELAY)

# --- Spark batch -----------------------------------------------------------------------------------------------

silver: ## Làm sạch CSV gốc thành Parquet (tầng silver)
	docker compose --profile tools run --rm spark-client spark-submit jobs/bronze_to_silver.py

features: ## Tạo bảng 221 đặc trưng (tầng gold)
	docker compose --profile tools run --rm spark-client spark-submit jobs/build_features.py

score: ## Chấm điểm bằng ONNX và kiểm tra ROC-AUC khớp notebook
	docker compose --profile tools run --rm spark-client spark-submit jobs/batch_score.py

reference: ## Xuất bảng tham chiếu từ notebook (chạy ngoài Docker, cần HOST_PYTHON và khoảng 6 GB RAM)
	$(HOST_PYTHON) tools/export_reference_features.py

compare: ## So đặc trưng Spark với notebook. Tham số: STAGE=application|bureau_balance|bureau|cash
	docker compose --profile tools run --rm spark-client spark-submit tools/compare_features.py --stage $(STAGE)

# --- Spark streaming -------------------------------------------------------------------------------------------

stream-up: ## Bật job streaming chấm điểm
	docker compose --profile stream up -d spark-stream

stream-down: ## Dừng job streaming (checkpoint vẫn còn nên bật lại sẽ chạy tiếp)
	docker compose --profile stream stop spark-stream

stream-logs: ## Xem log streaming (Ctrl+C để thoát, job vẫn chạy)
	docker compose --profile stream logs -f --tail=30 spark-stream

stream-wait: ## Chờ stream chấm xong 55,644 hồ sơ test thật (tối đa 20 phút)
	@echo "Đang chờ stream chấm hồ sơ test thật"; \
	for i in $$(seq 1 120); do \
		scored=$$($(PG) -c "SELECT count(*) FROM loan_predictions WHERE sk_id_curr < 900000000"); \
		labelled=$$($(PG) -c "SELECT count(*) FROM loan_outcomes WHERE application_id::bigint < 900000000"); \
		echo "  đã chấm $$scored/55644 hồ sơ, $$labelled nhãn"; \
		[ "$$scored" -ge 55644 ] && [ "$$labelled" -ge 55644 ] && exit 0; \
		sleep 10; \
	done; echo "LỖI: hết thời gian chờ stream"; exit 1

verify: ## Kiểm tra streaming khớp batch. Tham số: SOURCE=real-test|synthetic
	docker compose --profile tools run --rm spark-client spark-submit tools/verify_stream.py --source $(SOURCE)

# --- Giám sát và chạy liên tục ---------------------------------------------------------------------------------

db-monitoring: ## Tạo vai trò chỉ-đọc cho Grafana và các view giám sát trong PostgreSQL
	@set -a; . ./.env.grafana.local; set +a; \
	docker compose exec -T postgres psql -U credit -d credit -v ON_ERROR_STOP=1 -v reader_password="$$GRAFANA_DB_PASSWORD" < docker/postgres/02_monitoring.sql

grafana-up: ## Bật Grafana (http://localhost:3000, người dùng admin, mật khẩu trong .env.grafana.local)
	docker compose --profile ui up -d grafana

live-start: ## Bật producer chạy liên tục (hồ sơ mới mỗi giây) để giám sát
	docker compose --profile live up -d simulator-live

live-stop: ## Dừng producer chạy liên tục
	docker compose --profile live stop simulator-live

live-logs: ## Xem log producer chạy liên tục (Ctrl+C để thoát, producer vẫn chạy)
	docker compose --profile live logs -f --tail=20 simulator-live

# --- Chạy toàn bộ ----------------------------------------------------------------------------------------------

all: build up hdfs-ingest topics sim-pool-real hdfs-holdout silver features score stream-up all-replay stream-wait verify db-monitoring grafana-up sim-pool-synth ## Chạy toàn bộ pipeline từ trạng thái trống (khoảng 40 đến 60 phút)
	@echo "Hoàn tất. Grafana: http://localhost:3000. Chạy liên tục để xem: make live-start"

all-replay:
	$(MAKE) simulate SOURCE=real-test RATE=150 LIMIT=0 OUTCOME_DELAY=10
