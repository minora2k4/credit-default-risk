# Pipeline chấm điểm rủi ro vỡ nợ theo thời gian thực

Pipeline DE đưa mô hình LightGBM (đã huấn luyện ở [training](../training/README.md)) vào vận hành: nhận hồ sơ vay mới qua Kafka, tạo đặc trưng bằng Spark, chấm điểm bằng mô hình ONNX, lưu kết quả vào PostgreSQL và HDFS, và hiển thị trên Grafana.

## 1. Pipeline

- **Batch:** đọc dữ liệu gốc (CSV) từ HDFS, làm sạch, tạo bảng các đặc trưng và chấm điểm toàn bộ. Kết quả được dùng để kiểm chứng rằng Spark tạo đặc trưng giống hệt notebook huấn luyện.
- **Streaming:** nhận từng hồ sơ vay mới từ Kafka, làm sạch, tạo đặc trưng, chấm điểm bằng đúng các hàm của batch, rồi ghi kết quả. Nhãn vỡ nợ (đến sau) vào một topic riêng để tính chỉ số mô hình online.
- **Simulator:** bộ sinh hồ sơ phát vào Kafka, gồm tập test thật được split trong notebook, dữ liệu tổng hợp theo phân phối của dữ liệu gốc, và chế độ phát liên tục để giám sát.
- **Monitor:** dashboard Grafana về thông lượng, độ trễ, quyết định và chỉ số mô hình.

Mô hình chỉ cho ra **xác suất vỡ nợ**. Cột `decision` là quy tắc cắt ngưỡng 0.658 (lấy từ `metadata.json`, ngưỡng tối ưu F1 lúc huấn luyện): `REJECT` khi xác suất vượt ngưỡng, ngược lại `APPROVE`.

## 2. Kiến trúc

<p align="center"><img src="docs/architecture.svg" alt="Kiến trúc pipeline" width="100%"></p>

## 3. Công cụ

| Thành phần | Image / phiên bản | Vai trò |
|---|---|---|
| HDFS | `apache/hadoop:3.4.2` (1 NameNode, 1 DataNode, replication 1) | Lưu dữ liệu gốc, các tầng silver/gold, mô hình, checkpoint |
| Kafka | `apache/kafka:4.2.2` (KRaft, 1 node) | Hàng đợi hồ sơ và nhãn |
| Spark | `apache/spark:4.1.3-python3` (standalone, 1 master, 1 worker 4 core) | Batch và Structured Streaming |
| PostgreSQL | `postgres:17.11-alpine` | Kết quả chấm điểm để tra cứu và cho Grafana |
| Grafana | `grafana/grafana-oss:12.4.3` | Dashboard giám sát |
| Kafka UI, Adminer | `kafbat/kafka-ui:v1.5.0`, `adminer:6.1.2` | Xem message và duyệt bảng |
| Mô hình | LightGBM xuất ONNX, chạy bằng `onnxruntime 1.23.2` trong `mapInPandas` | Chấm điểm |
| Sinh dữ liệu | SDV 2.0.0 (GaussianCopula) | Hồ sơ tổng hợp |

## 4. Dữ liệu trên HDFS

```
/datalake/bronze/batch/{application_train,bureau,bureau_balance}/   CSV gốc
/datalake/bronze/stream/{loan_applications,loan_outcomes}/batch_id=N/   message thô từ Kafka
/datalake/silver/{application,bureau,bureau_balance}/               Parquet đã làm sạch
/datalake/gold/features_cash/                                       278,220 hồ sơ × 221 đặc trưng, kèm TARGET và split
/datalake/gold/predictions_batch/                                   điểm chấm bằng batch
/datalake/gold/predictions_stream/batch_id=N/                       điểm chấm bằng streaming
/datalake/reference/{holdout_ids,notebook}/                         tập test và bảng tham chiếu của notebook
/models/credit_default/lightgbm/v1/{model.onnx,metadata.json}       mô hình
/checkpoints/{stream_applications,stream_outcomes}/                 checkpoint của streaming
```

## 5. Luồng batch

| Lệnh | Việc làm | Kết quả |
|---|---|---|
| `make silver` | Làm sạch CSV: đổi mã hưu trí `365243` thành null, sửa giá trị sai logic ở bureau, đổi trạng thái tháng thành mức quá hạn | 3 bảng Parquet, số dòng khớp CSV gốc |
| `make features` | Tạo 221 đặc trưng (tỉ lệ, tổng hợp điểm tín dụng ngoài, one-hot, gom bureau theo khách hàng, biến tương tác), lọc Cash loans | `gold/features_cash` kèm cột `split` train/test của notebook |
| `make score` | Chấm điểm bằng ONNX, tính ROC-AUC tập test | `gold/predictions_batch`, báo lỗi nếu ROC-AUC lệch notebook quá 1e-5 |
| `make compare STAGE=...` | So từng cột đặc trưng Spark với bảng tham chiếu xuất từ notebook (cần `make reference hdfs-reference` trước) | báo cột nào lệch, lệch bao nhiêu |

Các thao tác mà Spark khác so với pandas được xử lý riêng: chia cho 0 (Spark 4 bật ANSI nên báo lỗi), tổng của nhóm toàn null (pandas ra 0, Spark ra null), so sánh với null, độ lệch chuẩn tính theo dòng, và quy tắc điền giá trị thiếu theo tên cột.

## 6. Luồng streaming

Job `spark-stream` (file `jobs/stream_score.py`) chạy hai query, mỗi 6 giây (chọn theo [benchmark](#13-benchmark)):

1. **loan_applications:** lưu message thô vào bronze, parse JSON, loại bản trùng trong cùng batch (giữ offset lớn nhất), tách thành ba bảng hồ sơ, khoản vay, lịch sử tháng, chạy đúng các hàm của batch, rồi ghi `gold/predictions_stream` và upsert `loan_predictions`.
2. **loan_outcomes:** ghi nhãn vào bronze và upsert `loan_outcomes`.

Mỗi hồ sơ nhận một trạng thái: `SCORED` (Cash loans, đủ số tiền), `INVALID_INPUT` (thiếu số tiền cần thiết) hoặc `OUT_OF_SCOPE` (không phải Cash loans, mô hình không học loại này). Hồ sơ không chấm điểm vẫn được ghi nhận.

Log của job báo từng batch (số message, số hồ sơ theo trạng thái, thời gian xử lý) kèm tổng số hồ sơ đã chấm từ lúc khởi động, và cứ mỗi phút báo "vẫn đang chạy" khi không có hồ sơ mới. Lỗi được in rõ rồi job dừng; Docker khởi động lại và Spark chạy tiếp từ checkpoint.

## 7. Dữ liệu giả lập

| Nguồn | Lệnh | Mô tả | Dùng để |
|---|---|---|---|
| `real-test` | `make sim-pool-real` | 55,644 hồ sơ thật của tập test hold-out | Chứng minh streaming khớp batch và notebook |
| `synthetic` | `make sim-pool-synth` | Hồ sơ sinh bằng SDV theo phân phối của toàn bộ 278,220 hồ sơ Cash | Thử tải, thử luồng |
| liên tục | `make live-start` | Phát lại pool sinh mãi mãi, mỗi hồ sơ một ID mới, đến khi `make live-stop` | Giám sát lâu dài trên Grafana |

Dữ liệu sinh: bảng hồ sơ dùng SDV GaussianCopula **fit riêng từng lớp TARGET** (fit chung làm mất quan hệ giữa điểm tín dụng và vỡ nợ); bảng bureau mượn khối lịch sử của một hồ sơ thật cùng lớp, đổi ID và nhiễu số tiền ±10%. Báo cáo độ giống phân phối nằm ở `work/sim_pool/synthetic/fidelity_report.txt`. Dữ liệu sinh để thử luồng, **không** dùng để đánh giá chất lượng mô hình, vì bộ sinh đã học từ cả phần dữ liệu dùng để huấn luyện.

## 8. Giám sát

| Giao diện | Địa chỉ | Xem gì |
|---|---|---|
| Grafana | http://localhost:3000 | Thông lượng, độ trễ, quyết định, phân bố điểm, chỉ số mô hình online |
| Kafka UI | http://localhost:8089 | Topic, message, phân bố partition |
| Adminer | http://localhost:8090 | Duyệt và truy vấn PostgreSQL |
| Spark master | http://localhost:8080 | Worker và ứng dụng đang chạy |
| Spark driver | http://localhost:4040 | Tab Structured Streaming: tốc độ vào, tốc độ xử lý |
| HDFS | http://localhost:9870 | DataNode, dung lượng, block |

## 9. Kết quả kiểm chứng

Số liệu đo trong quá trình phát triển, trên tập test thật 55,644 hồ sơ (tỉ lệ vỡ nợ 8.35%):

| Kiểm tra | Kết quả |
|---|---|
| ROC-AUC của kết quả batch và streaming so với notebook | Khớp trong dung sai 1e-5 (notebook: 0.7800977). Đây là bằng chứng gián tiếp rằng đặc trưng Spark giống notebook; muốn so từng cột dùng `make reference`, `make hdfs-reference` rồi `make compare` |
| Xác suất streaming so với batch | Lệch dưới 1e-6, quyết định giống hệt |
| ROC-AUC online (SQL) | 0.7801 |
| Precision / Recall / F1 lớp vỡ nợ (online) | 0.2672 / 0.4451 / 0.3339, so với notebook 0.2673 / 0.4453 / 0.3340 |
| Tỉ lệ REJECT | 13.9%, đúng bằng notebook |
| Kill job giữa chừng (SIGKILL) rồi bật lại | 3000/3000 hồ sơ có kết quả, không mất hồ sơ |
| Dữ liệu sinh | Tỉ lệ vỡ nợ 0.0835 khớp; KS trung bình các cột số 0.032; chạy xuyên suốt luồng, mọi hồ sơ `SCORED` |

## 10. Cấu trúc thư mục

```
pipeline/
├── Makefile                  điều khiển toàn bộ pipeline (make help)
├── docker-compose.yml        gộp cấu hình các công cụ
├── compose/                  hdfs, kafka, spark, postgres, simulator, monitoring
├── docker/                   Dockerfile và cấu hình từng công cụ (hadoop, kafka, spark, postgres, grafana, simulator, synth)
├── credit_pipeline/          mã dùng chung: làm sạch, đặc trưng, chấm điểm, ghi Postgres, log
├── jobs/                     job Spark: bronze_to_silver, build_features, batch_score, stream_score
├── simulator/                tạo pool, sinh dữ liệu, producer Kafka
├── tools/                    so sánh đặc trưng với notebook, kiểm tra kết quả streaming, xuất bảng tham chiếu
├── benchmark/                đo hiệu năng Kafka, Spark batch và streaming
├── scripts/aliases.sh        lệnh ngắn cho hdfs, kafka, psql trong container
└── work/                     dữ liệu trung gian trên máy (không đưa lên git)
```

## 11. Chạy pipeline

**Yêu cầu:** Linux, Docker với Compose v2.20 trở lên, `make`. Docker nên được cấp 8 CPU và 7 GB RAM (cả bộ chạy khoảng 4 đến 5 GB); cấp nhiều hơn dễ làm máy 15 GB hết RAM. Cần có sẵn `../data/` (CSV Home Credit) và `../mlops_artifacts/` (chạy notebook huấn luyện trước).

**Thiết lập lần đầu**, trong thư mục `pipeline/`:

```bash
cp .env.local.example .env.local              # đặt mật khẩu PostgreSQL
cp .env.grafana.local.example .env.grafana.local   # đặt mật khẩu Grafana
```

**Chạy toàn bộ từ đầu** (build, dựng cụm, nạp dữ liệu, batch, streaming, kiểm tra, Grafana):

```bash
make all
```

**Hoặc từng bước:**

```bash
make build && make up                       # dựng HDFS, Kafka, PostgreSQL, Spark
make hdfs-ingest topics                     # nạp dữ liệu, mô hình lên HDFS; tạo topic
make sim-pool-real hdfs-holdout             # tạo tập test thật, đưa holdout lên HDFS
make silver features score                  # batch; score báo lỗi nếu ROC-AUC lệch notebook
make stream-up                              # bật streaming
make simulate SOURCE=real-test RATE=150 LIMIT=0 OUTCOME_DELAY=10   # phát lại tập test thật
make stream-wait verify                     # chờ stream xong rồi kiểm tra khớp batch và notebook
make db-monitoring grafana-up               # view SQL và Grafana
```

**Giám sát liên tục:**

```bash
make sim-pool-synth        # một lần, tạo pool dữ liệu sinh
make live-start            # bật phát hồ sơ liên tục (LIVE_RATE hồ sơ/giây trong .env)
make live-logs             # xem log
make live-stop             # dừng
```

**Dừng và dọn dẹp:** 
``` bash
make stop   # dừng mọi container, giữ dữ liệu. 
make clean  # xóa cả container và dữ liệu trong volume (hỏi xác nhận).
```
## 12. Cấu hình

- `.env`: phiên bản image, cổng (chỉ mở trên `127.0.0.1`), kích thước Spark worker, tốc độ chế độ liên tục.
- `.env.local`, `.env.grafana.local`: mật khẩu (có file `.example` làm mẫu).
- `docker/spark/spark-defaults.conf`: cấu hình Spark mặc định.
- `jobs/stream_score.py`: trigger, số hồ sơ tối đa mỗi batch và tài nguyên của job streaming.
- Mỗi dịch vụ có giới hạn RAM (`mem_limit`) và healthcheck; các dịch vụ chờ nhau bằng `depends_on: service_healthy`.

## 13. Benchmark

Đo trên chính máy phát triển, bằng công cụ chuẩn của Kafka (`kafka-producer-perf-test`, `kafka-consumer-perf-test`, `kafka-e2e-latency`) và đúng các job Spark của pipeline. Benchmark Kafka và streaming dùng topic, database và thư mục HDFS riêng nên không đụng dữ liệu của pipeline. Benchmark batch chạy chính các job thật nên ghi đè `silver` và `gold` bằng cùng dữ liệu đó.

**Môi trường**

| Thành phần | Giá trị |
|---|---|
| CPU | Intel Core i5-11400H, 6 nhân 12 luồng, tối đa 4.5 GHz |
| RAM, Disk | 15 GB, NVMe |
| Docker Desktop | VM 10 CPU, 10 GB RAM |
| Cụm | Như mục 3 và `.env`: 1 broker Kafka (heap 512 MB, giới hạn 1 GB), 1 worker Spark (4 core, 3 GB), driver 1 GB, không có replication |

**Cách đo**

- Payload Kafka là 5,000 hồ sơ thật lấy mẫu ngẫu nhiên (trung bình 7.2 KB, p99 30 KB). Mỗi ô trong bảng là trung vị các lượt đo.
- Kafka: 3 lượt mỗi cấu hình, mỗi lượt khoảng 10 giây ở trạng thái ổn định, bỏ 10% record đầu.
- Spark batch: 3 lượt ở cấu hình mặc định (sau một lượt khởi động không tính), 2 lượt ở mỗi điểm sweep. Job tự kiểm tra số dòng và ROC-AUC nên mọi lượt được tính đều cho kết quả đúng.
- Streaming, năng lực: đọc tồn đọng hồ sơ thật, 2 lượt, bỏ 2 batch đầu.
- Streaming, tải đều: gửi 150 giây ở tốc độ cố định, bỏ 30 giây đầu. Đo 1 lượt; riêng mức 150/s của cấu hình cũ và các trigger 5, 6, 8 giây, cùng mức 300/s của cấu hình đã chọn, đo 3 lượt. Độ trễ tính từ lúc producer đóng dấu thời gian đến lúc dòng được ghi vào PostgreSQL.

**Giới hạn của số liệu**

- **Nhiệt:** CPU lên 94 đến 100°C trong mọi lượt đo, trung vị khoảng 6,000 lần giảm xung vì nhiệt mỗi lượt. Số liệu phản ánh hiệu năng khi máy đã nóng; tản nhiệt tốt hơn có thể cho kết quả cao hơn.
- **Một node, cùng máy:** broker và client chạy trên cùng một máy, không có độ trễ mạng thật và không có replication, nên `acks=all` không tốn thêm như trên cụm nhiều node.
- **Dữ liệu ở tốc độ cao:** các mức đánh dấu `*` vượt quá pool 55,644 hồ sơ thật nên dùng pool sinh, message nhỏ hơn khoảng 25%, do đó nhẹ hơn dữ liệu thật.
- **Cửa sổ ngắn:** mỗi mức tải đều chỉ đo 2 phút, chưa phải kiểm tra chạy dài.

### 13.1 Kafka

Ghi bằng một producer, chạy hết công suất, quanh cấu hình của pipeline (`acks=all`, idempotent, `zstd`, `linger=20`, batch 1 MB, 3 partition). Bảng dùng client Java của perf-test với thông số tương đương producer Python của pipeline (librdkafka, batch mặc định 1 MB):

| Thay đổi | record/s | MB/s (trước nén) | p50 ms | p99 ms | Nén (lần) |
|---|---:|---:|---:|---:|---:|
| **Cấu hình của pipeline** | **72,304** | **496** | **19** | **32** | **15.2** |
| `acks=1` | 73,035 | 501 | 19 | 32 | 15.2 |
| Không nén | 37,810 | 260 | 60 | 1,104 | 1.0 |
| `lz4` | 82,826 | 569 | 7 | 1,021 | 5.5 |
| `snappy` | 54,304 | 372 | 270 | 1,097 | 5.0 |
| `gzip` | 13,078 | 90 | 25 | 88 | 13.9 |
| `linger=0` | 40,555 | 278 | 4 | 21 | 13.0 |
| `linger=5` | 66,959 | 459 | 7 | 14 | 14.9 |
| `linger=50` | 68,866 | 472 | 21 | 41 | 15.3 |
| Batch 16 KB (mặc định của client Java) | 13,072 | 90 | 8 | 24 | 9.7 |
| Batch 64 KB | 41,726 | 286 | 3 | 10 | 13.4 |
| Batch 256 KB | 61,578 | 422 | 7 | 24 | 14.7 |
| 1 partition | 68,782 | 472 | 20 | 36 | 15.3 |
| 6 partition | 71,579 | 491 | 19 | 46 | 15.2 |

Các phép đo khác:

| Phép đo | Kết quả |
|---|---|
| 2 / 3 / 4 producer đồng thời | 111,165 / 128,755 / 147,307 record/s (763 / 885 / 1,012 MB/s); p99 56 / 894 / 630 ms; broker dùng khoảng 1 nhân CPU |
| Độ trễ producer theo tải (p50 / p99) | 150/s: 14 / 26 ms; 1,000/s: 13 / 25 ms; 36,187/s (50%): 17 / 28 ms; 68,346/s (90%): 19 / 35 ms |
| Đọc: 1 / 3 consumer | 132,153 / 256,979 record/s (907 / 1,763 MB/s) |
| Từ lúc gửi đến lúc đọc được, không tải | Trung bình 0.5 ms, p99 1 ms, p99.9 3 ms |
| Producer Python của pipeline | 22,912 hồ sơ/s; 55,644 hồ sơ thật (397 MB) cộng nhãn gửi xong trong 2.4 giây, trên đĩa còn 27 MB |

Kết luận:

- **Không cần đổi cấu hình producer.** `zstd` với batch 1 MB cho thông lượng cao và độ trễ ổn định. `lz4` nhanh hơn 15% nhưng chỉ nén được 5.5 lần nên ghi xuống đĩa nhiều gấp ba, và p99 lên tới hơn 1 giây (nhiều khả năng do nghẽn ghi đĩa); `snappy` và không nén cũng có p99 như vậy.
- **Kích thước batch quan trọng nhất:** batch 16 KB mặc định của client Java chậm hơn 5.5 lần so với batch 1 MB.
- **Giới hạn nằm ở phía client:** một tiến trình perf-test bị giới hạn bởi luồng nén của nó; nhiều producer đồng thời đạt khoảng 1 GB/s dữ liệu trước nén nhưng độ trễ tăng từ 3 producer trở lên.
- **Số luồng broker không có tác dụng:** `num.network.threads` 2 đến 6 và `num.io.threads` 4 đến 16 đều nằm trong khoảng 68,000 đến 69,300 record/s; `acks=1`, số partition và `linger` trên 20 ms cũng gần như không khác biệt.
- **Kafka không phải nút cổ chai của pipeline:** 150 hồ sơ/s chỉ bằng 0.2% công suất ghi. Giới hạn của cả pipeline nằm ở Spark.

### 13.2 Spark batch

Thời gian ứng dụng theo số core (giây; tổng 29.3 triệu dòng CSV, 679 MB, 278,220 hồ sơ × 221 đặc trưng):

| Số core | `silver` | `features` | `score` | Tổng |
|---:|---:|---:|---:|---:|
| 1 | 65.0 | 60.9 | 30.9 | 156.8 |
| 2 | 48.3 | 50.9 | 24.7 | 123.9 |
| **4 (mặc định)** | **32.6** | **46.1** | **21.4** | **100.1** |
| 6 | 30.0 | 45.8 | 25.2 | 101.0 |
| 8 | 28.5 | 47.5 | 22.4 | 98.4 |

Các tham số khác, so với mặc định (`silver` 31.0, `features` 47.3, `score` 21.1 giây; chênh giữa các lượt dưới 5%):

| Tham số | Kết quả |
|---|---|
| RAM executor 1 GB / 3 GB (mặc định 2 GB) | `features` 47.5 / 44.4 giây; 1 GB có spill 124 MB nhưng không chậm đi. GC chỉ khoảng 2% thời gian task |
| `spark.sql.shuffle.partitions` 8 đến 200 | `features` 44.5 đến 46.0 giây: gần như không đổi vì AQE tự gộp partition |
| Tắt AQE | `features` 43.0 giây, nhanh hơn 3 giây, nhưng streaming lại chậm đi 8% (mục 13.3) |
| 2 executor × 2 core | Chậm hơn: `silver` 36.8, `features` 58.7, `score` 24.4 giây |
| Arrow 1,000 / 50,000 record mỗi batch (mặc định 10,000) | `score` 23.8 / 20.9 giây; 50,000 tốn thêm khoảng 1.2 GB RAM |

Kết luận: **cấu hình hiện tại đã tối ưu, không đổi.** 4 core là điểm tới hạn: thêm core gần như không nhanh hơn. Các job còn có phần việc ngoài task không song song được (ở 1 core, khoảng 15 giây của `features` nằm ngoài task), và cùng khối lượng việc đó tốn nhiều thời gian CPU hơn khi tăng core (`features`: 45 giây ở 1 core, 135 giây ở 8 core). RAM không phải giới hạn: ở 2 GB không có spill và GC chỉ khoảng 2%.

### 13.3 Spark streaming

**Năng lực khi đọc tồn đọng** (hồ sơ thật, 2 core, executor 1 GB):

| Giới hạn hồ sơ mỗi batch | Hồ sơ/batch thực tế | Giây/batch | Hồ sơ/giây |
|---:|---:|---:|---:|
| 500 | 498 | 5.28 | 94 |
| 1,000 | 998 | 4.93 | 203 |
| 2,000 | 1,998 | 5.52 | 362 |
| 4,000 | 3,999 | 6.18 | 647 |
| 8,000 | 7,930 | 7.64 | 1,039 |
| 16,000 | 11,824 (pool hết trước khi đầy batch) | 9.82 | 1,204 |

Thời gian mỗi batch xấp xỉ **4.7 giây + 0.41 ms × số hồ sơ** (r = 0.99): chi phí cố định chiếm phần lớn. Ở batch 2,000 hồ sơ (5.5 giây), riêng việc dựng kế hoạch truy vấn cho 221 đặc trưng đã mất 1.9 giây, ghi bronze 0.16 giây, upsert PostgreSQL 0.13 giây; còn lại là tối ưu kế hoạch, tính đặc trưng, chấm điểm và ghi gold.

**Các tham số khác**, ở batch 2,000 hồ sơ (đối chứng 362 hồ sơ/s):

| Tham số | Hồ sơ/giây |
|---|---:|
| 1 / 3 / 4 core | 314 / 364 / 378 |
| Shuffle 4 / 16 partition (mặc định cũ 8) | 381 / 316 |
| RAM executor 2 GB | 355 |
| Tắt AQE | 335 |
| Tắt AQE, shuffle 4 | 384 |
| `spark.locality.wait=0` | 349 |
| Tắt whole-stage codegen | 274 |

Thêm core hay RAM gần như không giúp gì vì chi phí cố định phần lớn không song song được (riêng dựng kế hoạch ở driver đã 1.9 giây). Chỉ giảm shuffle xuống 4 là có ích, nhanh hơn khoảng 5%.

**Tải đều: độ trễ end-to-end** (giây, p50 / p95 / p99):

| Tốc độ (hồ sơ/s) | Cấu hình cũ: 10 s, batch 2,000 | **Đã chọn: 6 s, batch 8,000** | 10 s, batch 16,000 |
|---:|---:|---:|---:|
| 50 | 11.0 / 15.4 / 16.2 | **7.8 / 10.6 / 11.0** | |
| 100 | 10.3 / 14.7 / 15.4 | | |
| 150 | 10.3 / 14.9 / 15.6 | **7.8 / 10.5 / 11.2** | 10.1 / 14.6 / 15.2 |
| 190 | 10.7 / 15.1 / 15.9 | | |
| 250 | 42.6 / 56.3 / 60.3 (**không theo kịp**) | | |
| 300 | | **8.4 / 11.5 / 13.3** | |
| 450* | | 8.6 / 12.4 / 15.4 | |
| 550* | | 9.0 / 13.8 / 17.0 | |
| 600* | | | 11.7 / 16.4 / 18.1 |
| 650* | | 9.8 / 19.2 / 21.5 (sát giới hạn, batch 6.3 s) | |
| 900* | | | 12.4 / 17.4 / 20.7 |
| 1,100* | | | 20.9 / 33.1 / 35.7 (sát giới hạn, batch 10.9 s) |
| 1,300* | | | 28.2 / 36.7 / 38.8 (quá tải: còn tồn đọng khi producer dừng) |

Cấu hình cũ bị khóa trần ở 200 hồ sơ/s (2,000 hồ sơ mỗi 10 giây) dù năng lực xử lý cao hơn, nên ở 250/s độ trễ tăng dần và hồ sơ tồn đọng.

**Chọn trigger**, ở 150 hồ sơ/s (batch 8,000, shuffle 4):

| Trigger | p50 s | p95 s | Giây/batch | CPU worker | Nhận xét |
|---:|---:|---:|---:|---:|---|
| 5 giây | 7.5 | 10.1 | 5.06 | 110% | Batch lâu hơn trigger nên không còn dư địa; p95 chỉ thấp hơn 6 giây khoảng 4% |
| **6 giây** | **7.8** | **10.5** | **4.97** | **94%** | **Được chọn: dư khoảng 17%** |
| 8 giây | 8.9 | 12.5 | 5.10 | 83% | Chậm hơn, không được lợi gì thêm |
| 10 giây | 10.1 | 14.6 | 5.17 | 76% | Đo với giới hạn batch 16,000, không ảnh hưởng ở tải này |

Độ trễ xấp xỉ bằng nửa trigger cộng thời gian một batch, nên giảm trigger đến khi chạm thời gian batch là tốt nhất.

### 13.4 Cấu hình đã chọn

Đã áp dụng vào `jobs/stream_score.py` (`TRIGGER`, `MAX_APPLICATIONS_PER_BATCH`, `spark.sql.shuffle.partitions`):

| | Trước | Sau |
|---|---:|---:|
| Trigger | 10 giây | **6 giây** |
| Giới hạn hồ sơ mỗi batch | 2,000 | **8,000** |
| Số partition shuffle | 8 | **4** |
| Tốc độ cao nhất còn theo kịp | khoảng 190 hồ sơ/s | **khoảng 550 hồ sơ/s*** (gấp 2.9 lần) |
| Xử lý tồn đọng | 362 hồ sơ/s | **1,039 hồ sơ/s** (gấp 2.9 lần) |
| p50 / p95 ở 150 hồ sơ/s | 10.3 / 14.9 giây | **7.8 / 10.5 giây** (giảm 24% / 30%) |
| CPU worker ở 150 hồ sơ/s | 88% | 94% |

Đánh đổi: số micro-batch tăng từ 8,640 lên 14,400 mỗi ngày nên HDFS có nhiều thư mục nhỏ hơn. Giá trị shuffle lưu trong checkpoint, nên chỉ có hiệu lực khi tạo checkpoint mới; trigger và giới hạn batch có hiệu lực ngay.

Sau khi áp dụng, chạy lại pipeline thật từ batch đến streaming với 55,644 hồ sơ thật ở 150 hồ sơ/s: md5 trên HDFS khớp; ROC-AUC batch 0.7800977, lệch notebook 2.3e-08; so từng cột 278,220 hồ sơ với notebook lệch lớn nhất 4.6e-16; xác suất streaming trùng batch (lệch 0), ROC-AUC online 0.7800977.

Lưu ý khi đọc Grafana: cột `scored_at` được Spark chốt lúc bắt đầu tính toán, sớm hơn lúc dòng thực sự vào PostgreSQL khoảng 2 đến 3 giây, nên độ trễ trên Grafana thấp hơn độ trễ trong các bảng trên chừng đó.

Hướng cải thiện tiếp: chi phí cố định 4.7 giây mỗi batch là giới hạn lớn nhất, trong đó khoảng 1.9 giây để dựng kế hoạch truy vấn. Muốn giảm phần này cần viết lại cách tạo đặc trưng trong `features.py`, sau đó kiểm chứng lại với notebook.

### 13.5 Chạy lại benchmark

Cần pipeline đã chạy xong batch và có đủ pool (`make sim-pool-real sim-pool-synth`), rồi trong thư mục `pipeline/`:

```bash
make bench-kafka     # khoảng 1 giờ
make bench-batch     # khoảng 1 giờ
make bench-stream    # khoảng 2.5 giờ
make bench-report    # in bảng tổng hợp
make bench-clean     # xóa topic, database và thư mục HDFS của benchmark; giữ kết quả đo
```

Kết quả lưu từng lượt trong `work/benchmark/*.jsonl`. Chạy lại sẽ bỏ qua các lượt đã có, nên muốn đo lại từ đầu phải xóa các file này. Bộ đo tự dừng nếu máy còn dưới 800 MB RAM trống. Nên tắt các ứng dụng khác và đảm bảo máy thoáng khi đo.
