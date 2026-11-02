-- Vai trò chỉ-đọc cho Grafana và các view giám sát. Chạy được NHIỀU LẦN (idempotent): make db-monitoring
-- Mật khẩu truyền vào qua biến psql (-v reader_password=...), không ghi cứng trong file này.

-- 1. Vai trò chỉ-đọc: Grafana không có quyền ghi hay xóa, và container Grafana không nắm mật khẩu superuser
SELECT format('CREATE ROLE grafana_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') \gexec
SELECT format('ALTER ROLE grafana_reader PASSWORD %L', :'reader_password') \gexec
GRANT CONNECT ON DATABASE credit TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;

-- 2. Nền: thêm nguồn dữ liệu (thật / sinh) và độ trễ end-to-end của từng hồ sơ
--    ID hồ sơ sinh nằm ngoài dải ID thật (>= 900,000,000), xem simulator/synthesize_pool.py
CREATE OR REPLACE VIEW v_predictions AS
SELECT p.*,
       CASE WHEN p.sk_id_curr >= 900000000 THEN 'synthetic' ELSE 'real_test' END AS source,
       EXTRACT(EPOCH FROM (p.scored_at - p.event_time)) AS latency_seconds   -- từ lúc gửi vào Kafka đến lúc có kết quả
FROM loan_predictions p;

-- 3. Thông lượng: số hồ sơ chấm điểm mỗi phút
CREATE OR REPLACE VIEW v_throughput_minute AS
SELECT date_trunc('minute', scored_at) AS time, source, status, count(*) AS applications
FROM v_predictions
GROUP BY 1, 2, 3;

-- 4. Độ trễ p50 / p95 mỗi phút. Hồ sơ tồn đọng trong Kafka (gửi từ lâu, chấm sau) có độ trễ rất lớn, đó là sự thật
CREATE OR REPLACE VIEW v_latency_minute AS
SELECT date_trunc('minute', scored_at) AS time, source, count(*) AS applications,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_seconds) AS p50_seconds,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_seconds) AS p95_seconds
FROM v_predictions
WHERE latency_seconds IS NOT NULL
GROUP BY 1, 2;

-- 5. Quyết định: tỉ lệ APPROVE / REJECT
CREATE OR REPLACE VIEW v_decision_summary AS
SELECT source, decision, count(*) AS applications, round(avg(probability)::numeric, 4) AS avg_probability,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY source), 2) AS percent
FROM v_predictions
WHERE status = 'SCORED'
GROUP BY source, decision;

-- 6. Phân bố điểm: 20 khoảng độ rộng 0.05
CREATE OR REPLACE VIEW v_score_histogram AS
SELECT source, (width_bucket(probability, 0, 1, 20) - 1) * 0.05 AS bucket_start, count(*) AS applications
FROM v_predictions
WHERE status = 'SCORED' AND probability IS NOT NULL
GROUP BY 1, 2;

-- 7. Chỉ số mô hình ONLINE: chỉ tính được cho hồ sơ đã có nhãn (nhãn đến trễ vào bảng loan_outcomes).
--    ROC-AUC tính bằng công thức Mann-Whitney: (tổng hạng của nhóm vỡ nợ - n1(n1+1)/2) / (n1 * n0),
--    dùng hạng trung bình cho các xác suất bằng nhau.
CREATE OR REPLACE VIEW v_online_metrics AS
WITH labelled AS (
    SELECT p.source, p.decision, o.target,
           rank() OVER (PARTITION BY p.source ORDER BY p.probability)
             + (count(*) OVER (PARTITION BY p.source, p.probability) - 1) / 2.0 AS average_rank
    FROM v_predictions p
    JOIN loan_outcomes o ON o.application_id = p.application_id
    WHERE p.status = 'SCORED'
), aggregated AS (
    SELECT source,
           count(*) AS labelled,
           count(*) FILTER (WHERE target = 1) AS defaults,
           sum(average_rank) FILTER (WHERE target = 1) AS rank_sum_defaults,
           count(*) FILTER (WHERE decision = 'REJECT' AND target = 1) AS true_positive,
           count(*) FILTER (WHERE decision = 'REJECT' AND target = 0) AS false_positive,
           count(*) FILTER (WHERE decision = 'APPROVE' AND target = 1) AS false_negative,
           count(*) FILTER (WHERE decision = 'APPROVE' AND target = 0) AS true_negative
    FROM labelled
    GROUP BY source
)
SELECT source, labelled, defaults,
       round((defaults::float8 / labelled)::numeric, 4) AS default_rate,
       round(((rank_sum_defaults - defaults * (defaults + 1) / 2.0)
              / NULLIF(defaults::float8 * (labelled - defaults), 0))::numeric, 4) AS roc_auc,
       round((true_positive::float8 / NULLIF(true_positive + false_positive, 0))::numeric, 4) AS precision_class1,
       round((true_positive::float8 / NULLIF(true_positive + false_negative, 0))::numeric, 4) AS recall_class1,
       round((2.0 * true_positive / NULLIF(2 * true_positive + false_positive + false_negative, 0))::numeric, 4) AS f1_class1,
       round(((true_positive + false_positive)::float8 / labelled)::numeric, 4) AS reject_rate,
       round((false_negative::float8 / NULLIF(false_negative + true_negative, 0))::numeric, 4) AS default_rate_among_approved
FROM aggregated;

-- 8. Số hồ sơ theo nguồn và trạng thái
CREATE OR REPLACE VIEW v_status_summary AS
SELECT source, status, count(*) AS applications, max(scored_at) AS last_scored_at
FROM v_predictions
GROUP BY 1, 2;

-- 9. grafana_reader chỉ đọc được các view này (view chạy với quyền của chủ sở hữu, nên không cần quyền trên bảng gốc)
GRANT SELECT ON v_predictions, v_throughput_minute, v_latency_minute, v_decision_summary,
                v_score_histogram, v_online_metrics, v_status_summary TO grafana_reader;
