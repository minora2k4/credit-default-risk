-- Schema kết quả chấm điểm. Chạy một lần khi volume Postgres còn trống.
-- Khóa chính là application_id nên ghi lại một hồ sơ bao nhiêu lần cũng chỉ có một dòng (upsert):
-- đây là điều giúp streaming "đúng một lần" dù micro-batch bị chạy lại sau sự cố.

SET timezone = 'UTC';

CREATE TABLE loan_predictions (
    application_id   text PRIMARY KEY,                 -- = SK_ID_CURR dưới dạng chuỗi
    sk_id_curr       bigint,
    event_time       timestamptz,                      -- lúc hồ sơ được gửi vào Kafka (do producer ghi)
    scored_at        timestamptz NOT NULL DEFAULT now(),  -- lúc chấm điểm xong; scored_at - event_time = độ trễ end-to-end
    probability      double precision,                 -- xác suất vỡ nợ; NULL nếu không chấm điểm
    decision         text CHECK (decision IN ('APPROVE', 'REJECT')),
    threshold        double precision,                 -- ngưỡng quyết định tại thời điểm chấm
    status           text NOT NULL CHECK (status IN ('SCORED', 'INVALID_INPUT', 'OUT_OF_SCOPE')),
                                                       -- INVALID_INPUT: thiếu số tiền cần thiết; OUT_OF_SCOPE: không phải Cash loans
    model_version    text,
    kafka_partition  integer,                          -- vị trí trong Kafka, để truy vết khi cần
    kafka_offset     bigint
);
CREATE INDEX idx_loan_predictions_scored_at ON loan_predictions (scored_at);

CREATE TABLE loan_outcomes (
    application_id   text PRIMARY KEY,
    target           smallint NOT NULL CHECK (target IN (0, 1)),   -- 1 = vỡ nợ
    outcome_time     timestamptz                       -- lúc nhãn được biết (đến trễ so với hồ sơ)
);
