#!/usr/bin/env bash
# Tạo các topic của pipeline; chạy lại an toàn nhờ --if-not-exists.
set -euo pipefail

BOOTSTRAP=kafka:9092
KT=/opt/kafka/bin/kafka-topics.sh

# loan_applications: mỗi message là một hồ sơ vay (JSON lồng nhau). 3 partition để Spark đọc song song; key là
#   application_id nên mỗi hồ sơ luôn vào cùng một partition. max.message.bytes 5 MB (mặc định 1 MB).
# loan_outcomes: nhãn vỡ nợ gửi sau một khoảng trễ, message nhỏ.
# Cả hai giữ dữ liệu 7 ngày.
for topic in loan_applications loan_outcomes; do
  extra=()
  if [ "$topic" = "loan_applications" ]; then
    extra=(--config max.message.bytes=5242880)
  fi
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config retention.ms=604800000 "${extra[@]}"
done

"$KT" --bootstrap-server "$BOOTSTRAP" --list
