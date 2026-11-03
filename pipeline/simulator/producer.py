"""Gửi hồ sơ vay từ pool vào Kafka, kèm nhãn vỡ nợ gửi trễ vào một topic riêng.

  loan_applications  key = application_id, value = JSON hồ sơ kèm event_time (UTC lúc gửi)
  loan_outcomes      value = {application_id, TARGET, outcome_time}, gửi sau --outcome-delay giây để mô phỏng
                     việc chỉ biết hồ sơ có vỡ nợ hay không sau một thời gian

Nguồn dữ liệu: real-test (55,644 hồ sơ thật của tập test, chỉ phát một vòng) hoặc synthetic (dữ liệu sinh).
Chế độ --forever (chỉ với synthetic): phát lại pool nhiều vòng, mỗi hồ sơ mang ID mới, chạy đến khi nhận Ctrl+C hoặc
SIGTERM.

Chạy: make simulate (gửi số lượng cố định) hoặc make live-start (chạy liên tục)
"""
import argparse
import collections
import gzip
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from credit_pipeline.logs import make_logger

APPLICATIONS_TOPIC = "loan_applications"
OUTCOMES_TOPIC = "loan_outcomes"
REPORT_EVERY_SECONDS = 5
FOREVER_REPORT_EVERY_SECONDS = 30   # chạy liên tục thì log thưa hơn

# ID hồ sơ của chế độ liên tục nằm trong dải 1.0e9 đến 1.8e9, tách khỏi ID thật và ID của pool sinh;
# lấy theo mili giây hiện tại nên hai lần chạy khác nhau gần như không trùng ID

LIVE_ID_BASE = 1_000_000_000
LIVE_ID_SPAN = 800_000_000


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def with_event_time(line, event_time):
    """Chèn event_time vào cuối JSON object, không phải parse lại cả message."""
    line = line.rstrip()
    assert line.endswith("}")
    return f'{line[:-1]},"event_time":"{event_time}"}}'


def reassign_ids(line, new_id):
    """Đổi hồ sơ trong pool thành hồ sơ mới: application_id, SK_ID_CURR và SK_ID_BUREAU từng khoản vay.

    Giữ nguyên ID thì Postgres chỉ ghi đè dòng cũ và số hồ sơ không tăng. SK_ID_BUREAU cũng phải đổi: hai hồ sơ trong
    cùng một micro-batch mà trùng SK_ID_BUREAU thì lịch sử tháng bị gộp nhầm khi tính đặc trưng.
    """
    message = json.loads(line)
    message["application_id"] = str(new_id)
    message["application"]["SK_ID_CURR"] = new_id
    for index, loan in enumerate(message["bureau"]):
        loan["SK_ID_BUREAU"] = new_id * 1000 + index     # một hồ sơ có tối đa 116 khoản vay
    return json.dumps(message, allow_nan=False, separators=(",", ":"))


def build_producer(bootstrap_servers):
    from confluent_kafka import Producer  # import muộn để --dry-run chạy được khi chưa cài confluent-kafka

    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",                         # chờ broker xác nhận ghi xong
        "enable.idempotence": True,            # gửi lại không sinh bản trùng, giữ thứ tự trong partition
        "compression.type": "zstd",
        "linger.ms": 20,                       # gom các message gần nhau thành một batch
        "message.max.bytes": 5 * 1024 * 1024,  # khớp max.message.bytes của topic loan_applications
    })


def check_topics(producer):
    """Dừng sớm với thông báo rõ ràng nếu Kafka chưa chạy hoặc chưa có topic."""
    try:
        metadata = producer.list_topics(timeout=10)
    except Exception as error:
        sys.exit(f"LỖI: không kết nối được Kafka ({error}). Kafka đã chạy chưa? (docker compose up -d kafka)")
    missing = [t for t in (APPLICATIONS_TOPIC, OUTCOMES_TOPIC) if t not in metadata.topics]
    if missing:
        sys.exit(f"LỖI: thiếu topic {missing}. Chạy `make topics` trước.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["real-test", "synthetic"], default="real-test")
    parser.add_argument("--pool-dir", default="work/sim_pool")
    parser.add_argument("--rate", type=float, default=100, help="số hồ sơ mỗi giây")
    parser.add_argument("--limit", type=int, default=1000, help="số hồ sơ cần gửi; 0 là gửi hết pool (hoặc không giới hạn nếu --forever)")
    parser.add_argument("--outcome-delay", type=float, default=15, help="số giây trễ trước khi gửi nhãn")
    parser.add_argument("--forever", action="store_true",
                        help="chạy liên tục: phát lại pool nhiều vòng, mỗi hồ sơ mang ID mới, đến khi nhận Ctrl+C / SIGTERM. "
                             "Chỉ dùng được với --source synthetic")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--dry-run", action="store_true", help="không kết nối Kafka, chỉ kiểm tra dữ liệu và tốc độ")
    args = parser.parse_args()
    if args.forever and args.source != "synthetic":
        sys.exit("LỖI: --forever chỉ dùng với --source synthetic; hồ sơ thật của tập test chỉ phát một vòng "
                 "vì đổi ID sẽ làm mất ý nghĩa tập test thật")
    if args.forever and args.limit == 1000:
        args.limit = 0     # giá trị mặc định 1000 không có nghĩa khi chạy liên tục
    report_every = FOREVER_REPORT_EVERY_SECONDS if args.forever else REPORT_EVERY_SECONDS

    log = make_logger("producer")
    # Ctrl+C hoặc `docker stop` (SIGTERM) chỉ đặt cờ; vòng lặp chính tự thoát ở điểm an toàn
    stop_requested = False

    def request_stop(signal_number, frame):
        nonlocal stop_requested
        stop_requested = True
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    source_dir = os.path.join(args.pool_dir, args.source.replace("-", "_"))
    outcomes = pd.read_parquet(os.path.join(source_dir, "outcomes.parquet"))
    target_by_id = dict(zip(outcomes["application_id"], outcomes["TARGET"].astype(int)))

    producer = None if args.dry_run else build_producer(args.bootstrap)
    if producer is not None:
        check_topics(producer)

    counters = {"delivered": 0, "errors": 0}

    def on_delivery(error, message):
        if error is not None:
            counters["errors"] += 1
            print(f"LỖI gửi {message.key()}: {error}", file=sys.stderr, flush=True)
        else:
            counters["delivered"] += 1

    def send(topic, key, value):
        if producer is None:
            return
        while True:
            try:
                producer.produce(topic, key=key, value=value.encode("utf-8"), on_delivery=on_delivery)
                return
            except BufferError:      # hàng đợi nội bộ đầy: chờ gửi bớt rồi thử lại
                producer.poll(0.5)

    pending_outcomes = collections.deque()   # (thời điểm đến hạn, application_id, TARGET), theo thứ tự gửi
    sent_applications = sent_outcomes = 0
    largest_message = 0
    last_new_id = 0
    started = last_report = time.monotonic()
    limit_text = args.limit or ("không giới hạn" if args.forever else "hết pool")
    log(f"Bắt đầu gửi từ nguồn {args.source}: {args.rate:g} hồ sơ/giây, giới hạn {limit_text}, nhãn đến sau {args.outcome_delay:g}s"
        f"{', chạy liên tục (dừng bằng Ctrl+C)' if args.forever else ''}"
        f"{', chế độ thử (không gửi)' if args.dry_run else ', Kafka ' + args.bootstrap}")

    def next_application_id():
        """ID mới tăng dần, không trùng trong cùng một lần chạy."""
        nonlocal last_new_id
        candidate = LIVE_ID_BASE + (time.time_ns() // 1_000_000) % LIVE_ID_SPAN
        last_new_id = max(candidate, last_new_id + 1)
        return last_new_id

    def flush_due_outcomes(now, force=False, wait=True):
        """Gửi các nhãn đã đến hạn. force gửi hết; wait=False gửi ngay không chờ đến hạn (khi đang dừng)."""
        nonlocal sent_outcomes
        while pending_outcomes and (force or pending_outcomes[0][0] <= now):
            due, application_id, target = pending_outcomes.popleft()
            remaining = due - time.monotonic()
            if force and wait and remaining > 0:
                time.sleep(remaining)
            value = json.dumps({"application_id": application_id, "TARGET": target, "outcome_time": utc_now_iso()})
            send(OUTCOMES_TOPIC, application_id, value)
            sent_outcomes += 1

    def pool_lines():
        """Các dòng của pool; chế độ liên tục thì lặp lại mãi."""
        while True:
            with gzip.open(os.path.join(source_dir, "applications.jsonl.gz"), "rt", encoding="utf-8") as pool_file:
                for pool_line in pool_file:
                    yield pool_line
            if not args.forever:
                return

    for line in pool_lines():
        if stop_requested or (args.limit and sent_applications >= args.limit):
            break
        # giữ nhịp: hồ sơ thứ i được gửi vào thời điểm started + i / rate
        wait = started + sent_applications / args.rate - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        base_id = line[line.index('"application_id":"') + 18:].split('"', 1)[0]
        target = target_by_id[base_id]
        if args.forever:
            application_id = str(next_application_id())
            line = reassign_ids(line, int(application_id))
        else:
            application_id = base_id
        message = with_event_time(line, utc_now_iso())
        largest_message = max(largest_message, len(message))
        send(APPLICATIONS_TOPIC, application_id, message)
        sent_applications += 1
        pending_outcomes.append((time.monotonic() + args.outcome_delay, application_id, target))
        if producer is not None:
            producer.poll(0)
        now = time.monotonic()
        flush_due_outcomes(now)
        if now - last_report >= report_every:
            log(f"Đã gửi {sent_applications:,} hồ sơ và {sent_outcomes:,} nhãn ({sent_applications / (now - started):.0f} hồ sơ/s), "
                f"lỗi {counters['errors']}")
            last_report = now

    send_elapsed = time.monotonic() - started
    if stop_requested:
        log(f"Nhận lệnh dừng: gửi ngay {len(pending_outcomes)} nhãn còn chờ rồi thoát")
    # dừng theo lệnh thì gửi ngay các nhãn chưa đến hạn, để hồ sơ nào đã gửi cũng có nhãn
    flush_due_outcomes(time.monotonic(), force=True, wait=not stop_requested)
    if producer is not None:
        remaining = producer.flush(60)
        if remaining:
            sys.exit(f"LỖI: còn {remaining} message chưa gửi được sau 60 giây")
    elapsed = time.monotonic() - started
    log(f"Hoàn tất: {sent_applications:,} hồ sơ và {sent_outcomes:,} nhãn trong {elapsed:.1f}s "
        f"(gửi hồ sơ {send_elapsed:.1f}s, {sent_applications / max(send_elapsed, 1e-9):.0f} hồ sơ/s), message lớn nhất "
        f"{largest_message / 1024:.1f} KB, Kafka xác nhận {counters['delivered']:,}, lỗi {counters['errors']}")
    if counters["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
