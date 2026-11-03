"""Chạy đúng simulator/producer.py của pipeline nhưng ghi vào topic benchmark. Chạy trong container simulator."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))

import producer  # noqa: E402

producer.APPLICATIONS_TOPIC = os.environ["BENCH_APPLICATIONS_TOPIC"]
producer.OUTCOMES_TOPIC = os.environ["BENCH_OUTCOMES_TOPIC"]
producer.main()
