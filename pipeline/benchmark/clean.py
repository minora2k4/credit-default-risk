"""Xóa dữ liệu do benchmark tạo ra trong cụm; không đụng tới dữ liệu của pipeline và giữ nguyên kết quả đo."""
from common import log, run


def main():
    kafka_topics = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092"]
    for topic in run([*kafka_topics, "--list"]).split():
        if topic.startswith("bench_"):
            run([*kafka_topics, "--delete", "--topic", topic], check=False)
            log(f"Đã xóa topic {topic}")
    run(["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "credit", "-d", "credit", "-c", "DROP DATABASE IF EXISTS credit_bench"])
    log("Đã xóa database credit_bench")
    run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", "/benchmark"], check=False)
    log("Đã xóa HDFS /benchmark")


if __name__ == "__main__":
    main()
