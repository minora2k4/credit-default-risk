# Một image cho master, worker và driver: driver và executor phải cùng phiên bản Python và thư viện
# vì mapInPandas chuyển dữ liệu giữa JVM và Python worker bằng Arrow.
FROM apache/spark:4.1.3-python3

USER root
COPY requirements.txt /tmp/requirements.txt
# pip 22 của Ubuntu 22.04 đã cũ nên nâng lên trước
RUN python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
 # đặt tên cố định cho py4j (dạng .zip có sẵn trong image) để khai báo PYTHONPATH, vì ENV không dùng được glob
 && ln -s /opt/spark/python/lib/py4j-*-src.zip /opt/spark/python/lib/py4j-src.zip

# Kafka connector cho Structured Streaming, tải sẵn vào /opt/spark/jars lúc build thay vì dùng `--packages`
# (tải từ Maven mỗi lần chạy và có thể ra phiên bản khác nhau giữa các container).
# Phiên bản lấy từ POM của spark-sql-kafka-0-10_2.13:4.1.3; --checksum ghim SHA-256 nên file bị đổi thì build lỗi.
ADD --chmod=644 --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/

ENV PATH="${PATH}:/opt/spark/bin:/opt/spark/sbin" \
    PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-src.zip:/opt/pipeline" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/pipeline
