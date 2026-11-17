# syntax=docker/dockerfile:1.7
# Spark 3.5.3 + the Python deps the SocialScope jobs need, with the Kafka
# connector jars baked in so jobs need no --packages download at runtime.
FROM apache/spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir --retries 10 --timeout 120 \
        "pandas<2.3" "numpy<2" "pyarrow==17.0.0" vaderSentiment==3.3.2 \
        pymongo==4.10.1 "elasticsearch==8.15.1" neo4j==5.27.0 kafka-python==2.0.6

RUN python3 - <<'EOF'
import time
import urllib.request
base = "https://repo1.maven.org/maven2"
jars = [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
]
for j in jars:
    for attempt in range(8):  # flaky networks: retry each jar
        try:
            urllib.request.urlretrieve(f"{base}/{j}", "/opt/spark/jars/" + j.rsplit("/", 1)[1])
            break
        except Exception as e:
            print(f"retry {attempt + 1} for {j}: {e}")
            time.sleep(5 * (attempt + 1))
    else:
        raise SystemExit(f"could not download {j}")
EOF

ENV PYTHONPATH=/app \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
    PATH="/opt/spark/bin:${PATH}"
WORKDIR /app
