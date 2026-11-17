"""Spark Structured Streaming: Kafka -> HDFS data lake (Parquet).

Reads every social.* topic plus brand.metrics, parses the JSON payloads against
a fixed schema, de-duplicates on post_id within a watermark and lands them in
HDFS partitioned by platform and date.

    python processing/stream_to_hdfs.py            # run continuously
    python processing/stream_to_hdfs.py --once     # drain what is in Kafka, then exit

In Docker it is submitted to the standalone cluster (SPARK_MASTER=spark://spark-master:7077);
outside Docker it runs in local mode (local[*]) from the pip-installed pyspark.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os  # noqa: E402
# Spark workers must use this same interpreter (matters on Windows / in venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import (HDFS_BRAND_METRICS, HDFS_CHECKPOINTS, HDFS_POSTS, HDFS_REPLICATION,  # noqa: E402
                           KAFKA_BOOTSTRAP, SPARK_DRIVER_MEMORY, SPARK_KAFKA_PACKAGE,
                           SPARK_MASTER, TOPIC_BRAND_METRICS, TOPIC_PREFIX)
from processing.schemas import METRIC_SCHEMA, POST_SCHEMA  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="process available data then stop")
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--trigger", default="15 seconds")
    a = ap.parse_args()

    builder = (SparkSession.builder.appName("SocialScope-Stream-Ingest")
               .master(SPARK_MASTER)
               .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.hadoop.dfs.replication", HDFS_REPLICATION))
    if SPARK_KAFKA_PACKAGE:  # empty in Docker, where the connector jars are baked into the image
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def kafka(subscribe_opt: dict):
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", a.bootstrap)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .options(**subscribe_opt)
                .load())

    trig = {"availableNow": True} if a.once else {"processingTime": a.trigger}

    # ------------------------------------------------------------------ posts
    posts = (kafka({"subscribePattern": TOPIC_PREFIX.replace(".", r"\.") + ".*"})
             .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("p"),
                     F.col("timestamp").alias("ingested_at"))
             .select("p.*", "ingested_at")
             .where(F.col("post_id").isNotNull())
             .withColumn("created_ts", F.to_timestamp("created_at"))
             .withColumn("date", F.to_date("created_ts"))
             .withWatermark("ingested_at", "10 minutes")
             .dropDuplicates(["post_id"]))

    q_posts = (posts.writeStream.format("parquet")
               .option("path", HDFS_POSTS)
               .option("checkpointLocation", f"{HDFS_CHECKPOINTS}/posts")
               .partitionBy("platform", "date")
               .outputMode("append")
               .trigger(**trig)
               .start())

    # ------------------------------------------------------------------ follower snapshots
    metrics = (kafka({"subscribe": TOPIC_BRAND_METRICS})
               .select(F.from_json(F.col("value").cast("string"), METRIC_SCHEMA).alias("m"))
               .select("m.*")
               .where(F.col("brand").isNotNull()))

    q_metrics = (metrics.writeStream.format("parquet")
                 .option("path", HDFS_BRAND_METRICS)
                 .option("checkpointLocation", f"{HDFS_CHECKPOINTS}/brand_metrics")
                 .outputMode("append")
                 .trigger(**trig)
                 .start())

    # ------------------------------------------------------------------ console monitor
    if not a.once:
        (posts.groupBy("platform", "brand").count().writeStream
         .outputMode("complete").format("console").option("truncate", "false")
         .trigger(processingTime="60 seconds").start())

    for q in (q_posts, q_metrics):
        q.awaitTermination()
    print("stream ingest finished")


if __name__ == "__main__":
    main()
