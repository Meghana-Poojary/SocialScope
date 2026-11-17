"""Central configuration shared by ingestion, Spark jobs and the API.

Every value can be overridden with an environment variable so the same code
runs on the host (localhost ports) and inside docker-compose (service names).
"""
from __future__ import annotations
import os

# ---------------------------------------------------------------- platforms
PLATFORMS = ["instagram", "reddit", "youtube", "x"]

# ---------------------------------------------------------------- Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PREFIX = "social."                    # social.instagram, social.reddit, ...
TOPIC_BRAND_METRICS = "brand.metrics"       # daily follower snapshots


def topic_for(platform: str) -> str:
    return f"{TOPIC_PREFIX}{platform}"


# ---------------------------------------------------------------- Spark
# docker-compose sets these for the `spark` container; defaults suit a plain local run.
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
# Kafka connector for Structured Streaming, fetched from Maven on first run.
# Empty in Docker, where the jars are baked into the image.
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")

# ---------------------------------------------------------------- HDFS (data lake)
# Single-node HDFS (namenode + datanode containers). HDFS_BASE can point the
# lake elsewhere, e.g. file:///tmp/socialscope-lake for a quick local test.
HDFS_URL = os.getenv("HDFS_URL", "hdfs://localhost:9000")
HDFS_BASE = os.getenv("HDFS_BASE", f"{HDFS_URL}/socialscope")
# Spark is an HDFS *client*: it must be told the replication factor itself (default 3).
HDFS_REPLICATION = os.getenv("HDFS_REPLICATION", "1")
HDFS_POSTS = f"{HDFS_BASE}/raw/posts"
HDFS_BRAND_METRICS = f"{HDFS_BASE}/raw/brand_metrics"
HDFS_CHECKPOINTS = f"{HDFS_BASE}/checkpoints"
HDFS_CURATED = f"{HDFS_BASE}/curated/posts_enriched"

# ---------------------------------------------------------------- serving stores
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "socialscope")

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "socialscope-posts")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "socialscope123")

# ---------------------------------------------------------------- analytics knobs
INFLUENCER_MIN_FOLLOWERS = int(os.getenv("INFLUENCER_MIN_FOLLOWERS", "10000"))
# Datasets without follower counts (e.g. Kaggle Twitter support data) fall back to
# "activity mode": the most active, most engaged authors per brand.
INFLUENCER_MIN_POSTS = int(os.getenv("INFLUENCER_MIN_POSTS", "3"))
INFLUENCER_TOP_PER_BRAND = int(os.getenv("INFLUENCER_TOP_PER_BRAND", "60"))
TREND_WINDOW_DAYS = int(os.getenv("TREND_WINDOW_DAYS", "14"))
FORECAST_DAYS = int(os.getenv("FORECAST_DAYS", "7"))

# Complaint taxonomy: category -> keywords/phrases (lower-case, matched on word boundaries).
COMPLAINT_CATEGORIES = {
    "battery": ["battery", "charge", "charging", "drains", "dies", "power"],
    "connectivity": ["bluetooth", "disconnect", "disconnects", "pairing", "connection", "wifi", "lag", "latency"],
    "build_quality": ["broken", "cracked", "flimsy", "cheap plastic", "fell apart", "defective", "hinge"],
    "sound_quality": ["muffled", "distorted", "crackling", "static", "tinny", "audio cuts", "no bass"],
    "software": ["app", "firmware", "update", "bug", "bugs", "crash", "crashes", "glitch", "buggy"],
    "pricing": ["overpriced", "expensive", "price", "pricey", "rip off", "ripoff", "not worth"],
    "customer_service": ["support", "customer service", "no response", "ignored", "rude", "waiting", "dm", "help"],
    "delivery": ["delivery", "shipping", "shipped", "package", "late", "lost", "courier", "order"],
    "warranty_refund": ["refund", "warranty", "return", "replacement", "exchange", "money back"],
    "comfort_fit": ["uncomfortable", "hurts", "fit", "falls out", "heavy", "tight", "loose"],
    # broader categories that matter for real customer-support data (Kaggle twcs)
    "billing": ["bill", "billing", "charged", "overcharged", "double charged", "payment", "fee", "fees", "subscription"],
    "outage_network": ["outage", "down", "not working", "no service", "signal", "network", "offline", "server"],
}

# Words that never count as a "topic" (on top of Spark's English stop words).
EXTRA_STOPWORDS = {
    "just", "really", "get", "got", "like", "one", "new", "im", "dont", "cant", "ive",
    "would", "could", "also", "still", "even", "much", "thing", "things", "us", "amp",
    "https", "http", "co", "www", "com", "rt", "today", "day", "lol", "guys", "it's",
}
