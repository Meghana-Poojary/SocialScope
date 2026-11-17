"""Kafka producer: replays the unified JSONL files as a live social feed.

Posts go to one topic per platform (social.instagram, social.reddit,
social.youtube, social.x), keyed by brand so a brand's posts stay ordered
within a partition. Daily follower snapshots go to brand.metrics.

    python ingestion/producer.py                    # as fast as possible
    python ingestion/producer.py --rate 200         # ~200 msgs/sec, simulates a live stream
    python ingestion/producer.py --bootstrap localhost:9092   # explicit broker
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import KAFKA_BOOTSTRAP, PLATFORMS, TOPIC_BRAND_METRICS, topic_for  # noqa: E402

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def ensure_topics(bootstrap: str, recreate: bool = False):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="socialscope-admin")
    wanted = [topic_for(p) for p in PLATFORMS] + [TOPIC_BRAND_METRICS]
    existing = set(admin.list_topics())
    if recreate and existing & set(wanted):
        # Purge old messages so a new dataset never mixes with a previous one.
        admin.delete_topics(sorted(existing & set(wanted)))
        for _ in range(30):
            time.sleep(1)
            existing = set(admin.list_topics())
            if not existing & set(wanted):
                break
        print("recreated Kafka topics (old messages purged)")
    new = [NewTopic(t, num_partitions=3, replication_factor=1) for t in wanted if t not in existing]
    if new:
        try:
            admin.create_topics(new)
        except TopicAlreadyExistsError:
            pass
    # Topic creation is asynchronous: wait until every partition has a leader,
    # otherwise the first messages after a (re)create can be lost.
    for _ in range(60):
        try:
            desc = admin.describe_topics(wanted)
            ready = all(d.get("error_code", 0) == 0 and d["partitions"]
                        and all(p.get("leader", -1) >= 0 for p in d["partitions"]) for d in desc)
        except Exception:  # noqa: BLE001 - metadata not available yet
            ready = False
        if ready:
            break
        time.sleep(1)
    else:
        sys.exit("Kafka topics did not become ready - is the kafka container healthy?")
    admin.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--posts", default=str(RAW / "posts.jsonl"))
    ap.add_argument("--metrics", default=str(RAW / "brand_metrics.jsonl"),
                    help="daily follower snapshots; 'none' to skip (datasets without them)")
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    ap.add_argument("--rate", type=float, default=0, help="messages per second (0 = unthrottled)")
    ap.add_argument("--recreate-topics", action="store_true", help="delete and recreate the topics first")
    a = ap.parse_args()

    ensure_topics(a.bootstrap, a.recreate_topics)
    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20, batch_size=64 * 1024, acks=1,
        retries=10, retry_backoff_ms=500,          # kafka-python does not retry by default
    )

    sent = 0
    t0 = time.time()
    failed = []

    def send(topic, key, value):
        producer.send(topic, key=key, value=value).add_errback(lambda e: failed.append(e))

    def throttle():
        if a.rate > 0:
            ahead = sent / a.rate - (time.time() - t0)
            if ahead > 0:
                time.sleep(ahead)

    if a.metrics and Path(a.metrics).is_file():
        with open(a.metrics, encoding="utf-8") as f:
            for line in f:
                m = json.loads(line)
                send(TOPIC_BRAND_METRICS, m["brand"], m)
                sent += 1
        print(f"sent {sent:,} follower snapshots -> {TOPIC_BRAND_METRICS}")

    n_posts = 0
    with open(a.posts, encoding="utf-8") as f:
        for line in f:
            p = json.loads(line)
            if p["platform"] not in PLATFORMS:
                continue
            send(topic_for(p["platform"]), p["brand"], p)
            sent += 1
            n_posts += 1
            throttle()
            if n_posts % 10_000 == 0:
                print(f"  {n_posts:,} posts sent ({sent / (time.time() - t0):,.0f} msg/s)")
    producer.flush()
    if failed:
        sys.exit(f"{len(failed):,} messages could not be delivered (first error: {failed[0]!r})")
    print(f"sent {n_posts:,} posts in {time.time() - t0:,.1f}s - all delivered")


if __name__ == "__main__":
    main()
