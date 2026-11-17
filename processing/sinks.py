"""Writers for the serving layer: MongoDB (aggregates), Elasticsearch (post
search) and Neo4j (brand/influencer/topic graph)."""
from __future__ import annotations
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import config as C  # noqa: E402


def _clean(v):
    """Make values JSON/BSON friendly (NaN -> None, dates -> iso)."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v]
    return v


def clean_rows(rows):
    return [_clean(r) for r in rows]


# --------------------------------------------------------------------------- JSON dump
def dump_json(results: dict, out_dir: str):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in results.items():
        (out / f"{name}.json").write_text(json.dumps(clean_rows(rows), indent=1), encoding="utf-8")
    print(f"[json] wrote {len(results)} result sets to {out}")


# --------------------------------------------------------------------------- MongoDB
def write_mongo(results: dict):
    from pymongo import MongoClient
    client = MongoClient(C.MONGO_URI, serverSelectionTimeoutMS=10_000)
    db = client[C.MONGO_DB]
    for name, rows in results.items():
        coll = db[name]
        coll.delete_many({})
        rows = clean_rows(rows)
        if rows:
            coll.insert_many(rows)
    db["brand_daily"].create_index([("brand", 1), ("platform", 1), ("date", 1)])
    db["topics"].create_index([("brand", 1), ("rank", 1)])
    db["complaints"].create_index([("brand", 1)])
    client.close()
    print(f"[mongo] wrote {sum(len(r) for r in results.values()):,} docs into {len(results)} collections")


# --------------------------------------------------------------------------- Elasticsearch
ES_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {"properties": {
        "post_id": {"type": "keyword"}, "platform": {"type": "keyword"},
        "brand": {"type": "keyword"}, "product": {"type": "keyword"},
        "author": {"type": "keyword"}, "author_followers": {"type": "long"},
        "is_brand_account": {"type": "boolean"},
        "text": {"type": "text", "analyzer": "english"},
        "hashtags": {"type": "keyword"}, "categories": {"type": "keyword"},
        "likes": {"type": "long"}, "comments": {"type": "long"}, "shares": {"type": "long"},
        "views": {"type": "long"}, "engagement": {"type": "long"},
        "sentiment": {"type": "float"}, "sentiment_label": {"type": "keyword"},
        "is_complaint": {"type": "boolean"}, "created_at": {"type": "date"},
    }},
}

ES_FIELDS = list(ES_MAPPING["mappings"]["properties"])


def prepare_es_index(reset: bool = True):
    from elasticsearch import Elasticsearch
    es = Elasticsearch(C.ES_URL, request_timeout=60)
    if reset and es.indices.exists(index=C.ES_INDEX):
        es.indices.delete(index=C.ES_INDEX)
    if not es.indices.exists(index=C.ES_INDEX):
        es.indices.create(index=C.ES_INDEX, **ES_MAPPING)


def es_partition_writer(es_url: str, index: str):
    """Returns a function for DataFrame.foreachPartition (runs on executors)."""
    def write(rows):
        from elasticsearch import Elasticsearch, helpers
        es = Elasticsearch(es_url, request_timeout=120)

        def actions():
            for r in rows:
                d = r.asDict()
                if d.get("created_at") is not None and hasattr(d["created_at"], "isoformat"):
                    d["created_at"] = d["created_at"].isoformat()
                yield {"_index": index, "_id": d["post_id"], "_source": d}
        helpers.bulk(es, actions(), chunk_size=2000, raise_on_error=False)
    return write


# --------------------------------------------------------------------------- Neo4j
def write_neo4j(brands, influencers, topics, reset=True):
    from neo4j import GraphDatabase
    drv = GraphDatabase.driver(C.NEO4J_URI, auth=(C.NEO4J_USER, C.NEO4J_PASSWORD))
    with drv.session() as s:
        if reset:
            s.run("MATCH (n) DETACH DELETE n")
        for stmt in ("CREATE CONSTRAINT brand_name IF NOT EXISTS FOR (b:Brand) REQUIRE b.name IS UNIQUE",
                     "CREATE CONSTRAINT inf_handle IF NOT EXISTS FOR (i:Influencer) REQUIRE i.handle IS UNIQUE",
                     "CREATE CONSTRAINT topic_name IF NOT EXISTS FOR (t:Topic) REQUIRE t.name IS UNIQUE",
                     "CREATE CONSTRAINT platform_name IF NOT EXISTS FOR (p:Platform) REQUIRE p.name IS UNIQUE"):
            s.run(stmt)

        s.run("UNWIND $rows AS r MERGE (b:Brand {name: r.brand}) "
              "SET b.score = r.score, b.rank = r.rank, b.mentions = r.mentions",
              rows=clean_rows(brands))

        s.run("""
            UNWIND $rows AS r
            MERGE (i:Influencer {handle: r.author})
              SET i.followers = r.followers
            MERGE (p:Platform {name: r.platform})
            MERGE (i)-[:ACTIVE_ON]->(p)
            MERGE (b:Brand {name: r.brand})
            FOREACH (_ IN CASE WHEN r.role = 'promoter' THEN [1] ELSE [] END |
              MERGE (i)-[e:PROMOTES]->(b)
              SET e.posts = r.posts, e.sentiment = r.avg_sentiment,
                  e.engagement = r.engagement, e.score = r.score)
            FOREACH (_ IN CASE WHEN r.role = 'detractor' THEN [1] ELSE [] END |
              MERGE (i)-[e:CRITICIZES]->(b)
              SET e.posts = r.posts, e.sentiment = r.avg_sentiment,
                  e.engagement = r.engagement, e.score = r.score)
            """, rows=clean_rows(influencers))

        s.run("""
            UNWIND $rows AS r
            MERGE (b:Brand {name: r.brand})
            MERGE (t:Topic {name: r.topic})
            MERGE (b)-[e:DISCUSSED_WITH]->(t)
            SET e.count = r.count, e.sentiment = r.avg_sentiment
            """, rows=clean_rows([t for t in topics if t["rank"] <= 10]))
    drv.close()
    print(f"[neo4j] graph: {len(brands)} brands, {len(influencers)} influencer edges")
