"""SocialScope API: serves the analytics produced by the Spark batch job.

MongoDB       aggregates (metrics, topics, complaints, gaps, trends ...)
Elasticsearch full-text post search
Neo4j         influencer graph queries (falls back to MongoDB when unavailable)

    uvicorn app.main:app --reload --port 8000      (run from backend/)
"""
import logging
import os
import sys
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from common import config as C  # noqa: E402

log = logging.getLogger("socialscope")
app = FastAPI(title="SocialScope API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
                   allow_methods=["*"], allow_headers=["*"])


# --------------------------------------------------------------------------- clients
@lru_cache
def mongo():
    return MongoClient(C.MONGO_URI, serverSelectionTimeoutMS=3000)[C.MONGO_DB]


@lru_cache
def es():
    from elasticsearch import Elasticsearch
    return Elasticsearch(C.ES_URL, request_timeout=10)


@lru_cache
def neo():
    from neo4j import GraphDatabase
    return GraphDatabase.driver(C.NEO4J_URI, auth=(C.NEO4J_USER, C.NEO4J_PASSWORD))


def find(coll: str, q: dict | None = None, sort=None, limit: int = 0):
    cur = mongo()[coll].find(q or {}, {"_id": 0})
    if sort:
        cur = cur.sort(sort)
    if limit:
        cur = cur.limit(limit)
    return list(cur)


def brand_filter(brand: str | None) -> dict:
    return {"brand": brand} if brand else {}


# --------------------------------------------------------------------------- endpoints
@app.get("/api/health")
def health():
    status = {}
    try:
        mongo().command("ping")
        status["mongo"] = "up"
    except Exception as e:  # noqa: BLE001
        status["mongo"] = f"down: {e.__class__.__name__}"
    try:
        status["elasticsearch"] = "up" if es().ping() else "down"
    except Exception as e:  # noqa: BLE001
        status["elasticsearch"] = f"down: {e.__class__.__name__}"
    try:
        neo().verify_connectivity()
        status["neo4j"] = "up"
    except Exception as e:  # noqa: BLE001
        status["neo4j"] = f"down: {e.__class__.__name__}"
    return status


@app.get("/api/meta")
def meta():
    m = find("meta", limit=1)
    if not m:
        raise HTTPException(404, "No analytics yet - run the Spark batch job (processing/batch_analytics.py)")
    return m[0]


@app.get("/api/overview")
def overview():
    return {"leaderboard": find("leaderboard", sort=[("rank", 1)]),
            "metrics": find("brand_metrics", {"platform": "all"}, sort=[("brand", 1)])}


@app.get("/api/engagement")
def engagement(platform: str | None = None):
    q = {"platform": platform} if platform else {"platform": {"$ne": "all"}}
    return find("brand_metrics", q, sort=[("brand", 1), ("platform", 1)])


@app.get("/api/timeseries")
def timeseries(platform: str = "all", brand: str | None = None):
    return find("brand_daily", {"platform": platform, **brand_filter(brand)}, sort=[("date", 1)])


@app.get("/api/followers")
def followers(platform: str = "all"):
    return find("followers_daily", {"platform": platform}, sort=[("date", 1)])


@app.get("/api/topics")
def topics(brand: str | None = None, limit: int = Query(20, le=50)):
    return find("topics", {**brand_filter(brand), "rank": {"$lte": limit}}, sort=[("brand", 1), ("rank", 1)])


@app.get("/api/topics/trending")
def trending_topics():
    return find("trending_topics", sort=[("growth_pct", -1)])


@app.get("/api/complaints")
def complaints(brand: str | None = None):
    return find("complaints", brand_filter(brand), sort=[("count", -1)])


@app.get("/api/problems")
def problems(brand: str | None = None):
    return find("problems", brand_filter(brand), sort=[("brand", 1), ("rank", 1)])


@app.get("/api/gaps")
def gaps(my_brand: str | None = None, limit: int = 50):
    q = {"my_brand": my_brand} if my_brand else {}
    return find("gaps", q, sort=[("opportunity_score", -1)], limit=limit)


@app.get("/api/trends")
def trends(level: str = Query("brand", pattern="^(brand|product)$")):
    return find("trends", {"level": level}, sort=[("rank", 1)])


@app.get("/api/influencers")
def influencers(brand: str | None = None, role: str = "promoter", limit: int = Query(25, le=200)):
    """Influencers promoting (or criticising) a brand - from Neo4j, Mongo fallback."""
    rel = "PROMOTES" if role == "promoter" else "CRITICIZES"
    try:
        with neo().session() as s:
            res = s.run(f"""
                MATCH (i:Influencer)-[e:{rel}]->(b:Brand)
                WHERE $brand IS NULL OR b.name = $brand
                OPTIONAL MATCH (i)-[:ACTIVE_ON]->(p:Platform)
                OPTIONAL MATCH (i)-[:PROMOTES]->(other:Brand) WHERE other <> b
                RETURN i.handle AS author, i.followers AS followers, b.name AS brand,
                       e.posts AS posts, e.sentiment AS avg_sentiment, e.engagement AS engagement,
                       e.score AS score, collect(DISTINCT p.name) AS platforms,
                       collect(DISTINCT other.name) AS also_promotes
                ORDER BY score DESC LIMIT $limit""", brand=brand, limit=limit)
            return {"source": "neo4j", "items": [dict(r) for r in res]}
    except Exception as e:  # noqa: BLE001
        log.warning("neo4j unavailable (%s), falling back to mongo", e)
        q = {"role": role, **brand_filter(brand)}
        return {"source": "mongo", "items": find("influencers", q, sort=[("score", -1)], limit=limit)}


@app.get("/api/influencers/shared")
def shared_influencers(limit: int = 25):
    """Influencers who promote more than one competing brand (graph query)."""
    try:
        with neo().session() as s:
            res = s.run("""
                MATCH (i:Influencer)-[e:PROMOTES]->(b:Brand)
                WITH i, collect(b.name) AS brands, sum(e.score) AS score
                WHERE size(brands) > 1
                RETURN i.handle AS author, i.followers AS followers, brands, score
                ORDER BY score DESC LIMIT $limit""", limit=limit)
            return [dict(r) for r in res]
    except Exception:  # noqa: BLE001
        agg = mongo()["influencers"].aggregate([
            {"$match": {"role": "promoter"}},
            {"$group": {"_id": "$author", "followers": {"$max": "$followers"},
                        "brands": {"$addToSet": "$brand"}, "score": {"$sum": "$score"}}},
            {"$match": {"brands.1": {"$exists": True}}}, {"$sort": {"score": -1}}, {"$limit": limit}])
        return [{"author": a.pop("_id"), **a} for a in agg]


@app.get("/api/graph")
def graph(brand: str | None = None, limit: int = 60):
    """Nodes/edges for the brand-influencer network view."""
    try:
        with neo().session() as s:
            res = s.run("""
                MATCH (i:Influencer)-[e:PROMOTES|CRITICIZES]->(b:Brand)
                WHERE $brand IS NULL OR b.name = $brand
                RETURN i.handle AS src, b.name AS dst, type(e) AS kind, e.score AS score,
                       i.followers AS followers
                ORDER BY e.score DESC LIMIT $limit""", brand=brand, limit=limit)
            return [dict(r) for r in res]
    except Exception:  # noqa: BLE001
        rows = find("influencers", {"role": {"$ne": "neutral"}, **brand_filter(brand)},
                    sort=[("score", -1)], limit=limit)
        return [{"src": x["author"], "dst": x["brand"], "score": x["score"], "followers": x["followers"],
                 "kind": "PROMOTES" if x["role"] == "promoter" else "CRITICIZES"} for x in rows]


@app.get("/api/search")
def search(q: str = "", brand: str | None = None, platform: str | None = None,
           sentiment: str | None = None, complaints_only: bool = False,
           size: int = Query(25, le=100)):
    """Full-text search across every enriched post (Elasticsearch)."""
    must = [{"multi_match": {"query": q, "fields": ["text", "hashtags", "product"]}}] if q else [{"match_all": {}}]
    filt = []
    if brand:
        filt.append({"term": {"brand": brand}})
    if platform:
        filt.append({"term": {"platform": platform}})
    if sentiment:
        filt.append({"term": {"sentiment_label": sentiment}})
    if complaints_only:
        filt.append({"term": {"is_complaint": True}})
    body = {"query": {"bool": {"must": must, "filter": filt}},
            "sort": ["_score", {"engagement": "desc"}],
            "aggs": {"by_brand": {"terms": {"field": "brand"}},
                     "by_sentiment": {"terms": {"field": "sentiment_label"}}}}
    try:
        res = es().search(index=C.ES_INDEX, size=size, **body)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(503, f"Elasticsearch unavailable: {e.__class__.__name__}") from e
    return {
        "total": res["hits"]["total"]["value"],
        "hits": [h["_source"] for h in res["hits"]["hits"]],
        "by_brand": {b["key"]: b["doc_count"] for b in res["aggregations"]["by_brand"]["buckets"]},
        "by_sentiment": {b["key"]: b["doc_count"] for b in res["aggregations"]["by_sentiment"]["buckets"]},
    }


# --------------------------------------------------------------------------- dashboard
# In Docker the built React app is copied to /srv/frontend/dist and served from "/".
# Mounted last so every /api route above takes precedence.
_DIST = Path(os.getenv("FRONTEND_DIST", str(ROOT / "frontend" / "dist")))
if _DIST.is_dir():
    from fastapi.staticfiles import StaticFiles
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="dashboard")
