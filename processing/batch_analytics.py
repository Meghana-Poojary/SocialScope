"""SocialScope batch analytics (PySpark).

Reads the raw posts + follower snapshots from HDFS, enriches every post
(sentiment, engagement, complaint categories), computes all competitor
intelligence views and publishes them:

    MongoDB        -> aggregates the dashboard reads
    Elasticsearch  -> every enriched post, for full-text search
    Neo4j          -> brand / influencer / platform / topic graph
    HDFS           -> curated, enriched Parquet zone

    python processing/batch_analytics.py                      # HDFS -> all sinks
    python processing/batch_analytics.py --source local \\
        --skip-mongo --skip-es --skip-neo4j --dump-json data/output   # offline run
"""
from __future__ import annotations
import argparse
import math
import re
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.ml.feature import NGram, StopWordsRemover
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

ROOT = Path(__file__).resolve().parents[1]
import os  # noqa: E402
# Spark workers must use this same interpreter (matters on Windows / in venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(ROOT))
from common import config as C  # noqa: E402
from processing import sinks  # noqa: E402
from processing.schemas import METRIC_SCHEMA, POST_SCHEMA  # noqa: E402

# ============================================================================ enrichment
_ANALYZER = None


def _analyzer():
    global _ANALYZER
    if _ANALYZER is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _ANALYZER = SentimentIntensityAnalyzer()
    return _ANALYZER


@F.pandas_udf(DoubleType())
def vader_compound(texts: pd.Series) -> pd.Series:
    an = _analyzer()
    return texts.fillna("").map(lambda t: float(an.polarity_scores(t)["compound"]))


def category_pattern(words):
    return r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"


def enrich(posts: DataFrame) -> DataFrame:
    df = (posts.dropDuplicates(["post_id"])
          .withColumn("created_ts", F.to_timestamp("created_at"))
          .withColumn("date", F.to_date("created_ts"))
          .where(F.col("created_ts").isNotNull() & F.col("brand").isNotNull())
          .withColumn("text", F.coalesce("text", F.lit("")))
          .withColumn("text_lc", F.lower("text"))
          .withColumn("engagement", F.col("likes") + F.col("comments") + F.col("shares"))
          .withColumn("engagement_rate", F.when(F.col("author_followers") > 0,
                      F.col("engagement") / F.col("author_followers") * 100))
          .withColumn("sentiment", vader_compound("text"))
          .withColumn("sentiment_label",
                      F.when(F.col("sentiment") >= 0.05, "positive")
                       .when(F.col("sentiment") <= -0.05, "negative").otherwise("neutral")))

    cats = F.array(*[F.when(F.col("text_lc").rlike(category_pattern(kws)), F.lit(cat))
                     for cat, kws in C.COMPLAINT_CATEGORIES.items()])
    df = df.withColumn("matched_categories", F.filter(cats, lambda x: x.isNotNull()))
    complaint = (~F.col("is_brand_account")) & (
        ((F.col("sentiment") <= -0.05) & (F.size("matched_categories") > 0)) | (F.col("sentiment") <= -0.4))
    return (df.withColumn("is_complaint", complaint)
              .withColumn("categories", F.when(complaint & (F.size("matched_categories") == 0),
                                               F.array(F.lit("other")))
                                         .when(complaint, F.col("matched_categories"))
                                         .otherwise(F.array().cast("array<string>")))
              .drop("matched_categories"))


def tokens(df: DataFrame, brand_words: list[str]) -> DataFrame:
    """Adds `words` (clean tokens) and `bigrams` columns. Bigrams containing a
    brand or product name are dropped so topics describe *what* people discuss."""
    cleaned = F.regexp_replace(F.col("text_lc"), r"https?://\S+|@\w+|#\w+", " ")
    # sentence punctuation becomes a break token so bigrams never span two clauses
    cleaned = F.regexp_replace(cleaned, r"[.,!?;:()\-]+", " zzbrk ")
    df = df.withColumn("raw_words", F.filter(F.split(F.regexp_replace(cleaned, r"[^a-z\s]", " "), r"\s+"),
                                            lambda w: F.length(w) >= 3))
    stop = StopWordsRemover.loadDefaultStopWords("english") + sorted(C.EXTRA_STOPWORDS)
    df = StopWordsRemover(inputCol="raw_words", outputCol="words", stopWords=stop).transform(df)
    df = NGram(n=2, inputCol="words", outputCol="bigrams_all").transform(df)
    brand_re = r"\b(" + "|".join(re.escape(b) for b in brand_words) + r")\b" if brand_words else r"^$"
    brand_sub_re = "(" + "|".join(re.escape(b) for b in brand_words) + ")" if brand_words else r"^$"  # e.g. #novabeatair
    return (df.withColumn("bigrams", F.filter("bigrams_all", lambda g: ~g.rlike(brand_re) & ~g.contains("zzbrk")))
              .withColumn("words", F.filter("words", lambda w: ~w.rlike(brand_re) & (w != "zzbrk")))
              .withColumn("topic_tags", F.filter("hashtags", lambda h: ~h.rlike(brand_sub_re)))
              .drop("raw_words", "bigrams_all"))


# ============================================================================ helpers
def rows(df: DataFrame) -> list[dict]:
    return [r.asDict(recursive=True) for r in df.collect()]


def by_platform_and_all(df: DataFrame, keys: list[str], aggs: list) -> DataFrame:
    """Group by keys + platform, and union a platform='all' rollup."""
    per = df.groupBy(*keys, "platform").agg(*aggs)
    tot = df.groupBy(*keys).agg(*aggs).withColumn("platform", F.lit("all"))
    return per.unionByName(tot)


def minmax(values, min_rel_spread=0.05):
    """Rescale to 0..1 across competitors. When all brands are within 5% of each other
    the component is treated as a tie (0.5), so trivial differences (e.g. 0.50 vs 0.51
    replies per tweet) don't turn into 0-vs-100 scores."""
    vals = [v for v in values if v is not None]
    lo, hi = (min(vals), max(vals)) if vals else (0, 0)
    tie = hi == lo or (hi - lo) < min_rel_spread * max(abs(hi), abs(lo), 1e-9)
    return lambda v: 0.5 if v is None or tie else (v - lo) / (hi - lo)


def r(x, n=4):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), n)


# ============================================================================ analytics
def brand_metrics(e: DataFrame, followers: DataFrame | None, min_date, max_date):
    user = ~F.col("is_brand_account")
    # Audience growth: unique authors talking about the brand in the last 14 days vs the
    # first 14 days. Works for every dataset, and stands in for follower growth when a
    # dataset has no follower snapshots (e.g. Kaggle Twitter data).
    first = F.col("date") <= F.date_add(F.lit(min_date), 13)
    last = F.col("date") > F.date_sub(F.lit(max_date), 14)
    aggs = [
        F.count("*").alias("posts"),
        F.sum(F.when(user, 1).otherwise(0)).alias("mentions"),
        F.sum(F.when(~user, 1).otherwise(0)).alias("brand_posts"),
        F.sum("likes").alias("likes"), F.sum("comments").alias("comments"),
        F.sum("shares").alias("shares"), F.sum("views").alias("views"),
        F.sum("engagement").alias("engagement"),
        F.avg("engagement").alias("avg_engagement"),
        F.avg(F.when(~user, F.col("engagement_rate"))).alias("brand_engagement_rate"),
        F.avg(F.when(user, F.col("engagement_rate"))).alias("audience_engagement_rate"),
        F.avg(F.when(user, F.col("sentiment"))).alias("avg_sentiment"),
        F.avg(F.when(user, (F.col("sentiment_label") == "positive").cast("int"))).alias("positive_pct"),
        F.avg(F.when(user, (F.col("sentiment_label") == "neutral").cast("int"))).alias("neutral_pct"),
        F.avg(F.when(user, (F.col("sentiment_label") == "negative").cast("int"))).alias("negative_pct"),
        F.sum(F.col("is_complaint").cast("int")).alias("complaints"),
        F.countDistinct("author").alias("unique_authors"),
        F.countDistinct(F.when(user & first, F.col("author"))).alias("authors_first_14d"),
        F.countDistinct(F.when(user & last, F.col("author"))).alias("authors_last_14d"),
    ]
    m = by_platform_and_all(e, ["brand"], aggs)
    m = m.withColumn("audience_growth_pct", F.when(F.col("authors_first_14d") > 0,
                     (F.col("authors_last_14d") - F.col("authors_first_14d")) / F.col("authors_first_14d") * 100))
    w = Window.partitionBy("platform")
    m = m.withColumn("share_of_voice", F.col("mentions") / F.sum("mentions").over(w))

    fol_daily = []
    if followers is not None:
        f = followers.withColumn("date", F.to_date("date")).dropDuplicates(["brand", "platform", "date"])
        f = f.unionByName(f.groupBy("brand", "date").agg(F.sum("followers").alias("followers"))
                          .withColumn("platform", F.lit("all")))
        wb = Window.partitionBy("brand", "platform").orderBy("date")
        wall = wb.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        growth = (f.withColumn("first", F.first("followers").over(wall))
                   .withColumn("last", F.last("followers").over(wall))
                   .groupBy("brand", "platform")
                   .agg(F.first("first").alias("followers_start"), F.first("last").alias("followers_end"))
                   .withColumn("follower_growth_pct",
                               (F.col("followers_end") - F.col("followers_start")) / F.col("followers_start") * 100))
        m = m.join(growth, ["brand", "platform"], "left")
        fol_daily = rows(f.withColumn("date", F.date_format("date", "yyyy-MM-dd")).orderBy("brand", "platform", "date"))
    else:
        for c in ("followers_start", "followers_end", "follower_growth_pct"):
            m = m.withColumn(c, F.lit(None).cast("double"))
    return rows(m), fol_daily


def leaderboard(metrics: list[dict]):
    """Composite 'who is winning' score over the platform='all' rows."""
    allm = [m for m in metrics if m["platform"] == "all"]
    er = [m["brand_engagement_rate"] if m["brand_engagement_rate"] is not None else m["avg_engagement"] for m in allm]
    # growth = follower growth when the dataset has follower snapshots, else audience growth
    use_followers = any(m["follower_growth_pct"] is not None for m in allm)
    basis = "followers" if use_followers else "audience"
    growth = [m["follower_growth_pct"] if use_followers else m["audience_growth_pct"] for m in allm]
    n_er, n_sent = minmax(er), minmax([m["avg_sentiment"] for m in allm])
    n_grow, n_sov = minmax(growth), minmax([m["share_of_voice"] for m in allm])
    n_cmp = minmax([m["complaints"] / max(m["mentions"], 1) for m in allm])
    out = []
    for m, e, g in zip(allm, er, growth):
        parts = {
            "engagement": n_er(e), "sentiment": n_sent(m["avg_sentiment"]),
            "growth": n_grow(g), "share_of_voice": n_sov(m["share_of_voice"]),
            "complaint_health": 1 - n_cmp(m["complaints"] / max(m["mentions"], 1)),
        }
        score = (0.25 * parts["engagement"] + 0.25 * parts["sentiment"] + 0.15 * parts["growth"]
                 + 0.2 * parts["share_of_voice"] + 0.15 * parts["complaint_health"]) * 100
        out.append({"brand": m["brand"], "score": round(score, 1),
                    "components": {k: round(v * 100, 1) for k, v in parts.items()},
                    "mentions": m["mentions"], "avg_sentiment": r(m["avg_sentiment"]),
                    "share_of_voice": r(m["share_of_voice"]),
                    "follower_growth_pct": r(m["follower_growth_pct"], 2),
                    "audience_growth_pct": r(m["audience_growth_pct"], 2),
                    "growth_pct": r(g, 2), "growth_basis": basis,
                    "engagement_rate": r(m["brand_engagement_rate"], 3)})
    out.sort(key=lambda x: -x["score"])
    for i, x in enumerate(out, 1):
        x["rank"] = i
    return out


def daily(e: DataFrame):
    aggs = [F.sum((~F.col("is_brand_account")).cast("int")).alias("mentions"),
            F.sum("engagement").alias("engagement"),
            F.avg(F.when(~F.col("is_brand_account"), F.col("sentiment"))).alias("avg_sentiment"),
            F.sum(F.col("is_complaint").cast("int")).alias("complaints")]
    d = by_platform_and_all(e, ["brand", "date"], aggs)
    return rows(d.withColumn("date", F.date_format("date", "yyyy-MM-dd")).orderBy("brand", "platform", "date"))


def topics(t: DataFrame, max_date, top_k=20):
    """Bigram + hashtag topics per brand with volume, sentiment, 7-day growth and lift."""
    recent = F.col("date") > F.date_sub(F.lit(max_date), 7)
    prior = (F.col("date") <= F.date_sub(F.lit(max_date), 7)) & (F.col("date") > F.date_sub(F.lit(max_date), 14))
    base = t.where(~F.col("is_brand_account"))
    terms = (base.select("brand", "date", "sentiment", F.explode("bigrams").alias("topic"))
             .unionByName(base.select("brand", "date", "sentiment", F.explode("topic_tags").alias("tag"))
                          .select("brand", "date", "sentiment", F.concat(F.lit("#"), "tag").alias("topic"))))
    agg = (terms.groupBy("brand", "topic")
           .agg(F.count("*").alias("count"), F.avg("sentiment").alias("avg_sentiment"),
                F.sum(recent.cast("int")).alias("last_7d"), F.sum(prior.cast("int")).alias("prev_7d")))
    # lift = how much more this brand talks about the topic than the market does
    brand_tot = agg.groupBy("brand").agg(F.sum("count").alias("brand_total"))
    topic_tot = agg.groupBy("topic").agg(F.sum("count").alias("topic_total"))
    grand = agg.agg(F.sum("count")).first()[0] or 1
    agg = (agg.join(brand_tot, "brand").join(topic_tot, "topic")
           .withColumn("lift", (F.col("count") / F.col("brand_total")) / (F.col("topic_total") / F.lit(grand)))
           .withColumn("growth_pct", F.when(F.col("prev_7d") > 0,
                       (F.col("last_7d") - F.col("prev_7d")) / F.col("prev_7d") * 100))
           .where(F.col("count") >= 3))
    # rank by volume x distinctiveness so generic tags (#tech) don't drown real topics
    agg = agg.withColumn("relevance", F.col("count") * F.col("lift"))
    w = Window.partitionBy("brand").orderBy(F.desc("relevance"))
    top = agg.withColumn("rank", F.row_number().over(w)).where(F.col("rank") <= top_k)
    out = rows(top.drop("brand_total", "topic_total").orderBy("brand", "rank"))

    # market-wide trending topics (fastest growing with enough volume)
    trend = (terms.groupBy("topic")
             .agg(F.count("*").alias("count"), F.sum(recent.cast("int")).alias("last_7d"),
                  F.sum(prior.cast("int")).alias("prev_7d"), F.avg("sentiment").alias("avg_sentiment"),
                  F.collect_set("brand").alias("brands"))
             .where((F.col("last_7d") >= 10))
             .withColumn("growth_pct", (F.col("last_7d") - F.col("prev_7d")) / F.greatest(F.col("prev_7d"), F.lit(1)) * 100)
             .orderBy(F.desc("growth_pct")).limit(25))
    return out, rows(trend)


def complaints(t: DataFrame, mentions_by_brand: dict):
    c = t.where("is_complaint").select("*", F.explode("categories").alias("category"))
    total_c = {x["brand"]: x["n"] for x in rows(t.where("is_complaint").groupBy("brand").agg(F.count("*").alias("n")))}
    agg = c.groupBy("brand", "category").agg(
        F.count("*").alias("count"), F.avg("sentiment").alias("avg_sentiment"),
        F.sum("engagement").alias("engagement"), F.collect_set("platform").alias("platforms"))
    w = Window.partitionBy("brand", "category").orderBy(F.asc("sentiment"), F.desc("engagement"))
    samples = (c.withColumn("rn", F.row_number().over(w)).where("rn <= 3")
               .groupBy("brand", "category")
               .agg(F.collect_list(F.struct("text", "platform", "sentiment", "created_at")).alias("samples")))
    out = rows(agg.join(samples, ["brand", "category"], "left"))
    for x in out:
        x["pct_of_mentions"] = r(x["count"] / max(mentions_by_brand.get(x["brand"], 1), 1) * 100, 2)
        x["pct_of_complaints"] = r(x["count"] / max(total_c.get(x["brand"], 1), 1) * 100, 2)
    out.sort(key=lambda x: (x["brand"], -x["count"]))

    # frequently discussed problems = most common bigrams inside complaints
    pw = Window.partitionBy("brand").orderBy(F.desc("count"))
    problems = (t.where("is_complaint").select("brand", F.explode("bigrams").alias("phrase"))
                .groupBy("brand", "phrase").count()
                .withColumn("rank", F.row_number().over(pw)).where("rank <= 15"))
    return out, rows(problems.orderBy("brand", "rank"))


def gaps(complaint_rows: list[dict], brands: list[str]):
    """For every (my_brand, competitor, category): where the competitor is
    struggling clearly more than my brand and than the market average."""
    rate = {(x["brand"], x["category"]): x["pct_of_mentions"] or 0 for x in complaint_rows}
    samples = {(x["brand"], x["category"]): (x.get("samples") or [{}])[0].get("text") for x in complaint_rows}
    cats = sorted({x["category"] for x in complaint_rows})
    out = []
    for cat in cats:
        rates = {b: rate.get((b, cat), 0.0) for b in brands}
        market = sum(rates.values()) / max(len(rates), 1)
        for comp in brands:
            if rates[comp] <= market * 1.2 or rates[comp] < 1.0:
                continue
            for me in brands:
                if me == comp or rates[me] >= rates[comp] * 0.6:
                    continue
                delta = rates[comp] - rates[me]
                out.append({
                    "my_brand": me, "competitor": comp, "category": cat,
                    "competitor_rate": round(rates[comp], 2), "my_rate": round(rates[me], 2),
                    "market_rate": round(market, 2), "gap": round(delta, 2),
                    "opportunity_score": round(delta * (rates[comp] / max(market, 0.01)), 2),
                    "evidence": samples.get((comp, cat)),
                    "insight": (f"{comp} customers complain about {cat.replace('_', ' ')} in "
                                f"{rates[comp]:.1f}% of mentions vs {rates[me]:.1f}% for {me} "
                                f"(market avg {market:.1f}%). Lead with {cat.replace('_', ' ')} in messaging."),
                })
    out.sort(key=lambda x: -x["opportunity_score"])
    return out


def influencers(e: DataFrame, has_followers: bool):
    """Followers mode: accounts with INFLUENCER_MIN_FOLLOWERS+ followers.
    Activity mode (datasets without follower counts): authors who posted about a brand
    at least INFLUENCER_MIN_POSTS times, i.e. its most vocal customers."""
    if has_followers:
        src = e.where((~F.col("is_brand_account")) & (F.col("author_followers") >= C.INFLUENCER_MIN_FOLLOWERS))
    else:
        src = e.where(~F.col("is_brand_account"))
    inf = (src.groupBy("author", "brand")
           .agg(F.max("author_followers").alias("followers"),
                F.first("platform").alias("platform"),
                F.collect_set("platform").alias("platforms"),
                F.count("*").alias("posts"),
                F.sum((F.col("sentiment_label") == "positive").cast("int")).alias("positive_posts"),
                F.avg("sentiment").alias("avg_sentiment"),
                F.sum("engagement").alias("engagement"),
                F.avg("engagement_rate").alias("engagement_rate"),
                F.max_by("text", "engagement").alias("top_post"))
           .withColumn("role", F.when(F.col("avg_sentiment") >= 0.05, "promoter")
                                .when(F.col("avg_sentiment") <= -0.05, "detractor").otherwise("neutral"))
           )
    if has_followers:
        inf = inf.withColumn("score", F.round(F.log10("followers") * F.log1p("engagement")
                                              * (F.lit(0.5) + F.col("positive_posts") / F.col("posts")), 2))
    else:
        # reach unknown: score = activity x engagement received x positivity
        inf = (inf.where(F.col("posts") >= C.INFLUENCER_MIN_POSTS)
                  .withColumn("followers", F.lit(None).cast("long"))
                  .withColumn("score", F.round(F.log1p("posts") * F.log1p(F.col("engagement") + F.col("posts"))
                                               * (F.lit(0.5) + F.col("positive_posts") / F.col("posts")) * 5, 2)))
    w = Window.partitionBy("brand").orderBy(F.desc("score"))
    inf = inf.withColumn("_rk", F.row_number().over(w)).where(F.col("_rk") <= C.INFLUENCER_TOP_PER_BRAND).drop("_rk")
    return rows(inf.orderBy(F.desc("score")))


def trends(e: DataFrame, max_date, by_product=False):
    """Momentum + linear-regression trend over the last TREND_WINDOW_DAYS,
    turned into a probability of trending and a short forecast."""
    W = C.TREND_WINDOW_DAYS
    keys = ["brand", "product"] if by_product else ["brand"]
    src = e.where(~F.col("is_brand_account"))
    if by_product:
        src = src.where(F.col("product").isNotNull())
    d = (src.where(F.col("date") > F.date_sub(F.lit(max_date), W))
         .groupBy(*keys, "date")
         .agg(F.count("*").alias("mentions"), F.sum("engagement").alias("engagement"),
              F.avg("sentiment").alias("sentiment"))
         .withColumn("x", F.datediff("date", F.date_sub(F.lit(max_date), W)).cast("double")))
    half = W // 2
    stats = d.groupBy(*keys).agg(
        F.regr_slope("mentions", "x").alias("mention_slope"),
        F.regr_intercept("mentions", "x").alias("mention_intercept"),
        F.regr_slope("engagement", "x").alias("engagement_slope"),
        F.regr_slope("sentiment", "x").alias("sentiment_slope"),
        F.avg("mentions").alias("avg_daily_mentions"),
        F.avg("engagement").alias("avg_daily_engagement"),
        F.avg("sentiment").alias("avg_sentiment"),
        F.sum(F.when(F.col("x") > W - half, F.col("mentions"))).alias("recent"),
        F.sum(F.when(F.col("x") <= W - half, F.col("mentions"))).alias("previous"),
        F.count("*").alias("days_observed"))
    hist = {}
    for row in rows(d.orderBy("date")):
        hist.setdefault(tuple(row[k] for k in keys), []).append(
            {"date": row["date"].isoformat(), "mentions": row["mentions"]})

    out = []
    for s in rows(stats):
        if s["days_observed"] < max(4, W // 3) or (by_product and (s["avg_daily_mentions"] or 0) < 2):
            continue
        avg = max(s["avg_daily_mentions"] or 0, 1e-9)
        rel_slope = (s["mention_slope"] or 0) / avg                       # daily growth as share of volume
        momentum = (s["recent"] or 0) / max(s["previous"] or 0, 1) - 1    # second half vs first half
        eng_slope = (s["engagement_slope"] or 0) / max(s["avg_daily_engagement"] or 0, 1e-9)
        sent_slope = s["sentiment_slope"] or 0
        score = (0.4 * math.tanh(momentum * 2) + 0.3 * math.tanh(rel_slope * W)
                 + 0.15 * math.tanh(eng_slope * W) + 0.15 * math.tanh(sent_slope * W * 4))
        prob = 1 / (1 + math.exp(-4 * score))
        key = tuple(s[k] for k in keys)
        forecast = [{"day": i, "mentions": round(max(0.0, (s["mention_intercept"] or 0) + (s["mention_slope"] or 0) * (W + i)), 1)}
                    for i in range(1, C.FORECAST_DAYS + 1)]
        out.append({**{k: s[k] for k in keys},
                    "trend_score": round(score, 3), "trend_probability": round(prob, 3),
                    "direction": "rising" if score > 0.15 else "falling" if score < -0.15 else "stable",
                    "momentum_pct": round(momentum * 100, 1), "daily_growth_pct": round(rel_slope * 100, 2),
                    "sentiment_trend": round(sent_slope * W, 3), "avg_daily_mentions": round(avg, 1),
                    "avg_sentiment": r(s["avg_sentiment"]),
                    "history": hist.get(key, []), "forecast": forecast})
    out.sort(key=lambda x: -x["trend_probability"])
    for i, x in enumerate(out, 1):
        x["rank"] = i
    return out


# ============================================================================ main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["hdfs", "local"], default="hdfs")
    ap.add_argument("--local-dir", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--skip-mongo", action="store_true")
    ap.add_argument("--skip-es", action="store_true")
    ap.add_argument("--skip-neo4j", action="store_true")
    ap.add_argument("--skip-curated", action="store_true", help="don't write enriched Parquet")
    ap.add_argument("--dump-json", help="also write every result set as JSON into this folder")
    a = ap.parse_args()

    t0 = time.time()
    spark = (SparkSession.builder.appName("SocialScope-Batch-Analytics")
             .master(C.SPARK_MASTER)
             .config("spark.driver.memory", C.SPARK_DRIVER_MEMORY)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.hadoop.dfs.replication", C.HDFS_REPLICATION)
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    if a.source == "hdfs":
        posts = spark.read.parquet(C.HDFS_POSTS).select(*POST_SCHEMA.fieldNames())
        try:
            followers = spark.read.parquet(C.HDFS_BRAND_METRICS)
        except Exception:  # noqa: BLE001  - follower data is optional (e.g. Kaggle-only runs)
            followers = None
        curated_path = C.HDFS_CURATED
    else:
        posts = spark.read.schema(POST_SCHEMA).json(f"{a.local_dir}/posts.jsonl")
        mpath = Path(a.local_dir) / "brand_metrics.jsonl"
        followers = spark.read.schema(METRIC_SCHEMA).json(str(mpath)) if mpath.exists() else None
        curated_path = str(ROOT / "data" / "curated" / "posts_enriched")

    if followers is not None and followers.limit(1).count() == 0:
        followers = None   # e.g. an empty snapshot folder left by a Kaggle-only stream run
    e = enrich(posts).cache()
    n = e.count()
    if n == 0:
        sys.exit("no posts found - run the producer + stream job (or use --source local)")
    brand_list = sorted(x["brand"] for x in rows(e.select("brand").distinct()))
    stats = e.agg(F.min("date").alias("lo"), F.max("date").alias("hi"),
                  F.max("author_followers").alias("max_fol"), F.sum("likes").alias("likes"),
                  F.sum("comments").alias("comments"), F.sum("shares").alias("shares"),
                  F.sum("views").alias("views"), F.count("product").alias("with_product"),
                  F.sum(F.col("is_brand_account").cast("int")).alias("brand_posts")).first()
    min_date, max_date = stats["lo"], stats["hi"]
    # What this dataset actually contains; analyses adapt and the dashboard explains it.
    caps = {
        "follower_counts": (stats["max_fol"] or 0) > 0,           # per-author followers -> ER + influencer reach
        "follower_snapshots": followers is not None,              # daily brand followers -> follower growth
        "likes": (stats["likes"] or 0) > 0, "comments": (stats["comments"] or 0) > 0,
        "shares": (stats["shares"] or 0) > 0, "views": (stats["views"] or 0) > 0,
        "products": (stats["with_product"] or 0) > 0, "brand_posts": (stats["brand_posts"] or 0) > 0,
    }
    caps["influencer_mode"] = "followers" if caps["follower_counts"] else "activity"
    print(f"enriched {n:,} posts for {len(brand_list)} brands, {min_date} .. {max_date}")
    print("dataset capabilities:", caps)

    products = [x["product"] for x in rows(e.select("product").distinct()) if x["product"]]
    brand_words = sorted({w.lower() for b in brand_list + products
                          for w in re.findall(r"[A-Za-z]+", b) if len(w) >= 3})
    t = tokens(e, brand_words).cache()

    metrics, fol_daily = brand_metrics(e, followers, min_date, max_date)
    board = leaderboard(metrics)
    mentions_by_brand = {m["brand"]: m["mentions"] for m in metrics if m["platform"] == "all"}
    topic_rows, trending_topics = topics(t, max_date)
    complaint_rows, problem_rows = complaints(t, mentions_by_brand)
    gap_rows = gaps(complaint_rows, brand_list)
    inf_rows = influencers(e, caps["follower_counts"])
    trend_rows = trends(e, max_date) + trends(e, max_date, by_product=True)
    for x in trend_rows:
        x["level"] = "product" if x.get("product") else "brand"

    results = {
        "brand_metrics": metrics, "leaderboard": board, "brand_daily": daily(e),
        "followers_daily": fol_daily, "topics": topic_rows, "trending_topics": trending_topics,
        "complaints": complaint_rows, "problems": problem_rows, "gaps": gap_rows,
        "influencers": inf_rows, "trends": trend_rows,
        "meta": [{"run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "posts": n,
                  "brands": brand_list, "latest_date": max_date.isoformat(),
                  "first_date": min_date.isoformat(), "capabilities": caps,
                  "platforms": sorted(x["platform"] for x in rows(e.select("platform").distinct())),
                  "sources": sorted(x["source"] or "unknown" for x in rows(e.select("source").distinct()))}],
    }
    for k, v in results.items():
        print(f"  {k:<16} {len(v):>7,} rows")

    if a.dump_json:
        sinks.dump_json(results, a.dump_json)
    if not a.skip_mongo:
        sinks.write_mongo(results)
    if not a.skip_neo4j:
        sinks.write_neo4j(board, [i for i in inf_rows if i["role"] != "neutral"], topic_rows)
    if not a.skip_es:
        sinks.prepare_es_index(reset=True)
        (e.select(*[c for c in sinks.ES_FIELDS if c != "created_at"], F.col("created_ts").alias("created_at"))
          .repartition(4).foreachPartition(sinks.es_partition_writer(C.ES_URL, C.ES_INDEX)))
        print(f"[es] indexed {n:,} posts into {C.ES_INDEX}")
    if not a.skip_curated:
        (e.drop("text_lc").write.mode("overwrite").partitionBy("platform", "date").parquet(curated_path))
        print(f"[hdfs] curated zone written to {curated_path}")

    print(f"done in {time.time() - t0:,.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
