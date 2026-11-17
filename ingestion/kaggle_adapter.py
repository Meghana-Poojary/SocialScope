"""Convert a Kaggle dataset into the unified SocialScope schema (JSONL).

Supported formats
-----------------
twcs     "Customer Support on Twitter" (kaggle.com/datasets/thoughtvector/customer-support-on-twitter)
         File: twcs.csv  (tweet_id, author_id, inbound, created_at, text,
                          response_tweet_id, in_response_to_tweet_id)
         ~3M real tweets between customers and brands (@AppleSupport, @AmazonHelp,
         @SpotifyCares ...). Becomes platform "x". What the dataset provides and what
         SocialScope derives from it:
           - comments  = number of replies a tweet received (response_tweet_id)
           - product   = product named in the text (iPhone, Prime, Xbox Live ...)
           - likes / shares / views / followers are not in the dataset (left at 0);
             the analytics switch to activity-based influencers and author-based
             growth automatically (see processing/batch_analytics.py)

generic  Any CSV with one row per post. Supply a JSON column mapping, e.g.
         {"text": "Text", "platform": "Platform", "created_at": "Timestamp",
          "author": "User", "likes": "Likes", "shares": "Retweets"}
         Brands come from a "brand" column if mapped, else from keyword matches in the
         text (--brands). Works with e.g. "Social Media Sentiments Analysis Dataset".

Examples
    python ingestion/kaggle_adapter.py --format twcs --input data/kaggle/customer-support-on-twitter/twcs/twcs.csv \
        --brands AppleSupport,AmazonHelp,SpotifyCares,Uber_Support --out data/raw/kaggle_posts.jsonl
    python ingestion/kaggle_adapter.py --format generic --input data/kaggle/sentimentdataset.csv \
        --mapping data/kaggle/mapping.example.json --brands Nike,Adidas,Puma
"""
from __future__ import annotations
import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import PLATFORMS  # noqa: E402
from common.schema import make_post  # noqa: E402

PLATFORM_ALIASES = {"twitter": "x", "x": "x", "instagram": "instagram", "ig": "instagram",
                    "reddit": "reddit", "youtube": "youtube", "yt": "youtube"}
MENTION_RE = re.compile(r"@(\w+)")
SUPPORT_SUFFIX = re.compile(r"(_?support|_?help|_?cares?|_?service)$", re.I)

# Product lexicon for the most active brands in the twcs dataset. Keys are the display
# brand (lower-case); values map a product name to the phrases that identify it.
# Extend it, or pass --products my_products.json with the same shape.
PRODUCT_LEXICON = {
    "apple": {"iPhone": ["iphone"], "iPad": ["ipad"], "Mac": ["macbook", "imac", "mac"],
              "iOS": ["ios", "ios 11", "ios11"], "Apple Watch": ["apple watch", "applewatch"],
              "AirPods": ["airpods"], "Apple Music": ["apple music", "itunes"], "iCloud": ["icloud"]},
    "amazon": {"Prime": ["prime", "amazon prime"], "Prime Video": ["prime video"],
               "Kindle": ["kindle"], "Alexa / Echo": ["alexa", "echo"], "Fire TV": ["fire tv", "firestick", "fire stick"]},
    "spotify": {"Premium": ["premium"], "Playlists": ["playlist", "playlists"], "Podcasts": ["podcast", "podcasts"]},
    "uber": {"Uber rides": ["ride", "driver", "trip"], "Uber Eats": ["uber eats", "ubereats"]},
    "xbox": {"Xbox One": ["xbox one", "xbox one x", "one x"], "Xbox Live": ["xbox live", "xboxlive"],
             "Game Pass": ["game pass", "gamepass"]},
    "playstation": {"PS4": ["ps4", "playstation 4"], "PSN": ["psn", "playstation network"], "PS Plus": ["ps plus", "psplus"]},
    "sprint": {"Sprint service": ["service", "signal", "coverage"], "Sprint billing": ["bill", "billing"]},
    "comcast": {"Internet": ["internet", "wifi"], "TV": ["cable", "tv"]},
    "tmobile": {"T-Mobile network": ["signal", "coverage", "network"], "T-Mobile billing": ["bill", "billing"]},
    "delta": {"Flights": ["flight", "flights"], "Baggage": ["bag", "bags", "baggage", "luggage"]},
    "americanair": {"Flights": ["flight", "flights"], "Baggage": ["bag", "bags", "baggage", "luggage"]},
    "british_airways": {"Flights": ["flight", "flights"], "Baggage": ["bag", "bags", "baggage", "luggage"]},
}


def brand_display(handle: str) -> str:
    """AppleSupport -> Apple, Uber_Support -> Uber, sprintcare -> Sprint, AskPlayStation -> PlayStation."""
    name = SUPPORT_SUFFIX.sub("", handle).strip("_") or handle
    name = re.sub(r"^Ask_?(?=[A-Z])", "", name) or name        # AskPlayStation -> PlayStation
    return name[0].upper() + name[1:]


def iso(ts) -> str | None:
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    return None if pd.isna(t) else t.strftime("%Y-%m-%dT%H:%M:%SZ")


def compile_products(lexicon: dict) -> dict:
    """{brand_lower: [(product, regex), ...]} - longest phrases first."""
    out = {}
    for brand, prods in lexicon.items():
        pats = []
        for prod, words in prods.items():
            for w in sorted(words, key=len, reverse=True):
                pats.append((prod, re.compile(rf"\b{re.escape(w)}\b", re.I)))
        out[brand.lower()] = pats
    return out


def find_product(products: dict, brand: str, text: str) -> str | None:
    for prod, pat in products.get(brand.lower(), []):
        if pat.search(text):
            return prod
    return None


def reply_count(v) -> int:
    return len([x for x in str(v).split(",") if x.strip() and x.strip() != "nan"]) if isinstance(v, str) else 0


# --------------------------------------------------------------------------- twcs
def convert_twcs(path: str, brands: list[str] | None, top_n: int, chunksize: int, products: dict):
    # Pass 1 (only when no brands given): company handles = authors of outbound tweets.
    if not brands:
        counts = Counter()
        for ch in pd.read_csv(path, usecols=["author_id", "inbound"], chunksize=chunksize):
            counts.update(ch.loc[~ch["inbound"].astype(bool), "author_id"])
        brands = [b for b, _ in counts.most_common(top_n)]
        print("auto-selected brands:", ", ".join(brands))
    wanted = {b.lower(): b for b in brands}

    cols = ["tweet_id", "author_id", "inbound", "created_at", "text", "response_tweet_id"]
    for ch in pd.read_csv(path, chunksize=chunksize, usecols=cols,
                          dtype={"author_id": str, "text": str, "response_tweet_id": str}):
        ch["inbound"] = ch["inbound"].astype(bool)
        for row in ch.itertuples(index=False):
            text = row.text if isinstance(row.text, str) else ""
            if row.inbound:
                hit = next((wanted[m.lower()] for m in MENTION_RE.findall(text) if m.lower() in wanted), None)
                if not hit:
                    continue
                handle, is_brand = hit, False
            else:
                if str(row.author_id).lower() not in wanted:
                    continue
                handle, is_brand = wanted[str(row.author_id).lower()], True
            ts = iso(pd.to_datetime(row.created_at, format="%a %b %d %H:%M:%S %z %Y", errors="coerce"))
            if not ts:
                continue
            brand = brand_display(handle)
            yield make_post(
                post_id=f"x_twcs_{row.tweet_id}", platform="x", brand=brand,
                product=None if is_brand else find_product(products, brand, text),
                author=str(row.author_id), author_followers=0, is_brand_account=is_brand, text=text,
                likes=0, comments=reply_count(row.response_tweet_id), shares=0, views=0,
                created_at=ts, source="kaggle:twcs",
            )


# --------------------------------------------------------------------------- generic
def convert_generic(path: str, mapping: dict, brands: list[str] | None, default_platform: str,
                    chunksize: int, products: dict):
    if not brands and "brand" not in mapping:
        sys.exit("generic format needs a 'brand' column in --mapping or a --brands list")
    patterns = {b: re.compile(rf"\b{re.escape(b)}\b", re.I) for b in (brands or [])}
    col = lambda r, k, d=None: r.get(mapping[k], d) if k in mapping else d  # noqa: E731
    num = lambda v: 0 if v is None or (isinstance(v, float) and pd.isna(v)) else v  # noqa: E731
    n = 0
    for ch in pd.read_csv(path, chunksize=chunksize):
        for r in ch.to_dict("records"):
            text = str(col(r, "text", "") or "").strip()
            brand = col(r, "brand")
            if not isinstance(brand, str) or not brand.strip():
                brand = next((b for b, p in patterns.items() if p.search(text)), None)
            if not brand or not text:
                continue
            brand = brand.strip()
            plat = PLATFORM_ALIASES.get(str(col(r, "platform", default_platform)).strip().lower(), default_platform)
            if plat not in PLATFORMS:
                continue
            ts = iso(col(r, "created_at"))
            if not ts:
                continue
            tags = col(r, "hashtags")
            hashtags = re.findall(r"\w+", str(tags).lower()) if isinstance(tags, str) else None
            product = col(r, "product")
            if not isinstance(product, str):
                product = find_product(products, brand, text)
            n += 1
            yield make_post(
                post_id=f"{plat}_gen_{col(r, 'post_id', n)}", platform=plat, brand=brand,
                product=product, author=str(col(r, "author", "unknown")).strip(),
                author_followers=num(col(r, "author_followers", 0)),
                is_brand_account=bool(col(r, "is_brand_account", False)), text=text,
                hashtags=hashtags, likes=num(col(r, "likes", 0)), comments=num(col(r, "comments", 0)),
                shares=num(col(r, "shares", 0)), views=num(col(r, "views", 0)), created_at=ts,
                source=f"kaggle:{Path(path).stem}",
            )


# --------------------------------------------------------------------------- post-processing
def sample_per_brand(gen, max_per_brand: int, seed: int):
    """Reservoir-sample up to N audience posts and N/10 brand posts per brand, so a
    multi-million-row dataset fits a laptop while staying spread over the whole period."""
    if not max_per_brand:
        yield from gen
        return
    rng = random.Random(seed)
    res, seen = defaultdict(list), Counter()
    for p in gen:
        key = (p["brand"], p["is_brand_account"])
        cap = max(1, max_per_brand // 10) if p["is_brand_account"] else max_per_brand
        seen[key] += 1
        if len(res[key]) < cap:
            res[key].append(p)
        else:
            j = rng.randrange(seen[key])
            if j < cap:
                res[key][j] = p
    for key in sorted(res):
        yield from res[key]


def trim_sparse_edges(posts: list[dict], frac: float = 0.1) -> list[dict]:
    """Drop thin days at the start and end of a dataset (e.g. the few early-2017 tweets
    in twcs): keep the span between the first and last day with >= frac x median volume."""
    days = Counter(p["created_at"][:10] for p in posts)
    if len(days) < 5:
        return posts
    median = sorted(days.values())[len(days) // 2]
    dense = sorted(d for d, c in days.items() if c >= frac * median)
    lo, hi = dense[0], dense[-1]
    kept = [p for p in posts if lo <= p["created_at"][:10] <= hi]
    print(f"kept {lo} .. {hi} ({len(kept):,} of {len(posts):,} posts; trimmed sparse edges)")
    return kept


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--format", choices=["twcs", "generic"], required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "raw" / "kaggle_posts.jsonl"))
    ap.add_argument("--append", action="store_true", help="append to --out instead of overwriting")
    ap.add_argument("--brands", help="comma-separated brand handles/names")
    ap.add_argument("--top-n", type=int, default=6, help="twcs: number of brands when --brands is omitted")
    ap.add_argument("--mapping", help="generic: JSON file mapping schema fields to CSV columns")
    ap.add_argument("--platform", default="x", help="generic: platform when the CSV has none")
    ap.add_argument("--products", help="JSON file {brand: {product: [phrases]}} to extend product detection")
    ap.add_argument("--max-per-brand", type=int, default=30000,
                    help="sample at most N audience posts per brand (0 = keep all)")
    ap.add_argument("--no-trim", action="store_true", help="keep sparse days at the dataset edges")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--chunksize", type=int, default=200_000)
    a = ap.parse_args()

    lexicon = dict(PRODUCT_LEXICON)
    if a.products:
        lexicon.update(json.loads(Path(a.products).read_text()))
    products = compile_products(lexicon)

    brands = [b.strip() for b in a.brands.split(",") if b.strip()] if a.brands else None
    if a.format == "twcs":
        gen = convert_twcs(a.input, brands, a.top_n, a.chunksize, products)
    else:
        if not a.mapping:
            sys.exit("--mapping is required for the generic format")
        mapping = json.loads(Path(a.mapping).read_text())
        gen = convert_generic(a.input, mapping, brands, a.platform, a.chunksize, products)

    posts = list(sample_per_brand(gen, a.max_per_brand, a.seed))
    if not posts:
        sys.exit("no posts matched - check --brands (twcs: support handles such as AppleSupport) or the mapping")
    if not a.no_trim:
        posts = trim_sparse_edges(posts)
    posts.sort(key=lambda p: p["created_at"])       # replay in time order, like a live feed

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    per_brand, with_product = Counter(), 0
    with open(out, "a" if a.append else "w", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post) + "\n")
            per_brand[post["brand"]] += 1
            with_product += post["product"] is not None
    print(f"wrote {len(posts):,} posts to {out}  ({with_product:,} mention a known product)")
    for b, c in per_brand.most_common():
        print(f"  {b:<20} {c:,}")


if __name__ == "__main__":
    main()
