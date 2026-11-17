"""The unified record format every source (Kaggle adapters, synthetic generator)
is normalised into before it reaches Kafka.

post record
-----------
post_id            str   unique across platforms (prefix with platform)
platform           str   instagram | reddit | youtube | x
brand              str   brand the post is about / posted by
product            str?  product mentioned, if known
author             str   handle
author_followers   int   follower count of the author at post time (0 = unknown)
is_brand_account   bool  True when the brand itself published the post
text               str
hashtags           [str] without '#', lower-case
likes, comments, shares, views   int
created_at         str   ISO-8601 UTC, e.g. 2026-08-01T13:45:00Z
source             str   "kaggle:twcs", "synthetic", ...

brand metric record (daily follower snapshot)
---------------------------------------------
brand, platform, date (YYYY-MM-DD), followers (int)
"""
from __future__ import annotations
import re

POST_FIELDS = [
    "post_id", "platform", "brand", "product", "author", "author_followers",
    "is_brand_account", "text", "hashtags", "likes", "comments", "shares",
    "views", "created_at", "source",
]

_HASHTAG_RE = re.compile(r"#(\w+)")


def extract_hashtags(text: str) -> list[str]:
    return sorted({h.lower() for h in _HASHTAG_RE.findall(text or "")})


def make_post(**kw) -> dict:
    post = {f: kw.get(f) for f in POST_FIELDS}
    for f in ("likes", "comments", "shares", "views", "author_followers"):
        post[f] = int(post[f] or 0)
    post["is_brand_account"] = bool(post["is_brand_account"])
    post["hashtags"] = post["hashtags"] or extract_hashtags(post["text"])
    return post
