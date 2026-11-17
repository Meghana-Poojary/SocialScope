"""Synthetic multi-platform dataset generator.

Kaggle datasets rarely cover all four platforms for the same set of competing
brands, so this script produces a realistic, *labelled-as-synthetic* dataset in
the unified schema (see common/schema.py). Each brand gets hidden traits
(weak complaint categories, engagement strength, momentum) so the analytics
have genuine signal to discover.

    python ingestion/generate_sample.py --days 90 --posts-per-day 450
    python ingestion/generate_sample.py --brands "Auralis,Novabeat" --seed 7

Outputs data/raw/posts.jsonl and data/raw/brand_metrics.jsonl
"""
from __future__ import annotations
import argparse
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import PLATFORMS  # noqa: E402
from common.schema import make_post  # noqa: E402

# --------------------------------------------------------------------------- brand profiles
# Default competitors: fictional consumer-audio brands.
DEFAULT_BRANDS = {
    "Auralis": {
        "products": ["Auralis Pods Pro", "Auralis Max", "Auralis Sport"],
        "weak": ["pricing", "software"],           # what customers complain about
        "strength": 1.15,                          # engagement multiplier
        "positivity": 0.55,                        # base share of positive posts
        "momentum": 0.002,                         # daily growth of mention volume
        "launch": None,
    },
    "Novabeat": {
        "products": ["Novabeat Air", "Novabeat Bass+", "Novabeat Studio"],
        "weak": ["battery", "build_quality", "warranty_refund"],
        "strength": 0.95,
        "positivity": 0.38,
        "momentum": -0.003,
        "launch": None,
    },
    "Zephyr": {
        "products": ["Zephyr Buds", "Zephyr Halo", "Zephyr X2"],
        "weak": ["customer_service", "delivery"],
        "strength": 1.0,
        "positivity": 0.47,
        "momentum": 0.0,
        "launch": None,
    },
    "Kinetiq": {
        "products": ["Kinetiq One", "Kinetiq Flow", "Kinetiq Echo ANC"],
        "weak": ["connectivity", "comfort_fit"],
        "strength": 0.9,
        "positivity": 0.5,
        "momentum": 0.004,
        # A product that launches late in the window and takes off -> trend signal
        "launch": {"product": "Kinetiq Echo ANC", "day_frac": 0.86, "boost": 2.6},
    },
}

PLATFORM_PROFILE = {
    # base_reach: typical audience multiplier, share_ratio etc. shape the engagement mix
    "instagram": {"weight": 0.30, "like": 1.0, "comment": 0.04, "share": 0.01, "views": 0.0},
    "reddit": {"weight": 0.20, "like": 0.6, "comment": 0.25, "share": 0.005, "views": 0.0},
    "youtube": {"weight": 0.15, "like": 0.5, "comment": 0.06, "share": 0.02, "views": 12.0},
    "x": {"weight": 0.35, "like": 0.7, "comment": 0.05, "share": 0.18, "views": 6.0},
}

FEATURES = ["noise cancellation", "battery life", "sound quality", "bass", "fit",
            "case", "mic quality", "transparency mode", "spatial audio", "app", "bluetooth range"]

# --------------------------------------------------------------------------- text templates
POSITIVE = [
    "Absolutely loving my {product}! The {feature} is incredible",
    "{product} is the best purchase I've made this year. {feature} is amazing",
    "Switched to {product} and wow, the {feature} is fantastic",
    "Honestly impressed by the {feature} on the {product}. Great job {brand}",
    "{brand} nailed it with the {product}. Superb {feature}, highly recommend",
    "Gym session with my {product} and they stay put perfectly. Love the {feature}",
    "So happy with how good the {feature} is on {product}. Worth every penny",
    "Unboxing the {product} today, excellent {feature} and beautiful design",
]
NEUTRAL = [
    "Thinking about getting the {product}. Anyone tried the {feature}?",
    "{product} vs the competition, which has better {feature}?",
    "Just ordered the {product}, will report back on the {feature}",
    "Is the {product} on sale anywhere this week?",
    "Comparing {feature} across earbuds this weekend, {product} is on the list",
    "Saw the {brand} ad for {product} earlier",
]
NEGATIVE = {
    "battery": ["My {product} battery dies after two hours, terrible",
                "Battery drains so fast on the {product}, so disappointed",
                "{product} won't hold a charge anymore. Awful battery life"],
    "connectivity": ["{product} keeps disconnecting from bluetooth, so frustrating",
                     "Pairing issues again with my {product}. The lag is horrible",
                     "Constant connection drops on {product}, worst experience"],
    "build_quality": ["My {product} case cracked after a week, cheap plastic and awful quality",
                      "{product} broke already. Really flimsy build, very bad",
                      "Defective {product} out of the box, the hinge fell apart"],
    "sound_quality": ["Sound on my {product} is muffled and distorted, awful",
                      "Crackling static in the left {product} bud, so annoying",
                      "{product} sounds tinny, no bass at all. Disappointed"],
    "software": ["The {brand} app is so buggy, {product} firmware update broke everything",
                 "App crashes every time I connect the {product}. Terrible software",
                 "Latest firmware update ruined my {product}, glitch after glitch"],
    "pricing": ["{product} is overpriced for what you get, total rip off. Disappointed",
                "Way too expensive, {product} is not worth the price. Bad value",
                "{brand} raised the price again? {product} is a ripoff, ridiculous"],
    "customer_service": ["{brand} support ignored my emails for two weeks, horrible",
                         "Customer service at {brand} was rude and useless about my {product}",
                         "Still waiting for {brand} support to reply, no response at all. Unacceptable"],
    "delivery": ["My {product} order is two weeks late, terrible shipping",
                 "{brand} lost my package, delivery was a disaster",
                 "Courier says delivered but no {product}. Awful delivery experience"],
    "warranty_refund": ["{brand} refused my warranty claim for the {product}, terrible",
                        "Trying to get a refund for my {product} is a nightmare, so frustrated",
                        "Replacement {product} also defective and no money back. Furious"],
    "comfort_fit": ["{product} hurts my ears after an hour, really uncomfortable",
                    "{product} falls out constantly, the fit is terrible",
                    "Way too tight and heavy, {product} is painful and awful to wear"],
}
BRAND_OFFICIAL = [
    "Meet the all-new {product}. Next-level {feature}. #{tag} #audio",
    "Your music, your way. {product} with upgraded {feature} is here. #{tag}",
    "Weekend deal: save 20% on {product}! #{tag} #sale",
    "Behind the scenes: how we tuned the {feature} on {product} #{tag}",
    "Thank you for 1M listeners! Giveaway: win a {product} #{tag} #giveaway",
]
HASHTAG_POOL = ["earbuds", "audio", "music", "tech", "wireless", "headphones", "gadgets",
                "review", "unboxing", "techtok", "audiophile", "fitness"]


def tagify(s: str) -> str:
    return "".join(ch for ch in s if ch.isalnum()).lower()


class AuthorPool:
    def __init__(self, rng: random.Random, brands: list[str]):
        self.rng = rng
        # Regular users: log-normal followers (median ~300)
        self.users = [f"user_{i:05d}" for i in range(6000)]
        self.user_followers = {u: int(rng.lognormvariate(5.7, 1.3)) for u in self.users}
        # Influencers: each has 1-2 favourite brands they promote
        self.influencers = []
        for i in range(70):
            fav = rng.sample(brands, k=1 if rng.random() < 0.75 else 2)
            self.influencers.append({
                "handle": rng.choice(["tech", "audio", "beats", "gadget", "sound", "review"])
                + rng.choice(["guru", "lab", "nerd", "queen", "daily", "hub", "geek"]) + f"_{i:02d}",
                "followers": int(rng.lognormvariate(11.0, 1.1)) + 10_000,
                "platform": rng.choice(PLATFORMS),
                "brands": fav,
            })

    def pick(self, brand: str, platform: str):
        r = self.rng.random()
        if r < 0.06:
            cands = [i for i in self.influencers if brand in i["brands"]]
            if cands:
                inf = self.rng.choice(cands)
                # influencers mostly post on their home platform
                plat = inf["platform"] if self.rng.random() < 0.85 else platform
                return inf["handle"], inf["followers"], True, plat
        u = self.rng.choice(self.users)
        return u, self.user_followers[u], False, platform


def generate(brands: dict, days: int, posts_per_day: int, seed: int, end: datetime):
    rng = random.Random(seed)
    names = list(brands)
    pool = AuthorPool(rng, names)
    start = end - timedelta(days=days)
    posts, metrics = [], []
    pid = 0

    # base followers per brand/platform
    followers = {(b, p): int(rng.uniform(80_000, 900_000)) for b in names for p in PLATFORMS}

    for d in range(days):
        day = start + timedelta(days=d)
        for b in names:
            prof = brands[b]
            vol = posts_per_day / len(names) * math.exp(prof["momentum"] * d)
            launch = prof.get("launch")
            launched = launch and d >= int(days * launch["day_frac"])
            if launched:
                vol *= 1 + (launch["boost"] - 1) * min(1.0, (d - days * launch["day_frac"]) / 10)
            n = max(1, int(rng.gauss(vol, vol * 0.12)))

            # daily follower snapshot
            for p in PLATFORMS:
                g = 0.0008 + prof["momentum"] * 0.6 + (0.004 if launched else 0)
                followers[(b, p)] = int(followers[(b, p)] * (1 + rng.gauss(g, 0.0006)))
                metrics.append({"brand": b, "platform": p, "date": day.strftime("%Y-%m-%d"),
                                "followers": followers[(b, p)]})

            for _ in range(n):
                platform = rng.choices(PLATFORMS, [PLATFORM_PROFILE[p]["weight"] for p in PLATFORMS])[0]
                pp = PLATFORM_PROFILE[platform]
                products = [x for x in prof["products"] if not (launch and x == launch["product"] and not launched)]
                product = rng.choice(products)
                if launched and rng.random() < 0.45:
                    product = launch["product"]
                feature = rng.choice(FEATURES)
                ts = day + timedelta(seconds=rng.randint(0, 86399))

                if rng.random() < 0.07:  # official brand post
                    author, fol, is_brand = tagify(b) + "_official", followers[(b, platform)], True
                    text = rng.choice(BRAND_OFFICIAL).format(product=product, feature=feature, tag=tagify(b))
                    reach = fol * rng.uniform(0.01, 0.05) * prof["strength"]
                else:
                    author, fol, is_inf, platform = pool.pick(b, platform)
                    pp = PLATFORM_PROFILE[platform]
                    is_brand = False
                    pos = prof["positivity"] + (0.15 if launched and product == (launch or {}).get("product") else 0)
                    if is_inf:
                        pos = 0.85  # influencers promoting their favourite brand
                    r = rng.random()
                    if r < pos:
                        text = rng.choice(POSITIVE)
                    elif r < pos + 0.22:
                        text = rng.choice(NEUTRAL)
                    else:
                        # negative: mostly this brand's weak spots, sometimes anything
                        cat = rng.choice(prof["weak"]) if rng.random() < 0.7 else rng.choice(list(NEGATIVE))
                        text = rng.choice(NEGATIVE[cat])
                    text = text.format(product=product, feature=feature, brand=b)
                    if rng.random() < 0.5:
                        text += " " + " ".join("#" + t for t in rng.sample(HASHTAG_POOL, rng.randint(1, 3)))
                    if rng.random() < 0.3:
                        text += f" #{tagify(product)}"
                    reach = (fol * rng.uniform(0.02, 0.12) + rng.expovariate(1 / 25)) * prof["strength"]

                likes = int(reach * pp["like"] * rng.uniform(0.5, 1.5))
                posts.append(make_post(
                    post_id=f"{platform}_{pid:08d}", platform=platform, brand=b, product=product,
                    author=author, author_followers=fol, is_brand_account=is_brand, text=text,
                    likes=likes,
                    comments=int(reach * pp["comment"] * rng.uniform(0.3, 1.7)),
                    shares=int(reach * pp["share"] * rng.uniform(0.3, 1.7)),
                    views=int(reach * pp["views"] * rng.uniform(0.6, 1.4)),
                    created_at=ts.strftime("%Y-%m-%dT%H:%M:%SZ"), source="synthetic",
                ))
                pid += 1
    return posts, metrics


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--posts-per-day", type=int, default=450)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--brands", help="comma-separated brand names to use instead of the defaults")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "raw"))
    a = ap.parse_args()

    brands = DEFAULT_BRANDS
    if a.brands:
        templates = list(DEFAULT_BRANDS.values())
        brands = {}
        for i, name in enumerate(x.strip() for x in a.brands.split(",") if x.strip()):
            t = dict(templates[i % len(templates)])
            t["products"] = [f"{name} {s}" for s in ("Pro", "Lite", "Max")]
            if t.get("launch"):
                t["launch"] = dict(t["launch"], product=t["products"][2])
            brands[name] = t

    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    posts, metrics = generate(brands, a.days, a.posts_per_day, a.seed, end)

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "posts.jsonl", "w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p) + "\n")
    with open(out / "brand_metrics.jsonl", "w", encoding="utf-8") as f:
        for m in metrics:
            f.write(json.dumps(m) + "\n")
    print(f"wrote {len(posts):,} posts and {len(metrics):,} follower snapshots to {out}")


if __name__ == "__main__":
    main()
