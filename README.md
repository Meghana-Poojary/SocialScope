# SocialScope — Competitor Intelligence & Brand Analytics

SocialScope tracks competing brands across **Instagram, Reddit, YouTube and X** and works out which brand is doing better and why. It runs on a big-data pipeline: **Kafka → Spark Structured Streaming → HDFS → PySpark analytics → MongoDB / Elasticsearch / Neo4j → FastAPI → React**.

| Feature | Where it's computed | Where it's served from |
|---|---|---|
| Compare competitors across 4 platforms | `brand_metrics()` in `processing/batch_analytics.py` | MongoDB `brand_metrics`, `leaderboard` |
| Likes, comments, shares, engagement rate, follower growth | `brand_metrics()` + follower snapshots | MongoDB `brand_metrics`, `followers_daily` |
| Sentiment analysis of audience reactions | VADER as a Spark `pandas_udf` (`enrich()`) | MongoDB `brand_daily`, ES `sentiment` |
| Trending topics per brand | bigrams + hashtags, 7-day growth, lift (`topics()`) | MongoDB `topics`, `trending_topics`; Neo4j `DISCUSSED_WITH` |
| Customer complaints & frequent problems | complaint taxonomy + negative sentiment (`complaints()`) | MongoDB `complaints`, `problems` |
| Influencers promoting competitors | 10k+ follower authors by brand and sentiment (`influencers()`) | Neo4j `(:Influencer)-[:PROMOTES]->(:Brand)` |
| Gaps where competitors get negative feedback | competitor vs. own vs. market complaint rates (`gaps()`) | MongoDB `gaps` |
| Predict which brand/product trends next | regression slope + momentum → probability + forecast (`trends()`) | MongoDB `trends` |
| Full-text search over every post | — | Elasticsearch `socialscope-posts` |

---

## Architecture

```
 Kaggle CSV ──► kaggle_adapter.py ─┐
                                    ├─► data/raw/*.jsonl ─► producer.py ─► Kafka
 generate_sample.py (synthetic) ───┘                        topics: social.instagram | social.reddit
                                                                    social.youtube   | social.x | brand.metrics
                                                                          │
                                         stream_to_hdfs.py (Spark Structured Streaming)
                                                                          ▼
                                     HDFS  /socialscope/raw/posts  (Parquet, partitioned platform/date)
                                           /socialscope/raw/brand_metrics
                                                                          │
                                         batch_analytics.py (PySpark + MLlib feature transformers)
                     ┌────────────────────────┬───────────────┼──────────────────┬─────────────────┐
                     ▼                        ▼               ▼                  ▼                 ▼
               MongoDB aggregates     Elasticsearch posts   Neo4j graph    HDFS curated zone   (optional JSON dump)
                     └────────────────────────┴───────┬───────┘
                                                      ▼
                                          FastAPI  (backend/app/main.py)
                                                      ▼
                                          React + Recharts dashboard (frontend/)
```

### Project layout

```
common/        config.py (ports, topics, complaint taxonomy) · schema.py (unified record)
ingestion/     generate_sample.py · kaggle_adapter.py · producer.py
processing/    stream_to_hdfs.py · batch_analytics.py · sinks.py · schemas.py · Dockerfile (Spark image)
backend/       FastAPI app + Dockerfile (also builds and serves the dashboard)
frontend/      Vite + React dashboard
scripts/       docker_pipeline.ps1 / docker_pipeline.sh — end-to-end pipeline
docker-compose.yml   the whole stack
data/          raw/ (JSONL input) · kaggle/ (put downloaded CSVs here)
```

---

## The dataset

All sources are normalised into one record format (`common/schema.py`):
`post_id, platform, brand, product, author, author_followers, is_brand_account, text, hashtags, likes, comments, shares, views, created_at, source`.

### Option A — Kaggle: Customer Support on Twitter (one command)
[kaggle.com/datasets/thoughtvector/customer-support-on-twitter](https://www.kaggle.com/datasets/thoughtvector/customer-support-on-twitter) has about 3 million real tweets between customers and brands (`@AppleSupport`, `@AmazonHelp`, `@XboxSupport`, …).

```powershell
./scripts/docker_pipeline.ps1 -Source kaggle
```

This one command does everything:

- **Download.** If no `twcs.csv` is in `data/kaggle/`, it downloads the dataset with your Kaggle API token. To get a token, go to kaggle.com → Settings → API → *Create New Token*, and save the file as `%USERPROFILE%\.kaggle\kaggle.json`. The download resumes and retries on a flaky connection. You can also download the CSV yourself and put it anywhere under `data/kaggle/`.
- **Convert.** It converts the CSV for 6 competing brands: Apple, Amazon, Spotify, Uber, Xbox and PlayStation. Pick others with `-Brands "AppleSupport,AmazonHelp,..."` (use the support handles from the dataset).
- **Sample.** It keeps up to 30,000 customer tweets per brand (`-MaxPerBrand`), randomly sampled across the whole period, so it fits on a laptop.
- **Run.** It runs Kafka → Spark Streaming → HDFS → Spark analytics → stores, starting clean so nothing mixes with earlier data.

**How every feature works on this dataset.** The dataset has no likes, follower counts or other platforms, so SocialScope adapts. The dashboard shows a note saying which of these apply.

| Feature | On Kaggle Twitter data |
| --- | --- |
| Competitor comparison, performance score | Works; uses share of voice, sentiment, engagement, growth and complaint rate |
| Engagement | Replies each tweet received (from `response_tweet_id`); engagement rate needs follower counts, so it is hidden |
| Follower growth | Replaced by **audience growth**: unique authors in the last 14 days vs the first 14 |
| Sentiment, topics, complaints, gaps | Work as usual. Two extra complaint categories, *billing* and *outage/network*, suit support data |
| Influencers | **Activity mode**: the most active, most engaged customers per brand (3+ tweets), shown as promoters and critics |
| Trend prediction | Brands, plus products detected in the text (iPhone, Prime, Xbox Live, PS4, …) |
| Platforms | X only; platform filters hide themselves |

Switch back to the synthetic sample at any time with `./scripts/docker_pipeline.ps1`.

### Option B — any other Kaggle CSV (generic mapper)
Map your CSV columns to the schema fields with a JSON file (see `data/kaggle/mapping.example.json`):

- If the CSV has a brand column, map it as `"brand"`.
- Otherwise brands are found by keyword match on `-Brands`.

Any field you can map turns on more of the analytics:

- follower counts enable engagement rate and follower-based influencers
- likes and shares feed engagement
- a platform column enables the platform filters

```powershell
./scripts/docker_pipeline.ps1 -Source kaggle -KaggleFormat generic -KaggleFile data/kaggle/my.csv `
    -Mapping data/kaggle/mapping.example.json -Brands "Nike,Adidas,Puma"
```

### Option C — synthetic multi-platform sample (default, works offline)
```bash
docker compose exec spark python3 ingestion/generate_sample.py                     # 4 fictional audio brands, 90 days, ~47k posts
docker compose exec spark python3 ingestion/generate_sample.py --brands "Apple,Amazon,Spotify"   # same brands as your Kaggle run
```
The generator gives each brand hidden traits, so the analytics have something real to find:
- **Novabeat** has battery, build-quality and refund problems.
- **Zephyr** has customer-service and delivery problems.
- **Kinetiq** launches *Kinetiq Echo ANC* late in the window, and it takes off.
- **Auralis** leads on engagement but draws pricing and app complaints.


---

## Running it (Docker)

You only need **Docker Desktop**. Python, Java, Hadoop, Kafka and the rest all run in containers.

```powershell
./scripts/docker_up.ps1           # pulls ~5 GB of images, builds two, starts everything (retries on network drops)
./scripts/docker_pipeline.ps1     # data -> Kafka -> Spark Streaming -> HDFS -> Spark analytics -> stores
```
`docker_up.ps1` just wraps `docker compose pull`, `docker compose build` and `docker compose up -d` with retries. On a stable connection, `docker compose up -d --build` does the same thing.
(Use `./scripts/docker_pipeline.sh` from Git Bash, macOS or Linux.)

Then open **http://localhost:8000** for the dashboard.

| Service | URL |
|---|---|
| Dashboard | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| HDFS NameNode UI | http://localhost:9870 |
| Spark UI (while a job runs) | http://localhost:4040 |
| Neo4j browser | http://localhost:7474 (neo4j / socialscope123) |
| Elasticsearch | http://localhost:9200 |
| MongoDB | `mongodb://localhost:27018` (27018, so it doesn't clash with a local MongoDB) |

The pipeline script runs these steps inside the `spark` container:
1. **generate_sample.py** creates the synthetic data, only if `data/raw/posts.jsonl` doesn't exist yet.
2. **producer.py** publishes every record to Kafka, one topic per platform.
3. **stream_to_hdfs.py --once** uses Spark Structured Streaming to drain Kafka into HDFS Parquet.
4. **batch_analytics.py** runs the PySpark analytics and writes to MongoDB, Elasticsearch, Neo4j and the curated HDFS zone.

Options:
- `-Source synthetic` (default) or `-Source kaggle` (see *The dataset*).
- `-Regenerate` makes new synthetic data.
- Every run starts clean: Kafka topics are recreated and HDFS is wiped, so datasets never mix. Add `-Keep` to skip that.

**Live streaming demo.** Keep the streaming job running in one terminal and replay the data slowly in another:
```powershell
docker compose exec spark spark-submit --master "local[2]" processing/stream_to_hdfs.py
docker compose exec spark python3 ingestion/producer.py --rate 200
```
Re-run `batch_analytics.py` (step 3/3 of the script) to refresh the dashboard.

**Using a Kaggle dataset.** `./scripts/docker_pipeline.ps1 -Source kaggle`. See *The dataset* above.

### Memory
The stack is tuned for Docker Desktop's default VM (about 4 GB on an 8 GB laptop):
- every JVM has a capped heap (Kafka, HDFS, Elasticsearch, Neo4j)
- MongoDB's cache is limited
- Spark runs in local mode in a single container
- the API container also serves the built dashboard

Close other heavy apps while the pipeline runs. If you have more RAM, you can give Docker more through `%UserProfile%\.wslconfig`:
```ini
[wsl2]
memory=6GB
```
Then run `wsl --shutdown` and restart Docker Desktop. You can also stop services you aren't using (`docker compose stop neo4j`). The API falls back to MongoDB when Neo4j is down.

### Useful commands
```powershell
docker compose ps                          # what is running
docker compose logs -f api                 # follow a service's logs
docker compose exec namenode hdfs dfs -ls -R /socialscope | more   # browse the data lake
docker compose down                        # stop (data is kept in volumes)
docker compose down -v                     # stop and delete all data
```

---

## How the analytics work

**Engagement.** `engagement = likes + comments + shares`. *Engagement rate* is engagement ÷ the author's followers. The brand's rate uses only posts from its official account. The audience rate covers everyone else.

**Sentiment.** The VADER compound score runs as a vectorised Spark `pandas_udf`. A score ≥ 0.05 counts as positive and ≤ −0.05 as negative.

**Performance score (leaderboard).** Each metric is min-max normalised across competitors, then weighted: 25% engagement, 25% sentiment, 20% share of voice, 15% follower growth, 15% complaint health (fewer complaints per mention scores higher).

**Topics.** Text is tokenised and stop words removed (Spark ML `StopWordsRemover`). Bigrams come from `NGram`, and brand names are filtered out. Hashtags are added on top. Each topic gets:
- volume
- average sentiment
- growth over the last 7 days vs the prior 7
- *lift*: how much more one brand talks about the topic than the market does

**Complaints.** A post counts as a complaint if it isn't from the brand and either:
- it's negative and matches the complaint taxonomy in `common/config.py` (battery, connectivity, pricing, customer service, delivery, refunds, …), or
- it's strongly negative (≤ −0.4).

*Frequently discussed problems* are the most common bigrams inside complaints.

**Gaps.** For each category, every competitor's complaint rate (% of its mentions) is compared with yours and with the market average. A gap is flagged when the competitor is above 1.2× the market rate *and* you're below 0.6× the competitor's rate. Gaps are ranked by `gap × competitor/market`.

**Influencers.** These are non-brand authors with ≥ 10k followers. Average sentiment toward a brand makes them a *promoter* or a *detractor*. Score = log₁₀(followers) × ln(1+engagement) × (0.5 + positive share). In Neo4j this is stored as `(:Influencer)-[:PROMOTES|CRITICIZES]->(:Brand)` and `(:Influencer)-[:ACTIVE_ON]->(:Platform)`. That makes questions like "who promotes several competitors?" a single Cypher query.

**Trend prediction.** For each brand and product over the last 14 days, Spark computes:
- `regr_slope` and `regr_intercept` of daily mentions
- the engagement slope and sentiment slope
- momentum: second half of the window vs the first

These are combined with tanh-squashed weights into a score. A logistic function turns the score into a *probability of trending*. The regression line gives a 7-day mention forecast.

### Handy Cypher queries (Neo4j browser)
```cypher
// Influencers promoting a competitor
MATCH (i:Influencer)-[e:PROMOTES]->(b:Brand {name:'Zephyr'}) RETURN i, e, b ORDER BY e.score DESC LIMIT 25;
// Influencers shared by several brands
MATCH (i:Influencer)-[:PROMOTES]->(b:Brand) WITH i, collect(b.name) AS brands WHERE size(brands) > 1 RETURN i.handle, brands;
// Topics two brands have in common
MATCH (a:Brand {name:'Auralis'})-[:DISCUSSED_WITH]->(t:Topic)<-[:DISCUSSED_WITH]-(b:Brand {name:'Kinetiq'}) RETURN t.name;
```

## API reference
`GET /api/health · /api/meta · /api/overview · /api/engagement?platform= · /api/timeseries?platform=&brand= · /api/followers?platform= · /api/topics?brand= · /api/topics/trending · /api/complaints?brand= · /api/problems?brand= · /api/gaps?my_brand= · /api/trends?level=brand|product · /api/influencers?brand=&role=promoter|detractor · /api/influencers/shared · /api/graph · /api/search?q=&brand=&platform=&sentiment=&complaints_only=`

Interactive docs are at http://localhost:8000/docs.
