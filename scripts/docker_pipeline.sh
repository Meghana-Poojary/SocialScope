#!/usr/bin/env bash
# Runs the full pipeline inside the docker-compose stack (Git Bash / macOS / Linux).
#
#   ./scripts/docker_pipeline.sh                                  # synthetic sample
#   ./scripts/docker_pipeline.sh --regenerate                     # new synthetic sample first
#   ./scripts/docker_pipeline.sh --source kaggle                  # Kaggle "Customer Support on Twitter"
#   ./scripts/docker_pipeline.sh --source kaggle --brands AppleSupport,AmazonHelp,XboxSupport,AskPlayStation
#   ./scripts/docker_pipeline.sh --source kaggle --kaggle-file data/kaggle/twcs.csv
#   ./scripts/docker_pipeline.sh --source kaggle --format generic --kaggle-file data/kaggle/my.csv \
#       --mapping data/kaggle/mapping.example.json --brands Nike,Adidas,Puma
#
# Every run starts clean (Kafka topics recreated, HDFS wiped) unless --keep is given.
set -euo pipefail
cd "$(dirname "$0")/.."

SOURCE=synthetic; FORMAT=twcs; KFILE=""; MAPPING=""; BRANDS=""; MAXPB=30000; KEEP=0; REGEN=0
DATASET="thoughtvector/customer-support-on-twitter"
while [ $# -gt 0 ]; do
  case "$1" in
    --source) SOURCE="$2"; shift ;;
    --format) FORMAT="$2"; shift ;;
    --kaggle-file) KFILE="$2"; shift ;;
    --mapping) MAPPING="$2"; shift ;;
    --brands) BRANDS="$2"; shift ;;
    --dataset) DATASET="$2"; shift ;;
    --max-per-brand) MAXPB="$2"; shift ;;
    --keep) KEEP=1 ;;
    --regenerate) REGEN=1 ;;
    *) echo "unknown option $1"; exit 1 ;;
  esac
  shift
done

EXEC=(docker compose exec -T spark)
SUBMIT=("${EXEC[@]}" spark-submit --master "local[2]" --driver-memory 1200m)
PRODUCE=("${EXEC[@]}" python3 ingestion/producer.py)

if [ "$KEEP" = 0 ]; then
  echo "==> starting clean: wiping /socialscope in HDFS, recreating Kafka topics"
  MSYS_NO_PATHCONV=1 docker compose exec -T namenode hdfs dfs -rm -r -f /socialscope 2>&1 | grep -v WARN || true
  PRODUCE+=(--recreate-topics)
fi

if [ "$SOURCE" = synthetic ]; then
  if [ "$REGEN" = 1 ] || [ ! -f data/raw/posts.jsonl ]; then
    echo "==> 0/3 generating synthetic sample data"
    "${EXEC[@]}" python3 ingestion/generate_sample.py
  fi
  PRODUCE+=(--posts data/raw/posts.jsonl --metrics data/raw/brand_metrics.jsonl)
else
  if [ -z "$KFILE" ]; then
    [ "$FORMAT" = twcs ] || { echo "--kaggle-file is required for --format generic"; exit 1; }
    KFILE=$(find data/kaggle -name twcs.csv 2>/dev/null | head -1)
    if [ -z "$KFILE" ]; then
      CFG="$HOME/.kaggle/kaggle.json"
      if [ -z "${KAGGLE_USERNAME:-}" ] && [ -f "$CFG" ]; then
        KAGGLE_USERNAME=$(python3 -c "import json;print(json.load(open('$CFG'))['username'])")
        KAGGLE_KEY=$(python3 -c "import json;print(json.load(open('$CFG'))['key'])")
        export KAGGLE_USERNAME KAGGLE_KEY
      fi
      if [ -z "${KAGGLE_USERNAME:-}" ]; then
        echo "No twcs.csv in data/kaggle and no Kaggle API token. Download it from"
        echo "https://www.kaggle.com/datasets/$DATASET into data/kaggle/, or save a token as $CFG"
        exit 1
      fi
      echo "==> 0/3 downloading $DATASET from Kaggle"
      docker compose exec -T -e KAGGLE_USERNAME -e KAGGLE_KEY spark python3 ingestion/kaggle_download.py "$DATASET"
      KFILE=$(find data/kaggle -name twcs.csv | head -1)
    fi
  fi
  [ -z "$BRANDS" ] && [ "$FORMAT" = twcs ] && BRANDS="AppleSupport,AmazonHelp,SpotifyCares,Uber_Support,XboxSupport,AskPlayStation"
  ADAPT=("${EXEC[@]}" python3 ingestion/kaggle_adapter.py --format "$FORMAT" --input "$KFILE"
         --out data/raw/kaggle_posts.jsonl --max-per-brand "$MAXPB")
  [ -n "$BRANDS" ] && ADAPT+=(--brands "$BRANDS")
  [ -n "$MAPPING" ] && ADAPT+=(--mapping "$MAPPING")
  echo "==> 0/3 converting $KFILE to the SocialScope format"
  "${ADAPT[@]}"
  PRODUCE+=(--posts data/raw/kaggle_posts.jsonl --metrics none)
fi

echo "==> 1/3 producing records to Kafka";                  "${PRODUCE[@]}"
echo "==> 2/3 Spark Structured Streaming: Kafka -> HDFS";   "${SUBMIT[@]}" processing/stream_to_hdfs.py --once
echo "==> 3/3 Spark batch analytics -> Mongo / ES / Neo4j"; "${SUBMIT[@]}" processing/batch_analytics.py
echo "==> done ($SOURCE data). Dashboard: http://localhost:8000   API docs: http://localhost:8000/docs"
