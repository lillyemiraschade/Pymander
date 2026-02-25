#!/usr/bin/env bash
set -euo pipefail

BROKER="localhost:9092"

TOPICS=(
  "raw.twitter"
  "raw.reddit"
  "raw.telegram"
  "raw.youtube"
  "raw.tiktok"
  "raw.rss"
  "raw.web"
  "enriched.content"
  "narrative.events"
  "network.events"
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  docker compose exec kafka kafka-topics \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

echo "All topics created."
