"""Kafka topic name constants."""

RAW_TWITTER = "raw.twitter"
RAW_REDDIT = "raw.reddit"
RAW_TELEGRAM = "raw.telegram"
RAW_YOUTUBE = "raw.youtube"
RAW_TIKTOK = "raw.tiktok"
RAW_RSS = "raw.rss"
RAW_WEB = "raw.web"

ENRICHED = "enriched.content"
NARRATIVE_EVENTS = "narrative.events"
NETWORK_EVENTS = "network.events"

ALL_RAW_TOPICS = [
    RAW_TWITTER,
    RAW_REDDIT,
    RAW_TELEGRAM,
    RAW_YOUTUBE,
    RAW_TIKTOK,
    RAW_RSS,
    RAW_WEB,
]

ALL_TOPICS = ALL_RAW_TOPICS + [ENRICHED, NARRATIVE_EVENTS, NETWORK_EVENTS]
