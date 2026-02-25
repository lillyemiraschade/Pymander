"""Kafka topic name constants."""

# Raw ingestion topics
RAW_TWITTER = "raw.twitter"
RAW_REDDIT = "raw.reddit"
RAW_TELEGRAM = "raw.telegram"
RAW_YOUTUBE = "raw.youtube"
RAW_TIKTOK = "raw.tiktok"
RAW_RSS = "raw.rss"
RAW_WEB = "raw.web"
RAW_FOURCHAN = "raw.4chan"
RAW_BLUESKY = "raw.bluesky"
RAW_SUBSTACK = "raw.substack"
RAW_WIKIPEDIA = "raw.wikipedia"

# Processing topics
ENRICHED = "enriched.content"
MEDIA_TO_HASH = "media.to_hash"
VELOCITY_UPDATES = "processed.velocity_updates"

# Narrative topics
NARRATIVE_NEW = "narrative.new"
NARRATIVE_UPDATED = "narrative.updated"
NARRATIVE_VALIDATED = "narrative.validated"
NARRATIVE_EVENTS = "narrative.events"

# Network topics
NETWORK_EVENTS = "network.events"
NETWORK_EDGES = "network.edges"

# Alert topics
ALERTS_VELOCITY = "alerts.velocity"
ALERTS_COORDINATION = "alerts.coordination"
ALERTS_MIGRATION = "alerts.migration"
ALERTS_BEHAVIORAL = "alerts.behavioral"
VELOCITY_ANOMALIES = "alerts.velocity_anomaly"

# Briefing topics
BRIEFING_GENERATED = "briefings.generated"

ALL_RAW_TOPICS = [
    RAW_TWITTER, RAW_REDDIT, RAW_TELEGRAM, RAW_YOUTUBE, RAW_TIKTOK,
    RAW_RSS, RAW_WEB, RAW_FOURCHAN, RAW_BLUESKY, RAW_SUBSTACK, RAW_WIKIPEDIA,
]

ALL_TOPICS = ALL_RAW_TOPICS + [
    ENRICHED, MEDIA_TO_HASH, VELOCITY_UPDATES,
    NARRATIVE_NEW, NARRATIVE_UPDATED, NARRATIVE_VALIDATED, NARRATIVE_EVENTS,
    NETWORK_EVENTS, NETWORK_EDGES,
    ALERTS_VELOCITY, ALERTS_COORDINATION, ALERTS_MIGRATION, ALERTS_BEHAVIORAL,
    VELOCITY_ANOMALIES, BRIEFING_GENERATED,
]
