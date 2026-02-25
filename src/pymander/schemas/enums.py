"""Shared enumerations for the unified schema."""

from __future__ import annotations

from enum import StrEnum


class Platform(StrEnum):
    TWITTER = "twitter"
    REDDIT = "reddit"
    TELEGRAM = "telegram"
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    FOURCHAN = "4chan"
    GAB = "gab"
    TRUTH_SOCIAL = "truth_social"
    RUMBLE = "rumble"
    RSS = "rss"
    WEB = "web"
    BLUESKY = "bluesky"
    SUBSTACK = "substack"
    WIKIPEDIA = "wikipedia"
    OTHER = "other"


class ContentType(StrEnum):
    POST = "post"
    COMMENT = "comment"
    REPLY = "reply"
    REPOST = "repost"
    QUOTE = "quote"
    ARTICLE = "article"
    VIDEO = "video"
    IMAGE = "image"
    THREAD = "thread"
    FORWARD = "forward"
    EDIT = "edit"
    OTHER = "other"


class NarrativeStatus(StrEnum):
    EMERGING = "emerging"
    GROWING = "growing"
    VIRAL = "viral"
    PEAKING = "peaking"
    DECLINING = "declining"
    DORMANT = "dormant"
    DEAD = "dead"
    RESURGENT = "resurgent"


class SentimentLabel(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class EntityType(StrEnum):
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    EVENT = "event"
    HASHTAG = "hashtag"
    URL = "url"
    OTHER = "other"


class CoordinationSignalType(StrEnum):
    TEMPORAL_BURST = "temporal_burst"
    SYNCHRONIZED_ACTIVATION = "synchronized_activation"
    TIME_ZONE_ANOMALY = "time_zone_anomaly"
    SEMANTIC_CLONE = "semantic_clone"
    TEMPLATE_LANGUAGE = "template_language"
    AMPLIFICATION_CHAIN = "amplification_chain"
    STAR_TOPOLOGY = "star_topology"
    FRESH_ACCOUNT_SWARM = "fresh_account_swarm"
    POSTING_REGULARITY = "posting_regularity"
    CONTENT_DIVERSITY_ANOMALY = "content_diversity_anomaly"
    ENGAGEMENT_ASYMMETRY = "engagement_asymmetry"


class AlertSeverity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class BriefingType(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    ON_DEMAND = "on_demand"
    ALERT = "alert"
