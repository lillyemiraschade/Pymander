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
    OTHER = "other"


class NarrativeStatus(StrEnum):
    EMERGING = "emerging"
    GROWING = "growing"
    PEAKING = "peaking"
    DECLINING = "declining"
    DORMANT = "dormant"
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
