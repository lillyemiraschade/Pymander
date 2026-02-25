"""Unified content record — the core schema for all ingested content."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from pymander.schemas.enums import ContentType, EntityType, Platform, SentimentLabel


class ActorInfo(BaseModel):
    """Metadata about the content author."""

    platform_id: str
    username: str
    display_name: str | None = None
    follower_count: int | None = None
    following_count: int | None = None
    account_created_at: datetime | None = None
    is_verified: bool = False
    bio: str | None = None
    profile_url: str | None = None


class EngagementMetrics(BaseModel):
    """Engagement counters at collection time."""

    likes: int = 0
    shares: int = 0
    replies: int = 0
    views: int | None = None
    quotes: int = 0
    bookmarks: int = 0


class GeoLocation(BaseModel):
    """Optional geolocation data."""

    latitude: float | None = None
    longitude: float | None = None
    country_code: str | None = None
    region: str | None = None
    city: str | None = None


class EntityMention(BaseModel):
    """A named entity extracted from content."""

    text: str
    entity_type: EntityType
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    start_offset: int | None = None
    end_offset: int | None = None


class NLPEnrichment(BaseModel):
    """NLP-derived enrichments (populated by pipeline stages)."""

    language: str | None = None
    sentiment: SentimentLabel | None = None
    sentiment_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    topics: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    entities: list[EntityMention] = Field(default_factory=list)
    summary: str | None = None
    embedding: list[float] | None = None


class UnifiedContentRecord(BaseModel):
    """The single canonical schema for every piece of content flowing through Pymander.

    Approximately 3 KB per record when serialized to JSON.
    """

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    platform: Platform
    content_type: ContentType
    platform_content_id: str
    created_at: datetime
    collected_at: datetime = Field(default_factory=datetime.utcnow)

    # Content body
    text: str | None = None
    title: str | None = None
    url: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    language: str | None = None

    # Threading / hierarchy
    parent_id: str | None = None
    root_id: str | None = None
    conversation_id: str | None = None

    # Author
    actor: ActorInfo

    # Engagement snapshot
    engagement: EngagementMetrics = Field(default_factory=EngagementMetrics)

    # Location
    geo: GeoLocation | None = None

    # NLP enrichments (populated downstream)
    nlp: NLPEnrichment = Field(default_factory=NLPEnrichment)

    # Tags / labels
    hashtags: list[str] = Field(default_factory=list)
    narrative_ids: list[uuid.UUID] = Field(default_factory=list)

    # Raw payload for debugging
    raw_payload: dict | None = None
