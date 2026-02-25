"""Tests for the UnifiedContentRecord schema."""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from pydantic import ValidationError

from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    EntityMention,
    GeoLocation,
    NLPEnrichment,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, EntityType, Platform, SentimentLabel


def _make_record(**overrides) -> UnifiedContentRecord:
    defaults = dict(
        platform=Platform.TWITTER,
        content_type=ContentType.POST,
        platform_content_id="tweet-12345",
        created_at=datetime(2024, 6, 15, 12, 0, 0),
        text="This is test content about narrative intelligence.",
        actor=ActorInfo(
            platform_id="user-1",
            username="testuser",
            display_name="Test User",
            follower_count=1000,
        ),
    )
    defaults.update(overrides)
    return UnifiedContentRecord(**defaults)


class TestUnifiedContentRecord:
    def test_minimal_valid_record(self):
        record = _make_record()
        assert record.platform == Platform.TWITTER
        assert record.content_type == ContentType.POST
        assert record.actor.username == "testuser"
        assert record.id is not None

    def test_auto_generated_fields(self):
        record = _make_record()
        assert isinstance(record.id, uuid.UUID)
        assert isinstance(record.collected_at, datetime)

    def test_full_record_with_enrichments(self):
        record = _make_record(
            engagement=EngagementMetrics(likes=100, shares=50, replies=25, views=5000),
            geo=GeoLocation(
                latitude=40.7128, longitude=-74.006, country_code="US", city="New York"
            ),
            nlp=NLPEnrichment(
                language="en",
                sentiment=SentimentLabel.POSITIVE,
                sentiment_score=0.85,
                topics=["politics", "media"],
                keywords=["narrative", "intelligence"],
                entities=[
                    EntityMention(
                        text="United States",
                        entity_type=EntityType.LOCATION,
                        confidence=0.95,
                    )
                ],
            ),
            hashtags=["#osint", "#narratives"],
        )
        assert record.engagement.likes == 100
        assert record.geo.city == "New York"
        assert record.nlp.sentiment == SentimentLabel.POSITIVE
        assert len(record.nlp.entities) == 1

    def test_serialization_roundtrip(self):
        record = _make_record()
        data = record.model_dump(mode="json")
        restored = UnifiedContentRecord.model_validate(data)
        assert restored.platform == record.platform
        assert restored.actor.username == record.actor.username
        assert restored.id == record.id

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            UnifiedContentRecord(
                platform=Platform.TWITTER,
                content_type=ContentType.POST,
                # missing platform_content_id, created_at, actor
            )

    def test_invalid_platform_raises(self):
        with pytest.raises(ValidationError):
            _make_record(platform="nonexistent_platform")

    def test_sentiment_score_bounds(self):
        with pytest.raises(ValidationError):
            NLPEnrichment(sentiment_score=1.5)
        with pytest.raises(ValidationError):
            NLPEnrichment(sentiment_score=-1.5)

    def test_confidence_bounds(self):
        with pytest.raises(ValidationError):
            EntityMention(text="x", entity_type=EntityType.PERSON, confidence=2.0)

    def test_defaults_are_empty_collections(self):
        record = _make_record()
        assert record.hashtags == []
        assert record.narrative_ids == []
        assert record.media_urls == []
        assert record.nlp.topics == []

    def test_all_platforms_valid(self):
        for platform in Platform:
            record = _make_record(platform=platform)
            assert record.platform == platform
