"""Faker-based mock data generator for testing."""

from __future__ import annotations

import random
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime

from faker import Faker

from pymander.ingestion.adapters.base import AbstractSourceAdapter
from pymander.schemas.content import (
    ActorInfo,
    EngagementMetrics,
    NLPEnrichment,
    UnifiedContentRecord,
)
from pymander.schemas.enums import ContentType, Platform, SentimentLabel

fake = Faker()


def _random_record() -> UnifiedContentRecord:
    platform = random.choice(list(Platform))
    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=platform,
        content_type=random.choice(list(ContentType)),
        platform_content_id=str(uuid.uuid4()),
        created_at=fake.date_time_between(start_date="-30d", end_date="now"),
        collected_at=datetime.utcnow(),
        text=fake.paragraph(nb_sentences=random.randint(1, 5)),
        url=fake.url(),
        language="en",
        actor=ActorInfo(
            platform_id=str(uuid.uuid4()),
            username=fake.user_name(),
            display_name=fake.name(),
            follower_count=random.randint(0, 1_000_000),
            following_count=random.randint(0, 10_000),
            account_created_at=fake.date_time_between(start_date="-5y", end_date="-1d"),
            is_verified=random.random() < 0.1,
            bio=fake.sentence(),
        ),
        engagement=EngagementMetrics(
            likes=random.randint(0, 50_000),
            shares=random.randint(0, 10_000),
            replies=random.randint(0, 5_000),
            views=random.randint(0, 1_000_000),
        ),
        nlp=NLPEnrichment(
            language="en",
            sentiment=random.choice(list(SentimentLabel)),
            sentiment_score=round(random.uniform(-1, 1), 3),
            topics=[fake.word() for _ in range(random.randint(0, 3))],
            keywords=[fake.word() for _ in range(random.randint(0, 5))],
        ),
        hashtags=[f"#{fake.word()}" for _ in range(random.randint(0, 4))],
    )


class MockSourceAdapter(AbstractSourceAdapter):
    """Generates fake content records for testing and development."""

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def fetch(self, count: int = 10, **kwargs) -> AsyncGenerator[UnifiedContentRecord, None]:
        for _ in range(count):
            yield _random_record()
