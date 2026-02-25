"""Kafka → PostgreSQL ingestion consumer.

Reads UnifiedContentRecord messages from all raw.* Kafka topics,
persists them to the content_records table, and increments Redis
metrics counters so the dashboard shows accurate totals.
"""

from __future__ import annotations

import asyncio
import time

import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.db.models.content import ContentRecord
from pymander.ingestion.consumer import KafkaConsumerWrapper
from pymander.ingestion.topics import ALL_RAW_TOPICS
from pymander.schemas.content import UnifiedContentRecord

logger = structlog.get_logger()

# Map content_type → metric suffix used by the dashboard
_METRIC_SUFFIX = {
    "post": "posts",
    "comment": "comments",
    "reply": "comments",
    "repost": "posts",
    "quote": "posts",
    "article": "articles",
    "video": "posts",
    "image": "posts",
    "thread": "posts",
    "forward": "posts",
    "edit": "posts",
    "other": "posts",
}

BATCH_SIZE = 100
FLUSH_INTERVAL = 3.0


def _to_orm(r: UnifiedContentRecord) -> ContentRecord:
    """Convert a Pydantic record to an ORM model instance."""
    return ContentRecord(
        id=r.id,
        platform=str(r.platform),
        content_type=str(r.content_type),
        platform_content_id=r.platform_content_id,
        content_created_at=r.created_at,
        collected_at=r.collected_at,
        text=r.text,
        title=r.title,
        url=r.url,
        language=r.language,
        parent_id=r.parent_id,
        root_id=r.root_id,
        conversation_id=r.conversation_id,
        actor=r.actor.model_dump(),
        engagement=r.engagement.model_dump(),
        geo=r.geo.model_dump() if r.geo else None,
        nlp=r.nlp.model_dump(),
        hashtags=r.hashtags or [],
        raw_payload=None,
    )


async def _upsert_batch(
    session: AsyncSession, records: list[UnifiedContentRecord]
) -> int:
    """Insert records into content_records, skipping duplicates. Returns rows inserted."""
    if not records:
        return 0

    inserted = 0
    for r in records:
        # Check for existing record to avoid unique constraint violation
        existing = await session.execute(
            select(ContentRecord.id).where(
                ContentRecord.platform == str(r.platform),
                ContentRecord.platform_content_id == r.platform_content_id,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue
        session.add(_to_orm(r))
        inserted += 1

    if inserted:
        await session.commit()

    return inserted


async def run() -> None:
    settings = get_settings()

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)

    db_engine = create_async_engine(
        settings.db.url, echo=False, pool_size=settings.db.pool_size
    )
    session_factory = async_sessionmaker(
        db_engine, class_=AsyncSession, expire_on_commit=False
    )

    consumer = KafkaConsumerWrapper(
        *ALL_RAW_TOPICS,
        group_id="pymander-ingestion-v2",
    )
    await consumer.start()
    logger.info("ingestion_consumer_started", topics=ALL_RAW_TOPICS)

    batch: list[UnifiedContentRecord] = []
    last_flush = time.time()

    async def flush() -> None:
        nonlocal batch, last_flush
        if not batch:
            return
        to_insert = batch[:]
        batch = []
        last_flush = time.time()

        async with session_factory() as session:
            try:
                inserted = await _upsert_batch(session, to_insert)
            except Exception:
                logger.exception("ingestion_batch_failed", count=len(to_insert))
                return

        # Increment metrics for each record (even if it was a duplicate in PG,
        # the metric should reflect total consumed from Kafka)
        for r in to_insert:
            suffix = _METRIC_SUFFIX.get(str(r.content_type), "posts")
            await metrics.increment(f"{r.platform}.{suffix}.ingested")

        logger.info(
            "ingestion_batch_flushed",
            total=len(to_insert),
            inserted=inserted,
            skipped=len(to_insert) - inserted,
        )

    try:
        async for msg in consumer.messages():
            try:
                record = UnifiedContentRecord.model_validate(msg)
                batch.append(record)
            except Exception as e:
                logger.warning("ingestion_record_parse_error", error=str(e))
                continue

            if len(batch) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL:
                await flush()
    except KeyboardInterrupt:
        pass
    finally:
        await flush()
        await consumer.stop()
        await db_engine.dispose()
        await redis.aclose()
        logger.info("ingestion_consumer_stopped")


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)
    await run()


if __name__ == "__main__":
    asyncio.run(main())
