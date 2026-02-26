"""Embedding generation pipeline using sentence-transformers + Qdrant."""

from __future__ import annotations

import asyncio
import time

import structlog
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from redis.asyncio import Redis
from sentence_transformers import SentenceTransformer

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.consumer import KafkaConsumerWrapper
from pymander.ingestion.topics import ALL_RAW_TOPICS
from pymander.schemas.content import UnifiedContentRecord

logger = structlog.get_logger()

MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 is 384-dimensional
BATCH_SIZE = 256
FLUSH_INTERVAL = 5.0


class EmbeddingPipeline:
    """Consumes content from Kafka, generates embeddings, stores in Qdrant."""

    def __init__(
        self,
        qdrant: AsyncQdrantClient,
        metrics: MetricsCollector,
        collection_name: str = "content_embeddings",
    ) -> None:
        self.qdrant = qdrant
        self.metrics = metrics
        self.collection_name = collection_name
        self.model: SentenceTransformer | None = None
        self._batch: list[UnifiedContentRecord] = []
        self._running = True

    async def setup(self) -> None:
        """Load model and ensure Qdrant collection exists."""
        logger.info("embedding_pipeline_loading_model", model=MODEL_NAME)
        self.model = await asyncio.to_thread(SentenceTransformer, MODEL_NAME)
        logger.info("embedding_pipeline_model_loaded")

        collections = await self.qdrant.get_collections()
        existing = [c.name for c in collections.collections]
        if self.collection_name not in existing:
            await self.qdrant.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIM, distance=Distance.COSINE
                ),
            )
            logger.info(
                "qdrant_collection_created", name=self.collection_name
            )

    def _prepare_text(self, record: UnifiedContentRecord) -> str:
        """Combine title and text for embedding."""
        parts = []
        if record.title:
            parts.append(record.title)
        if record.text:
            parts.append(record.text)
        text = " ".join(parts)
        words = text.split()[:400]  # ~512 tokens
        return " ".join(words)

    async def process_batch(
        self, records: list[UnifiedContentRecord]
    ) -> None:
        """Generate embeddings for a batch and upsert into Qdrant."""
        if not records or not self.model:
            return

        start = time.time()
        texts = [self._prepare_text(r) for r in records]

        embeddings = await asyncio.to_thread(
            self.model.encode,
            texts,
            batch_size=BATCH_SIZE,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        points = []
        for record, embedding in zip(records, embeddings, strict=False):
            points.append(
                PointStruct(
                    id=str(record.id),
                    vector=embedding.tolist(),
                    payload={
                        "platform": str(record.platform),
                        "content_type": str(record.content_type),
                        "created_at": record.created_at.isoformat(),
                        "created_at_ts": record.created_at.timestamp(),
                        "collected_at": record.collected_at.isoformat(),
                        "author_id": record.actor.platform_id,
                        "text_preview": texts[
                            records.index(record)
                        ][:200],
                    },
                )
            )

        await self.qdrant.upsert(
            collection_name=self.collection_name, points=points
        )

        elapsed = time.time() - start
        await self.metrics.increment(
            "embeddings.generated", value=len(points)
        )
        await self.metrics.timer("embeddings.batch.duration", elapsed)
        logger.info(
            "embedding_batch_processed",
            count=len(points),
            elapsed=f"{elapsed:.2f}s",
        )

    async def _flush(self) -> None:
        if self._batch:
            batch = self._batch[:]
            self._batch.clear()
            await self.process_batch(batch)

    async def run(self) -> None:
        """Consume from Kafka and process embeddings in batches."""
        await self.setup()

        consumer = KafkaConsumerWrapper(
            *ALL_RAW_TOPICS,
            group_id="pymander-embedding",
        )
        await consumer.start()
        logger.info("embedding_pipeline_started")

        last_flush = time.time()
        try:
            async for msg in consumer.messages():
                if not self._running:
                    break
                try:
                    record = UnifiedContentRecord.model_validate(msg)
                    self._batch.append(record)
                except Exception as e:
                    logger.warning(
                        "embedding_record_parse_error", error=str(e)
                    )
                    await self.metrics.increment("embeddings.parse_errors")
                    continue

                if (
                    len(self._batch) >= BATCH_SIZE
                    or time.time() - last_flush >= FLUSH_INTERVAL
                ):
                    await self._flush()
                    last_flush = time.time()

                await self.metrics.gauge(
                    "embeddings.queue_depth", len(self._batch)
                )
        finally:
            await self._flush()
            await consumer.stop()

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    qdrant = AsyncQdrantClient(
        host=settings.qdrant.host, port=settings.qdrant.port,
        api_key=settings.qdrant.api_key or None,
        check_compatibility=False,
    )

    pipeline = EmbeddingPipeline(qdrant, metrics)
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        pipeline.stop()
    finally:
        await qdrant.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
