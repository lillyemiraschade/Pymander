"""HDBSCAN narrative clustering on recent embeddings from Qdrant."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta

import hdbscan
import numpy as np
import structlog
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    PointStruct,
    Range,
    VectorParams,
)
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector
from pymander.ingestion.producer import KafkaProducerWrapper
from pymander.ingestion.topics import NARRATIVE_EVENTS

logger = structlog.get_logger()

# HDBSCAN parameters — starting values, will need tuning
MIN_CLUSTER_SIZE = 25
MIN_SAMPLES = 10
CLUSTER_SELECTION_EPSILON = 0.0
METRIC = "euclidean"  # L2-normalized vectors: euclidean ≈ cosine

CENTROID_MATCH_THRESHOLD = 0.80
CLUSTERING_INTERVAL = 1800  # 30 minutes
LOOKBACK_HOURS = 24
EMBEDDING_DIM = 384


class NarrativeClusterer:
    """Runs HDBSCAN on recent embeddings to detect narrative clusters."""

    def __init__(
        self,
        qdrant: AsyncQdrantClient,
        redis: Redis,
        metrics: MetricsCollector,
        producer: KafkaProducerWrapper,
        content_collection: str = "content_embeddings",
        centroid_collection: str = "narrative_centroids",
    ) -> None:
        self.qdrant = qdrant
        self.redis = redis
        self.metrics = metrics
        self.producer = producer
        self.content_collection = content_collection
        self.centroid_collection = centroid_collection
        self._running = True

    async def setup(self) -> None:
        """Ensure narrative_centroids collection exists."""
        collections = await self.qdrant.get_collections()
        existing = [c.name for c in collections.collections]
        if self.centroid_collection not in existing:
            await self.qdrant.create_collection(
                collection_name=self.centroid_collection,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIM, distance=Distance.COSINE
                ),
            )
            logger.info(
                "qdrant_collection_created",
                name=self.centroid_collection,
            )

    async def fetch_recent_embeddings(
        self,
    ) -> tuple[list[str], np.ndarray] | None:
        """Fetch embeddings from the last LOOKBACK_HOURS hours."""
        cutoff = (
            datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)
        ).isoformat()

        points = []
        offset = None
        while True:
            result = await self.qdrant.scroll(
                collection_name=self.content_collection,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="created_at",
                            range=Range(gte=cutoff),
                        )
                    ]
                ),
                limit=1000,
                with_vectors=True,
                offset=offset,
            )
            batch_points, next_offset = result
            points.extend(batch_points)
            if next_offset is None or len(batch_points) == 0:
                break
            offset = next_offset

            if len(points) >= 100000:
                break

        if len(points) < MIN_CLUSTER_SIZE * 2:
            logger.info(
                "insufficient_data_for_clustering", count=len(points)
            )
            return None

        ids = [p.id for p in points]
        vectors = np.array([p.vector for p in points])
        return ids, vectors

    async def match_existing_narrative(
        self, centroid: np.ndarray
    ) -> str | None:
        """Check if centroid matches an existing narrative."""
        try:
            results = await self.qdrant.query_points(
                collection_name=self.centroid_collection,
                query=centroid.tolist(),
                limit=1,
            )
            if results.points and results.points[0].score > CENTROID_MATCH_THRESHOLD:
                return results.points[0].payload.get("narrative_id")
        except Exception:
            pass
        return None

    async def create_narrative_candidate(
        self, centroid: np.ndarray, content_ids: list[str]
    ) -> str:
        """Create a new narrative candidate and store its centroid."""
        narrative_id = str(uuid.uuid4())

        await self.qdrant.upsert(
            collection_name=self.centroid_collection,
            points=[
                PointStruct(
                    id=narrative_id,
                    vector=centroid.tolist(),
                    payload={
                        "narrative_id": narrative_id,
                        "content_count": len(content_ids),
                        "created_at": datetime.now(UTC).isoformat(),
                        "status": "candidate",
                    },
                )
            ],
        )

        # Store candidate details in Redis for validation service
        import json

        await self.redis.set(
            f"narrative:candidate:{narrative_id}",
            json.dumps({
                "narrative_id": narrative_id,
                "content_ids": content_ids[:50],  # Sample for validation
                "content_count": len(content_ids),
                "created_at": datetime.now(UTC).isoformat(),
            }),
            ex=86400,
        )

        await self.producer.send(
            NARRATIVE_EVENTS,
            {
                "event": "narrative_candidate_created",
                "narrative_id": narrative_id,
                "content_count": len(content_ids),
            },
        )

        return narrative_id

    async def update_narrative(
        self, narrative_id: str, content_ids: list[str]
    ) -> None:
        """Add new content to an existing narrative."""
        await self.producer.send(
            NARRATIVE_EVENTS,
            {
                "event": "narrative_updated",
                "narrative_id": narrative_id,
                "new_content_count": len(content_ids),
            },
        )

    async def run_clustering(self) -> None:
        """Run one clustering cycle."""
        start = time.time()
        result = await self.fetch_recent_embeddings()
        if result is None:
            return

        ids, vectors = result
        logger.info("clustering_start", vectors=len(vectors))

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=MIN_CLUSTER_SIZE,
            min_samples=MIN_SAMPLES,
            cluster_selection_epsilon=CLUSTER_SELECTION_EPSILON,
            metric=METRIC,
            core_dist_n_jobs=-1,
        )
        labels = await asyncio.to_thread(clusterer.fit_predict, vectors)

        unique_labels = set(labels)
        unique_labels.discard(-1)
        noise_ratio = float((labels == -1).sum()) / len(labels)

        await self.metrics.gauge(
            "clustering.clusters_found", len(unique_labels)
        )
        await self.metrics.gauge("clustering.noise_ratio", noise_ratio)

        for label in unique_labels:
            cluster_mask = labels == label
            cluster_ids = [
                ids[i] for i in range(len(ids)) if cluster_mask[i]
            ]
            cluster_vectors = vectors[cluster_mask]
            centroid = cluster_vectors.mean(axis=0)

            matched = await self.match_existing_narrative(centroid)
            if matched:
                await self.update_narrative(matched, cluster_ids)
                await self.metrics.increment("clustering.narrative_updates")
            else:
                await self.create_narrative_candidate(centroid, cluster_ids)
                await self.metrics.increment("clustering.new_candidates")

        elapsed = time.time() - start
        await self.metrics.timer("clustering.duration", elapsed)
        await self.redis.set(
            "metrics:clustering:last_run",
            datetime.now(UTC).isoformat(),
        )
        logger.info(
            "clustering_complete",
            clusters=len(unique_labels),
            noise_ratio=f"{noise_ratio:.2%}",
            elapsed=f"{elapsed:.2f}s",
        )

    async def run(self) -> None:
        """Run clustering on a schedule."""
        await self.setup()
        logger.info("clustering_service_started")
        while self._running:
            try:
                await self.run_clustering()
            except Exception as e:
                logger.error("clustering_error", error=str(e))
                await self.metrics.increment("clustering.errors")
            await asyncio.sleep(CLUSTERING_INTERVAL)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    settings = get_settings()
    from pymander.core.logging import setup_logging

    setup_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    metrics = MetricsCollector(redis)
    producer = KafkaProducerWrapper()
    await producer.start()
    qdrant = AsyncQdrantClient(
        host=settings.qdrant.host, port=settings.qdrant.port
    )

    clusterer = NarrativeClusterer(qdrant, redis, metrics, producer)
    try:
        await clusterer.run()
    except KeyboardInterrupt:
        clusterer.stop()
    finally:
        await qdrant.close()
        await producer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
