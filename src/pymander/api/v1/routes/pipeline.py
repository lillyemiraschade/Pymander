"""Pipeline status API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.core.metrics import MetricsCollector

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/status")
async def get_pipeline_status(
    redis: Redis = Depends(get_redis),
) -> dict:
    """Status of each pipeline stage."""
    m = MetricsCollector(redis)

    # Embedding
    emb_throughput = await m.get_rate_per_minute("embeddings.generated")
    emb_queue = await m.get_gauge("embeddings.queue_depth")
    emb_errors = await m.get_counter_for_day("embeddings.parse_errors")

    # Clustering
    last_run = await redis.get("metrics:clustering:last_run")
    clusters = await m.get_gauge("clustering.clusters_found")
    noise = await m.get_gauge("clustering.noise_ratio")
    clustering_errors = await m.get_counter_for_day("clustering.errors")

    # Narrative validation
    claude_calls = await m.get_counter_for_day("claude.api.calls")
    claude_cost = await m.get_counter_for_day("claude.api.cost_cents")
    validated = await m.get_counter_for_day("narratives.validated")
    rejected = await m.get_counter_for_day("narratives.rejected")

    # Engagement poller
    queue_depth = await redis.zcard("engagement_poll_queue")
    snapshots = await m.get_counter_for_day("engagement.snapshots.captured")
    snapshot_errors = await m.get_counter_for_day("engagement.snapshots.errors")
    anomalies = await m.get_counter_for_day("engagement.anomalies.detected")

    # Image hasher
    images_hashed = await m.get_counter_for_day("images.hashed")
    image_errors = await m.get_counter_for_day("images.hash_errors")
    cross_platform = await m.get_counter_for_day("images.cross_platform_detected")

    return {
        "embedding": {
            "throughput_per_minute": round(emb_throughput, 1),
            "queue_depth": emb_queue or 0,
            "errors_today": emb_errors,
        },
        "clustering": {
            "last_run": last_run.decode() if last_run else None,
            "clusters_found": clusters,
            "noise_ratio": noise,
            "errors_today": clustering_errors,
        },
        "narrative_validation": {
            "api_calls_today": claude_calls,
            "api_cost_cents_today": claude_cost,
            "validated_today": validated,
            "rejected_today": rejected,
        },
        "engagement_poller": {
            "queue_depth": queue_depth,
            "snapshots_today": snapshots,
            "errors_today": snapshot_errors,
            "anomalies_today": anomalies,
        },
        "image_hasher": {
            "hashed_today": images_hashed,
            "errors_today": image_errors,
            "cross_platform_today": cross_platform,
        },
    }
