"""Dashboard metrics API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis

from pymander.api.deps import get_app_settings, get_redis
from pymander.core.config import Settings
from pymander.core.metrics import MetricsCollector

router = APIRouter(prefix="/metrics", tags=["metrics"])

PLATFORMS = ["reddit", "rss", "twitter", "telegram", "youtube", "4chan", "web"]


async def _get_metrics(redis: Redis) -> MetricsCollector:
    return MetricsCollector(redis)


@router.get("/overview")
async def get_overview_metrics(
    redis: Redis = Depends(get_redis),
    settings: Settings = Depends(get_app_settings),
) -> dict:
    """All metrics for the overview page."""
    m = await _get_metrics(redis)

    # Total records across all platforms
    total = 0
    today_total = 0
    source_health = []
    for platform in PLATFORMS:
        posts = await m.get_counter(f"{platform}.posts.ingested")
        comments = await m.get_counter(f"{platform}.comments.ingested")
        articles = await m.get_counter(f"{platform}.articles.ingested")
        platform_total = posts + comments + articles

        posts_today = await m.get_counter_for_day(f"{platform}.posts.ingested")
        comments_today = await m.get_counter_for_day(f"{platform}.comments.ingested")
        articles_today = await m.get_counter_for_day(f"{platform}.articles.ingested")
        platform_today = posts_today + comments_today + articles_today

        total += platform_total
        today_total += platform_today

        rpm = await m.get_rate_per_minute(f"{platform}.posts.ingested")
        rpm += await m.get_rate_per_minute(f"{platform}.comments.ingested")
        rpm += await m.get_rate_per_minute(f"{platform}.articles.ingested")

        errors_today = await m.get_counter_for_day(f"{platform}.errors")
        rate_limit_pauses = await m.get_counter_for_day(
            f"{platform}.rate_limit.pauses"
        )

        status = "gray"
        if platform_total > 0:
            if errors_today > 10 or rpm == 0:
                status = "red"
            elif errors_today > 0 or rate_limit_pauses > 5:
                status = "yellow"
            else:
                status = "green"

        source_health.append({
            "platform": platform,
            "status": status,
            "records_per_hour": rpm * 60,
            "records_today": platform_today,
            "errors_today": errors_today,
            "rate_limit_pauses": rate_limit_pauses,
        })

    # Overall RPM
    overall_rpm = sum(s["records_per_hour"] / 60 for s in source_health)
    active_sources = sum(
        1 for s in source_health if s["status"] in ("green", "yellow")
    )

    # Narrative counts
    narratives_validated = await m.get_counter("narratives.validated")

    # Pipeline status
    embedding_throughput = await m.get_rate_per_minute("embeddings.generated")
    last_clustering = await redis.get("metrics:clustering:last_run")
    clusters_found = await m.get_gauge("clustering.clusters_found")

    return {
        "total_records": total,
        "records_today": today_total,
        "records_per_minute": round(overall_rpm, 1),
        "active_sources": active_sources,
        "total_sources": len(PLATFORMS),
        "source_health": source_health,
        "active_narratives": narratives_validated,
        "pipeline": {
            "embedding_throughput": round(embedding_throughput, 1),
            "last_clustering": (
                last_clustering.decode() if last_clustering else None
            ),
            "clusters_found": clusters_found,
        },
    }


@router.get("/ingestion_rate")
async def get_ingestion_rate(
    hours: int = Query(default=24, le=168),
    redis: Redis = Depends(get_redis),
) -> dict:
    """Time-series ingestion rate by platform."""
    m = await _get_metrics(redis)
    series = {}
    for platform in PLATFORMS:
        for metric in ("posts.ingested", "comments.ingested", "articles.ingested"):
            key = f"{platform}.{metric}"
            hourly = await m.get_hourly_counts(key, hours=hours)
            if platform not in series:
                series[platform] = []
            if not series[platform]:
                series[platform] = [
                    {"timestamp": h["timestamp"], "count": h["count"]}
                    for h in hourly
                ]
            else:
                for i, h in enumerate(hourly):
                    if i < len(series[platform]):
                        series[platform][i]["count"] += h["count"]
    return {"hours": hours, "series": series}


@router.get("/source/{platform}")
async def get_source_metrics(
    platform: str,
    redis: Redis = Depends(get_redis),
) -> dict:
    """Detailed metrics for a specific source."""
    m = await _get_metrics(redis)

    posts = await m.get_counter(f"{platform}.posts.ingested")
    comments = await m.get_counter(f"{platform}.comments.ingested")
    articles = await m.get_counter(f"{platform}.articles.ingested")
    errors_today = await m.get_counter_for_day(f"{platform}.errors")
    rpm = await m.get_rate_per_minute(f"{platform}.posts.ingested")
    rpm += await m.get_rate_per_minute(f"{platform}.comments.ingested")
    rpm += await m.get_rate_per_minute(f"{platform}.articles.ingested")

    hourly = await m.get_hourly_counts(
        f"{platform}.posts.ingested", hours=24
    )

    return {
        "platform": platform,
        "total_posts": posts,
        "total_comments": comments,
        "total_articles": articles,
        "errors_today": errors_today,
        "records_per_minute": round(rpm, 1),
        "hourly_counts": hourly,
    }
