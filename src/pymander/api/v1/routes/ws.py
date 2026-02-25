"""WebSocket endpoint for real-time dashboard updates."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from pymander.core.config import get_settings
from pymander.core.metrics import MetricsCollector

router = APIRouter(tags=["websocket"])

PLATFORMS = ["reddit", "rss", "twitter", "telegram", "youtube", "4chan", "web"]


async def _collect_realtime_metrics(redis: Redis) -> dict:
    m = MetricsCollector(redis)

    source_status = {}
    total_rpm = 0.0
    for platform in PLATFORMS:
        rpm = await m.get_rate_per_minute(f"{platform}.posts.ingested")
        rpm += await m.get_rate_per_minute(f"{platform}.comments.ingested")
        rpm += await m.get_rate_per_minute(f"{platform}.articles.ingested")
        total_rpm += rpm
        errors = await m.get_counter_for_day(f"{platform}.errors")

        status = "gray"
        total = await m.get_counter(f"{platform}.posts.ingested")
        total += await m.get_counter(f"{platform}.comments.ingested")
        total += await m.get_counter(f"{platform}.articles.ingested")
        if total > 0:
            status = "red" if (errors > 10 or rpm == 0) else (
                "yellow" if errors > 0 else "green"
            )

        source_status[platform] = {
            "status": status,
            "rpm": round(rpm, 1),
            "errors": errors,
        }

    emb_throughput = await m.get_rate_per_minute("embeddings.generated")
    narratives = await m.get_counter("narratives.validated")
    anomalies = await m.get_counter_for_day("engagement.anomalies.detected")

    return {
        "records_per_minute": round(total_rpm, 1),
        "sources": source_status,
        "embedding_rpm": round(emb_throughput, 1),
        "active_narratives": narratives,
        "anomalies_today": anomalies,
    }


@router.websocket("/ws/metrics")
async def metrics_websocket(websocket: WebSocket) -> None:
    """Push real-time metric updates every 5 seconds."""
    await websocket.accept()
    settings = get_settings()
    redis = Redis.from_url(settings.redis.url)
    try:
        while True:
            metrics = await _collect_realtime_metrics(redis)
            await websocket.send_json(metrics)
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await redis.aclose()
