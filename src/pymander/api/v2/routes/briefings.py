"""Client API v2 — Briefing endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key
from pymander.core.metrics import MetricsCollector
from pymander.schemas.enums import BriefingType

router = APIRouter(prefix="/briefings", tags=["briefings"])


@router.get("")
async def list_briefings(
    limit: int = 20,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """List recent briefings."""
    briefing_ids = await redis.lrange("briefing:index", 0, limit - 1)
    briefings = []
    for bid in briefing_ids:
        bid_str = bid.decode() if isinstance(bid, bytes) else bid
        raw = await redis.get(f"briefing:{bid_str}")
        if raw:
            data = json.loads(raw)
            # Return summary without full content for list view
            briefings.append({
                "id": data.get("id"),
                "type": data.get("type"),
                "generated_at": data.get("generated_at"),
                "period_start": data.get("period_start"),
                "period_end": data.get("period_end"),
                "model_used": data.get("model_used"),
                "token_cost": data.get("token_cost"),
            })
    return {"count": len(briefings), "briefings": briefings}


@router.get("/latest")
async def get_latest_briefing(
    briefing_type: str = "daily",
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Get the most recent briefing of a given type."""
    raw = await redis.get(f"briefing:latest:{briefing_type}")
    if not raw:
        raise HTTPException(status_code=404, detail="No briefing found")
    return json.loads(raw)


@router.get("/{briefing_id}")
async def get_briefing(
    briefing_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Full briefing detail."""
    raw = await redis.get(f"briefing:{briefing_id}")
    if not raw:
        raise HTTPException(status_code=404, detail="Briefing not found")
    return json.loads(raw)


@router.post("/generate")
async def generate_briefing(
    hours: int = 24,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Generate an on-demand briefing."""
    from pymander.intelligence.briefings import BriefingGenerator

    metrics = MetricsCollector(redis)
    generator = BriefingGenerator(redis, metrics)
    briefing = await generator.generate_briefing(
        briefing_type=BriefingType.ON_DEMAND, hours=hours,
    )
    return briefing.model_dump(mode="json")
