"""Narrative API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis

from pymander.api.deps import get_redis

router = APIRouter(prefix="/narratives", tags=["narratives"])


@router.get("")
async def list_narratives(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    redis: Redis = Depends(get_redis),
) -> dict:
    """List validated narratives."""
    # Scan for validated narratives stored in Redis
    narratives = []
    cursor = 0
    while True:
        cursor, keys = await redis.scan(
            cursor, match="narrative:validated:*", count=100
        )
        for key in keys:
            import json

            raw = await redis.get(key)
            if raw:
                narrative = json.loads(raw)
                if status and narrative.get("status") != status:
                    continue
                narratives.append(narrative)
                if len(narratives) >= limit:
                    break
        if cursor == 0 or len(narratives) >= limit:
            break

    return {
        "count": len(narratives),
        "narratives": narratives,
    }


@router.get("/{narrative_id}")
async def get_narrative(
    narrative_id: str,
    redis: Redis = Depends(get_redis),
) -> dict:
    """Full narrative detail."""
    import json

    raw = await redis.get(f"narrative:validated:{narrative_id}")
    if not raw:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Narrative not found")

    narrative = json.loads(raw)

    # Get velocity history if available
    velocity_raw = await redis.get(
        f"narrative:velocity:{narrative_id}"
    )
    velocity = json.loads(velocity_raw) if velocity_raw else None

    return {
        "narrative": narrative,
        "velocity": velocity,
    }
