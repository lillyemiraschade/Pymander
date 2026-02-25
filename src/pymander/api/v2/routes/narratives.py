"""Client API v2 — Narrative endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key

router = APIRouter(prefix="/narratives", tags=["narratives"])


@router.get("")
async def list_narratives(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    cursor: str | None = Query(default=None),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """List narratives with filters."""
    narratives = []
    scan_cursor = int(cursor) if cursor else 0
    while True:
        scan_cursor, keys = await redis.scan(
            scan_cursor, match="narrative:validated:*", count=100,
        )
        for key in keys:
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw)
                if status and data.get("status") != status:
                    continue
                narratives.append(data)
                if len(narratives) >= limit:
                    break
        if scan_cursor == 0 or len(narratives) >= limit:
            break
    return {
        "count": len(narratives),
        "cursor": str(scan_cursor) if scan_cursor else None,
        "narratives": narratives,
    }


@router.get("/{narrative_id}")
async def get_narrative(
    narrative_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Full narrative detail."""
    raw = await redis.get(f"narrative:validated:{narrative_id}")
    if not raw:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Narrative not found")
    narrative = json.loads(raw)
    velocity_raw = await redis.get(f"narrative:velocity:{narrative_id}")
    velocity = json.loads(velocity_raw) if velocity_raw else None
    return {"narrative": narrative, "velocity": velocity}


@router.get("/{narrative_id}/velocity")
async def get_narrative_velocity(
    narrative_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Narrative velocity time-series."""
    raw = await redis.get(f"narrative:velocity:{narrative_id}")
    return {"narrative_id": narrative_id, "velocity": json.loads(raw) if raw else []}


@router.get("/{narrative_id}/predictions")
async def get_narrative_predictions(
    narrative_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Pattern matches and predictions for a narrative."""
    raw = await redis.get(f"narrative:predictions:{narrative_id}")
    if not raw:
        return {"narrative_id": narrative_id, "predictions": []}
    pred_ids = json.loads(raw)
    predictions = []
    for pid in pred_ids:
        pred_raw = await redis.get(f"prediction:{pid}")
        if pred_raw:
            predictions.append(json.loads(pred_raw))
    return {"narrative_id": narrative_id, "predictions": predictions}


@router.get("/{narrative_id}/migrations")
async def get_narrative_migrations(
    narrative_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Migration events for a narrative."""
    raw_list = await redis.lrange(f"narrative:migrations:{narrative_id}", 0, 49)
    migrations = [json.loads(r) for r in raw_list]
    return {"narrative_id": narrative_id, "migrations": migrations}
