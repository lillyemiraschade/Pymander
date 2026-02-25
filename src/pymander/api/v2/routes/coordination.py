"""Client API v2 — Coordination detection endpoints."""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key

router = APIRouter(prefix="/coordination", tags=["coordination"])


@router.get("/clusters")
async def list_clusters(
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=50, le=200),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """List active coordination clusters."""
    cluster_ids = await redis.smembers("coordination:clusters:active")
    clusters = []
    for cid in cluster_ids:
        cid_str = cid.decode() if isinstance(cid, bytes) else cid
        raw = await redis.get(f"coordination:cluster:{cid_str}")
        if raw:
            cluster = json.loads(raw)
            if cluster.get("confidence", 0) >= min_confidence:
                clusters.append(cluster)
    clusters.sort(key=lambda c: c.get("confidence", 0), reverse=True)
    return {"count": len(clusters[:limit]), "clusters": clusters[:limit]}


@router.get("/clusters/{cluster_id}")
async def get_cluster(
    cluster_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Cluster detail with full evidence."""
    raw = await redis.get(f"coordination:cluster:{cluster_id}")
    if not raw:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Cluster not found")
    return json.loads(raw)


@router.get("/signals")
async def list_signals(
    hours: int = Query(default=24, le=168),
    signal_type: str | None = Query(default=None),
    limit: int = Query(default=100, le=500),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Recent coordination signals."""
    cutoff = time.time() - (hours * 3600)
    signal_ids = await redis.zrangebyscore(
        "coordination:signals:index", cutoff, "+inf",
    )
    signals = []
    for sid in signal_ids:
        sid_str = sid.decode() if isinstance(sid, bytes) else sid
        raw = await redis.get(f"coordination:signal:{sid_str}")
        if raw:
            signal = json.loads(raw)
            if signal_type and signal.get("type") != signal_type:
                continue
            signals.append(signal)
            if len(signals) >= limit:
                break
    signals.sort(key=lambda s: s.get("detected_at", ""), reverse=True)
    return {"count": len(signals), "signals": signals}
