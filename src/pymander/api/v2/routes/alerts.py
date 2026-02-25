"""Client API v2 — Alert endpoints with SSE streaming."""

from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis
from sse_starlette.sse import EventSourceResponse

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key

router = APIRouter(prefix="/alerts", tags=["alerts"])


async def _collect_alerts(
    redis: Redis, hours: int, severity: str | None, limit: int,
) -> list[dict]:
    """Collect alerts from all alert indices."""
    cutoff = time.time() - (hours * 3600)
    alerts = []

    # Coordination signals
    signal_ids = await redis.zrangebyscore(
        "coordination:signals:index", cutoff, "+inf",
    )
    for sid in signal_ids:
        sid_str = sid.decode() if isinstance(sid, bytes) else sid
        raw = await redis.get(f"coordination:signal:{sid_str}")
        if raw:
            data = json.loads(raw)
            data["alert_category"] = "coordination"
            alerts.append(data)

    # Behavioral alerts
    alert_ids = await redis.zrangebyscore(
        "behavioral:alerts:index", cutoff, "+inf",
    )
    for aid in alert_ids:
        aid_str = aid.decode() if isinstance(aid, bytes) else aid
        raw = await redis.get(f"behavioral:alert:{aid_str}")
        if raw:
            data = json.loads(raw)
            data["alert_category"] = "behavioral"
            alerts.append(data)

    # Filter by severity if specified
    if severity:
        alerts = [a for a in alerts if a.get("severity") == severity]

    # Sort by detection time, newest first
    alerts.sort(
        key=lambda a: a.get("detected_at", a.get("timestamp", "")),
        reverse=True,
    )
    return alerts[:limit]


@router.get("")
async def list_alerts(
    hours: int = Query(default=24, le=168),
    severity: str | None = Query(default=None),
    limit: int = Query(default=100, le=500),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Alert feed with filters."""
    alerts = await _collect_alerts(redis, hours, severity, limit)
    return {"count": len(alerts), "alerts": alerts}


@router.get("/stream")
async def alert_stream(
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> EventSourceResponse:
    """SSE stream of real-time alerts."""

    async def event_generator():
        last_check = time.time()
        while True:
            await asyncio.sleep(5)
            now = time.time()
            # Check for new alerts since last check
            new_alerts = await _collect_alerts(
                redis, hours=1, severity=None, limit=10,
            )
            for alert in new_alerts:
                detected = alert.get("detected_at", "")
                if detected:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(detected)
                        if dt.timestamp() > last_check:
                            yield {
                                "event": "alert",
                                "data": json.dumps(alert),
                            }
                    except (ValueError, TypeError):
                        continue
            last_check = now

    return EventSourceResponse(event_generator())


@router.put("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Mark an alert as acknowledged."""
    # Try coordination signal
    raw = await redis.get(f"coordination:signal:{alert_id}")
    if raw:
        data = json.loads(raw)
        data["acknowledged"] = True
        data["acknowledged_by"] = _client.get("client", "unknown")
        await redis.set(
            f"coordination:signal:{alert_id}", json.dumps(data), ex=172800,
        )
        return {"status": "acknowledged", "alert_id": alert_id}

    # Try behavioral alert
    raw = await redis.get(f"behavioral:alert:{alert_id}")
    if raw:
        data = json.loads(raw)
        data["acknowledged"] = True
        data["acknowledged_by"] = _client.get("client", "unknown")
        await redis.set(
            f"behavioral:alert:{alert_id}", json.dumps(data), ex=604800,
        )
        return {"status": "acknowledged", "alert_id": alert_id}

    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail="Alert not found")
