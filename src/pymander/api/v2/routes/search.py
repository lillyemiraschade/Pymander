"""Client API v2 — Global search endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key
from pymander.network.neo4j_client import Neo4jClient

router = APIRouter(prefix="/search", tags=["search"])


@router.get("")
async def global_search(
    q: str = Query(min_length=2),
    platform: str | None = Query(default=None),
    content_type: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Global search across all content types."""
    results = {"narratives": [], "actors": [], "alerts": []}

    # Search narratives
    cursor = 0
    while True:
        cursor, keys = await redis.scan(
            cursor, match="narrative:validated:*", count=100,
        )
        for key in keys:
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw)
                text = json.dumps(data).lower()
                if q.lower() in text:
                    results["narratives"].append({
                        "id": data.get("narrative_id", data.get("id")),
                        "summary": data.get("summary", ""),
                        "status": data.get("status", ""),
                        "type": "narrative",
                    })
        if cursor == 0 or len(results["narratives"]) >= limit:
            break

    # Search actors in Neo4j
    try:
        neo4j = Neo4jClient()
        await neo4j.connect()
        actor_results = await neo4j.execute("""
            MATCH (a:Actor)
            WHERE toLower(a.username) CONTAINS toLower($q)
               OR toLower(coalesce(a.display_name, '')) CONTAINS toLower($q)
            RETURN a.internal_uuid AS id, a.username AS username,
                   a.primary_platform AS platform,
                   a.influence_score AS influence
            ORDER BY a.influence_score DESC
            LIMIT $limit
        """, q=q, limit=limit)
        for actor in actor_results:
            results["actors"].append({**actor, "type": "actor"})
        await neo4j.close()
    except Exception:
        pass

    total = sum(len(v) for v in results.values())
    return {"query": q, "total": total, "results": results}


@router.get("/actors")
async def search_actors(
    q: str = Query(min_length=2),
    platform: str | None = Query(default=None),
    min_influence: float = Query(default=0.0),
    limit: int = Query(default=50, le=200),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Search actors by name/platform."""
    neo4j = Neo4jClient()
    await neo4j.connect()
    try:
        filters = [
            "(toLower(a.username) CONTAINS toLower($q)"
            " OR toLower(coalesce(a.display_name, ''))"
            " CONTAINS toLower($q))"
        ]
        params: dict = {"q": q, "limit": limit}
        if platform:
            filters.append("a.primary_platform = $platform")
            params["platform"] = platform
        if min_influence > 0:
            filters.append("a.influence_score >= $min_influence")
            params["min_influence"] = min_influence

        where = "WHERE " + " AND ".join(filters)
        results = await neo4j.execute(f"""
            MATCH (a:Actor)
            {where}
            RETURN a {{.*}} AS actor
            ORDER BY a.influence_score DESC
            LIMIT $limit
        """, **params)
        actors = [r["actor"] for r in results]
        return {"query": q, "count": len(actors), "actors": actors}
    finally:
        await neo4j.close()
