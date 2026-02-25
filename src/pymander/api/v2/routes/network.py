"""Client API v2 — Network graph endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from redis.asyncio import Redis

from pymander.api.deps import get_redis
from pymander.api.v2.auth import validate_api_key
from pymander.network.neo4j_client import Neo4jClient

router = APIRouter(prefix="/network", tags=["network"])


async def get_neo4j() -> Neo4jClient:
    """Get a Neo4j client instance."""
    client = Neo4jClient()
    await client.connect()
    return client


@router.get("/graph")
async def get_graph(
    community_id: str | None = Query(default=None),
    min_influence: float = Query(default=0.0, ge=0.0, le=1.0),
    platform: str | None = Query(default=None),
    limit: int = Query(default=500, le=10000),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Get graph data with filters for visualization."""
    neo4j = await get_neo4j()
    try:
        filters = []
        params: dict = {"limit": limit}
        if community_id:
            filters.append("a.community_id = $community_id")
            params["community_id"] = community_id
        if min_influence > 0:
            filters.append("a.influence_score >= $min_influence")
            params["min_influence"] = min_influence
        if platform:
            filters.append("a.primary_platform = $platform")
            params["platform"] = platform

        where = "WHERE " + " AND ".join(filters) if filters else ""

        nodes = await neo4j.execute(f"""
            MATCH (a:Actor)
            {where}
            RETURN a.internal_uuid AS id, a.username AS label,
                   a.primary_platform AS platform,
                   a.influence_score AS influence,
                   a.bridge_score AS bridge_score,
                   a.community_id AS community,
                   a.total_content_count AS content_count,
                   a.coordination_cluster_id AS coordination_cluster
            ORDER BY a.influence_score DESC
            LIMIT $limit
        """, **params)

        node_ids = [n["id"] for n in nodes]
        edges = []
        if node_ids:
            edges = await neo4j.execute("""
                UNWIND $ids AS nid
                MATCH (a:Actor {internal_uuid: nid})-[r]->(b:Actor)
                WHERE b.internal_uuid IN $ids
                RETURN a.internal_uuid AS source, b.internal_uuid AS target,
                       type(r) AS edge_type, r.weight AS weight,
                       r.interaction_count AS interactions
                LIMIT 5000
            """, ids=node_ids)

        return {"nodes": nodes, "edges": edges}
    finally:
        await neo4j.close()


@router.get("/communities")
async def list_communities(
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """List detected communities."""
    raw = await redis.get("network:communities")
    communities = json.loads(raw) if raw else []
    return {"count": len(communities), "communities": communities}


@router.get("/bridges")
async def list_bridges(
    limit: int = Query(default=50, le=1000),
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Top bridge nodes (accounts connecting otherwise disconnected communities)."""
    raw = await redis.get("network:top_bridges")
    bridges = json.loads(raw) if raw else []
    return {"count": len(bridges[:limit]), "bridges": bridges[:limit]}


@router.get("/actors/{actor_id}")
async def get_actor(
    actor_id: str,
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Full actor behavioral profile."""
    neo4j = await get_neo4j()
    try:
        results = await neo4j.execute("""
            MATCH (a:Actor {internal_uuid: $id})
            OPTIONAL MATCH (a)-[r]->(b:Actor)
            WITH a, count(r) AS outgoing_connections
            OPTIONAL MATCH (c:Actor)-[r2]->(a)
            WITH a, outgoing_connections, count(r2) AS incoming_connections
            OPTIONAL MATCH (a)-[:SAME_PERSON]-(linked:Actor)
            RETURN a {.*} AS actor,
                   outgoing_connections,
                   incoming_connections,
                   collect(linked {.internal_uuid, .username,
                       .primary_platform}) AS linked_accounts
        """, id=actor_id)
        if not results:
            raise HTTPException(status_code=404, detail="Actor not found")
        row = results[0]
        return {
            "actor": row["actor"],
            "outgoing_connections": row["outgoing_connections"],
            "incoming_connections": row["incoming_connections"],
            "linked_accounts": row["linked_accounts"],
        }
    finally:
        await neo4j.close()


@router.get("/actors/{actor_id}/history")
async def get_actor_history(
    actor_id: str,
    redis: Redis = Depends(get_redis),
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Actor behavioral history (recent activity)."""
    platforms = ["reddit", "twitter", "telegram", "youtube", "4chan"]
    history = []
    for plat in platforms:
        key = f"author:history:{plat}:{actor_id}"
        items = await redis.lrange(key, 0, 99)
        for item in items:
            data = json.loads(item)
            data["platform"] = plat
            history.append(data)
    history.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return {"actor_id": actor_id, "history": history[:200]}


@router.get("/path/{from_id}/{to_id}")
async def shortest_path(
    from_id: str,
    to_id: str,
    _client: dict = Depends(validate_api_key),
) -> dict:
    """Shortest path between two actors in the graph."""
    neo4j = await get_neo4j()
    try:
        results = await neo4j.execute("""
            MATCH p = shortestPath(
                (a:Actor {internal_uuid: $from_id})-[*..10]-(b:Actor {internal_uuid: $to_id})
            )
            RETURN [n IN nodes(p) | n {.internal_uuid, .username, .primary_platform}] AS nodes,
                   [r IN relationships(p) | {type: type(r), weight: r.weight}] AS edges,
                   length(p) AS path_length
        """, from_id=from_id, to_id=to_id)
        if not results:
            return {"path_found": False, "from": from_id, "to": to_id}
        row = results[0]
        return {
            "path_found": True,
            "path_length": row["path_length"],
            "nodes": row["nodes"],
            "edges": row["edges"],
        }
    finally:
        await neo4j.close()
