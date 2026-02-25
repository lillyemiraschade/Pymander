"""Async Neo4j driver wrapper for graph operations."""

from __future__ import annotations

from typing import Any

import structlog
from neo4j import AsyncDriver, AsyncGraphDatabase

from pymander.core.config import get_settings

logger = structlog.get_logger()


class Neo4jClient:
    """Async wrapper around the Neo4j Python driver."""

    def __init__(self, uri: str | None = None, user: str | None = None,
                 password: str | None = None) -> None:
        settings = get_settings()
        self._uri = uri or settings.neo4j.uri
        self._user = user or settings.neo4j.user
        self._password = password or settings.neo4j.password
        self._driver: AsyncDriver | None = None

    async def connect(self) -> None:
        self._driver = AsyncGraphDatabase.driver(
            self._uri, auth=(self._user, self._password),
        )
        await self._driver.verify_connectivity()
        logger.info("neo4j_connected", uri=self._uri)

    async def close(self) -> None:
        if self._driver:
            await self._driver.close()
            logger.info("neo4j_disconnected")

    async def execute(self, query: str, **params: Any) -> list[dict]:
        """Execute a Cypher query and return results as list of dicts."""
        if not self._driver:
            raise RuntimeError("Neo4j client not connected")
        async with self._driver.session() as session:
            result = await session.run(query, params)
            records = await result.data()
            return records

    async def execute_write(self, query: str, **params: Any) -> list[dict]:
        """Execute a write transaction."""
        if not self._driver:
            raise RuntimeError("Neo4j client not connected")
        async with self._driver.session() as session:

            async def _tx(tx):
                result = await tx.run(query, params)
                return await result.data()

            return await session.execute_write(_tx)

    async def execute_batch(self, queries: list[tuple[str, dict]]) -> None:
        """Execute multiple queries in a single transaction."""
        if not self._driver:
            raise RuntimeError("Neo4j client not connected")
        async with self._driver.session() as session:

            async def _tx(tx):
                for query, params in queries:
                    await tx.run(query, params)

            await session.execute_write(_tx)

    async def setup_constraints(self) -> None:
        """Create indexes and constraints for the graph schema."""
        constraints = [
            "CREATE CONSTRAINT actor_uuid IF NOT EXISTS"
            " FOR (a:Actor) REQUIRE a.internal_uuid IS UNIQUE",
            "CREATE CONSTRAINT actor_platform_key IF NOT EXISTS"
            " FOR (a:Actor) REQUIRE a.platform_key IS UNIQUE",
            "CREATE CONSTRAINT community_id IF NOT EXISTS"
            " FOR (c:Community) REQUIRE c.community_id IS UNIQUE",
            "CREATE INDEX actor_platform IF NOT EXISTS FOR (a:Actor) ON (a.primary_platform)",
            "CREATE INDEX actor_username IF NOT EXISTS FOR (a:Actor) ON (a.username)",
            "CREATE INDEX actor_influence IF NOT EXISTS FOR (a:Actor) ON (a.influence_score)",
            "CREATE INDEX actor_community IF NOT EXISTS FOR (a:Actor) ON (a.community_id)",
            "CREATE INDEX actor_last_seen IF NOT EXISTS FOR (a:Actor) ON (a.last_seen)",
        ]
        for query in constraints:
            try:
                await self.execute(query)
            except Exception as e:
                logger.debug("constraint_setup_skip", query=query[:60], error=str(e))
        logger.info("neo4j_constraints_created")
