"""Tests for the Neo4j graph builder pipeline."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from pymander.network.graph_builder import GraphBuilder
from pymander.schemas.content import ActorInfo, EngagementMetrics, UnifiedContentRecord
from pymander.schemas.enums import ContentType, Platform


def _make_record(
    platform=Platform.REDDIT,
    author_id="test_user",
    content_id="post_123",
    parent_id=None,
    root_id=None,
    raw_payload=None,
) -> UnifiedContentRecord:
    return UnifiedContentRecord(
        id=uuid.uuid4(),
        platform=platform,
        content_type=ContentType.POST,
        platform_content_id=content_id,
        created_at=datetime.now(UTC),
        text="Test content",
        actor=ActorInfo(platform_id=author_id, username=author_id),
        engagement=EngagementMetrics(likes=10),
        parent_id=parent_id,
        root_id=root_id,
        raw_payload=raw_payload or {},
    )


@pytest.fixture
def graph_builder():
    neo4j = AsyncMock()
    neo4j.execute_write = AsyncMock(return_value=[{"uuid": "test-uuid-123"}])
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    metrics = AsyncMock()
    metrics.increment = AsyncMock()
    return GraphBuilder(neo4j, redis, metrics)


async def test_upsert_actor(graph_builder):
    actor_data = {"platform_id": "user1", "username": "user1", "display_name": "User One"}
    result = await graph_builder.upsert_actor(actor_data, "reddit")
    assert result is not None
    graph_builder.neo4j.execute_write.assert_called_once()


async def test_cache_content_author(graph_builder):
    await graph_builder.cache_content_author("post_123", "reddit", "uuid-123")
    graph_builder.redis.set.assert_called_once()
    call_args = graph_builder.redis.set.call_args
    assert "content:author:reddit:post_123" in str(call_args)


async def test_process_content_basic(graph_builder):
    record = _make_record()
    await graph_builder.process_content(record)
    # Should upsert actor and cache mapping
    assert graph_builder.neo4j.execute_write.call_count >= 1
    graph_builder.metrics.increment.assert_any_call("network.content_processed")


async def test_process_content_with_reply(graph_builder):
    graph_builder.redis.get = AsyncMock(return_value=b"parent-author-uuid")
    record = _make_record(parent_id="parent_post_456")
    await graph_builder.process_content(record)
    # Should create REPLIED_TO edge
    graph_builder.metrics.increment.assert_any_call("network.edges.replied_to")


async def test_process_content_with_mentions(graph_builder):
    record = _make_record(raw_payload={"mentions": ["mentioned_user"]})
    await graph_builder.process_content(record)
    graph_builder.metrics.increment.assert_any_call("network.edges.mentioned")


async def test_process_content_with_forward(graph_builder):
    record = _make_record(
        platform=Platform.TELEGRAM,
        raw_payload={"forwarded_from_channel_id": "channel_123"},
    )
    await graph_builder.process_content(record)
    graph_builder.metrics.increment.assert_any_call("network.edges.forwarded")


async def test_upsert_edge_skips_self(graph_builder):
    await graph_builder.upsert_edge("same-uuid", "same-uuid", "REPLIED_TO", "reddit")
    graph_builder.neo4j.execute_write.assert_not_called()


async def test_resolve_author_from_content_cached(graph_builder):
    graph_builder.redis.get = AsyncMock(return_value=b"cached-uuid")
    result = await graph_builder.resolve_author_from_content("post_1", "reddit")
    assert result == "cached-uuid"


async def test_resolve_author_from_content_miss(graph_builder):
    graph_builder.redis.get = AsyncMock(return_value=None)
    result = await graph_builder.resolve_author_from_content("post_1", "reddit")
    assert result is None


async def test_stop(graph_builder):
    graph_builder.stop()
    assert graph_builder._running is False
