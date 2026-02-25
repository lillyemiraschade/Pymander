"""Tests for cross-platform identity resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from pymander.intelligence.identity import CrossPlatformLinker


@pytest.fixture
def linker():
    neo4j = AsyncMock()
    redis = AsyncMock()
    redis.lrange = AsyncMock(return_value=[])
    metrics = AsyncMock()
    return CrossPlatformLinker(neo4j, redis, metrics)


async def test_username_exact_match(linker):
    conf = await linker.method_username_matching("testuser", "testuser")
    assert conf == 0.60


async def test_username_case_insensitive(linker):
    conf = await linker.method_username_matching("TestUser", "testuser")
    assert conf == 0.60


async def test_username_fuzzy_match(linker):
    conf = await linker.method_username_matching("testuser1", "testuser2")
    assert conf == 0.35


async def test_username_stripped_match(linker):
    conf = await linker.method_username_matching("test_user", "testuser")
    assert conf == 0.45


async def test_username_no_match(linker):
    conf = await linker.method_username_matching("alice", "bob")
    assert conf == 0.0


async def test_bio_cross_reference_match(linker):
    bio = "Follow me on twitter: twitter.com/myhandle"
    conf = await linker.method_bio_cross_reference(bio, "twitter", "myhandle")
    assert conf == 0.90


async def test_bio_cross_reference_no_bio(linker):
    conf = await linker.method_bio_cross_reference(None, "twitter", "user")
    assert conf == 0.0


async def test_bio_cross_reference_no_match(linker):
    bio = "I like cats and dogs"
    conf = await linker.method_bio_cross_reference(bio, "twitter", "user")
    assert conf == 0.0


async def test_content_fingerprinting_no_data(linker):
    conf = await linker.method_content_fingerprinting("uuid_a", "uuid_b")
    assert conf == 0.0


async def test_behavioral_fingerprinting_with_overlap(linker):
    actor_a = {
        "active_hours_utc": [9, 10, 11, 12, 13, 14],
        "primary_topics": ["politics", "tech", "news"],
    }
    actor_b = {
        "active_hours_utc": [10, 11, 12, 13, 14, 15],
        "primary_topics": ["politics", "tech", "science"],
    }
    conf = await linker.method_behavioral_fingerprinting(actor_a, actor_b)
    assert conf > 0.0
    assert conf <= 0.4  # Max from behavioral alone


async def test_resolve_identity_same_platform(linker):
    actor_a = {"primary_platform": "reddit", "username": "user1", "internal_uuid": "a"}
    actor_b = {"primary_platform": "reddit", "username": "user1", "internal_uuid": "b"}
    result = await linker.resolve_identity(actor_a, actor_b)
    assert result is None  # Same platform = no link


async def test_resolve_identity_high_confidence(linker):
    actor_a = {
        "primary_platform": "reddit",
        "username": "myuser",
        "bio": "Follow me on twitter: twitter.com/myuser",
        "internal_uuid": "uuid_a",
    }
    actor_b = {
        "primary_platform": "twitter",
        "username": "myuser",
        "bio": "",
        "internal_uuid": "uuid_b",
    }
    result = await linker.resolve_identity(actor_a, actor_b)
    assert result is not None
    assert result.confidence > 0.90  # Username match + bio cross-reference
