"""Tests for the pattern matching and prediction engine."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from pymander.intelligence.prediction import PatternMatcher, PredictionEngine
from pymander.schemas.briefing import PatternMatch


@pytest.fixture
def matcher():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.scan = AsyncMock(return_value=(0, []))
    metrics = AsyncMock()
    return PatternMatcher(redis, metrics)


@pytest.fixture
def engine(matcher):
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    redis.scan = AsyncMock(return_value=(0, []))
    metrics = AsyncMock()
    metrics.increment = AsyncMock()
    return PredictionEngine(matcher, redis, metrics)


def test_velocity_similarity_identical():
    sim = PatternMatcher._velocity_similarity([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
    assert sim > 0.95


def test_velocity_similarity_opposite():
    sim = PatternMatcher._velocity_similarity([1, 2, 3, 4, 5], [5, 4, 3, 2, 1])
    assert sim < 0.3


def test_velocity_similarity_empty():
    assert PatternMatcher._velocity_similarity([], [1, 2, 3]) == 0.0
    assert PatternMatcher._velocity_similarity([1, 2], []) == 0.0


def test_platform_similarity():
    sim = PatternMatcher._platform_similarity(
        ["reddit", "twitter"], ["reddit", "twitter", "telegram"]
    )
    assert 0.5 < sim < 0.8  # 2/3 overlap


def test_platform_similarity_identical():
    sim = PatternMatcher._platform_similarity(["reddit", "twitter"], ["reddit", "twitter"])
    assert sim == 1.0


def test_platform_similarity_disjoint():
    sim = PatternMatcher._platform_similarity(["reddit"], ["twitter"])
    assert sim == 0.0


def test_origin_similarity():
    assert PatternMatcher._origin_similarity("reddit", "reddit") == 1.0
    assert PatternMatcher._origin_similarity("reddit", "twitter") == 0.0
    assert PatternMatcher._origin_similarity("", "twitter") == 0.0


async def test_find_matches_no_data(matcher):
    matches = await matcher.find_matches("narrative_1")
    assert matches == []  # No narrative found


async def test_predict_peak_timing_no_matches(engine):
    pred = await engine.predict_peak_timing("n1", [])
    assert pred is None


async def test_predict_peak_timing_with_matches(engine):
    matches = [
        PatternMatch(
            matched_narrative_id="hist_1",
            matched_narrative_summary="Historical",
            similarity_score=0.8,
            matched_lifecycle={"total_duration_hours": 48},
        ),
        PatternMatch(
            matched_narrative_id="hist_2",
            matched_narrative_summary="Historical 2",
            similarity_score=0.7,
            matched_lifecycle={"total_duration_hours": 72},
        ),
    ]
    pred = await engine.predict_peak_timing("n1", matches)
    assert pred is not None
    assert pred.prediction_type == "peak_timing"
    assert pred.confidence > 0
    assert pred.predicted_timeframe_hours > 0
    assert len(pred.caveats) > 0


async def test_predict_platform_migration(engine):
    engine.redis.get = AsyncMock(
        return_value=json.dumps({"platforms": ["reddit"]}).encode()
    )
    matches = [
        PatternMatch(
            matched_narrative_id="h1",
            similarity_score=0.8,
            matched_lifecycle={"platforms_reached": ["reddit", "twitter"]},
        ),
        PatternMatch(
            matched_narrative_id="h2",
            similarity_score=0.7,
            matched_lifecycle={"platforms_reached": ["reddit", "twitter", "telegram"]},
        ),
    ]
    pred = await engine.predict_platform_migration("n1", matches)
    assert pred is not None
    assert "twitter" in pred.description


async def test_predict_coordination_risk(engine):
    engine.redis.get = AsyncMock(
        return_value=json.dumps({"coordination_score": 0.3}).encode()
    )
    matches = [
        PatternMatch(
            matched_narrative_id="h1",
            similarity_score=0.8,
            matched_lifecycle={"coordination_detected": True},
        ),
        PatternMatch(
            matched_narrative_id="h2",
            similarity_score=0.7,
            matched_lifecycle={"coordination_detected": False},
        ),
    ]
    pred = await engine.predict_coordination_risk("n1", matches)
    assert pred is not None
    assert pred.prediction_type == "coordination_risk"
