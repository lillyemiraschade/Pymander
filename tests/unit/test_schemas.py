"""Tests for Phase 3/4 schema models."""

from __future__ import annotations

from pymander.schemas.briefing import Briefing, PatternMatch, Prediction
from pymander.schemas.coordination import (
    BehavioralSignalAlert,
    CoordinationCluster,
    CoordinationSignal,
    IdentityLink,
    MigrationEvent,
)
from pymander.schemas.enums import (
    AlertSeverity,
    BriefingType,
    CoordinationSignalType,
    NarrativeStatus,
    Platform,
)


def test_coordination_signal_creation():
    signal = CoordinationSignal(
        type=CoordinationSignalType.TEMPORAL_BURST,
        accounts=["user1", "user2", "user3"],
        confidence=0.85,
        evidence={"window_seconds": 120},
    )
    assert signal.type == CoordinationSignalType.TEMPORAL_BURST
    assert len(signal.accounts) == 3
    assert signal.confidence == 0.85


def test_coordination_cluster_creation():
    cluster = CoordinationCluster(
        accounts=["a", "b", "c"],
        account_count=3,
        confidence=0.92,
        signal_types=[CoordinationSignalType.TEMPORAL_BURST],
        signal_count=5,
        severity=AlertSeverity.HIGH,
    )
    assert cluster.account_count == 3
    assert cluster.severity == AlertSeverity.HIGH


def test_identity_link():
    link = IdentityLink(
        account_a_uuid="uuid_a",
        account_b_uuid="uuid_b",
        account_a_platform="reddit",
        account_b_platform="twitter",
        confidence=0.85,
        methods=[{"method": "username_matching", "confidence": 0.6}],
    )
    assert link.confidence == 0.85
    assert len(link.methods) == 1


def test_migration_event():
    event = MigrationEvent(
        narrative_id="narr_1",
        from_platform="reddit",
        to_platform="twitter",
        migration_time_seconds=7200,
    )
    assert event.from_platform == "reddit"
    assert event.migration_time_seconds == 7200


def test_behavioral_signal_alert():
    alert = BehavioralSignalAlert(
        type="language_shift",
        community_id="comm_1",
        severity=AlertSeverity.HIGH,
        details={"ngram_drift": 0.25},
    )
    assert alert.type == "language_shift"
    assert alert.severity == AlertSeverity.HIGH


def test_briefing_model():
    briefing = Briefing(
        type=BriefingType.DAILY,
        content="Executive summary...",
        model_used="claude-sonnet-4-5-20250929",
        token_cost=3500,
    )
    assert briefing.type == BriefingType.DAILY
    assert briefing.token_cost == 3500


def test_pattern_match():
    match = PatternMatch(
        matched_narrative_id="hist_1",
        matched_narrative_summary="Past event",
        similarity_score=0.82,
        matched_lifecycle={"total_duration_hours": 72},
    )
    assert match.similarity_score == 0.82


def test_prediction_model():
    pred = Prediction(
        narrative_id="narr_1",
        prediction_type="peak_timing",
        description="Will peak in 24h",
        confidence=0.7,
        caveats=["External events may shift timing"],
        predicted_timeframe_hours=24.0,
    )
    assert pred.prediction_type == "peak_timing"
    assert len(pred.caveats) == 1


def test_new_enums():
    assert NarrativeStatus.VIRAL == "viral"
    assert NarrativeStatus.DEAD == "dead"
    assert Platform.BLUESKY == "bluesky"
    assert Platform.WIKIPEDIA == "wikipedia"
    assert CoordinationSignalType.STAR_TOPOLOGY == "star_topology"
    assert AlertSeverity.CRITICAL == "critical"
    assert BriefingType.ON_DEMAND == "on_demand"
