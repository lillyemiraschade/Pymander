"""Coordination detection schema models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from pymander.schemas.enums import AlertSeverity, CoordinationSignalType


class CoordinationSignal(BaseModel):
    """A single coordination signal detected by the analysis engine."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: CoordinationSignalType
    narrative_id: str | None = None
    accounts: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: dict = Field(default_factory=dict)
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    platform: str | None = None


class CoordinationCluster(BaseModel):
    """A group of accounts identified as operating in coordination."""

    cluster_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    accounts: list[str] = Field(default_factory=list)
    account_count: int = 0
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    signal_types: list[CoordinationSignalType] = Field(default_factory=list)
    signal_count: int = 0
    signals: list[dict] = Field(default_factory=list)
    associated_narratives: list[str] = Field(default_factory=list)
    first_detected: datetime | None = None
    last_signal: datetime | None = None
    estimated_reach: int = 0
    status: str = "active"
    analyst_notes: str = ""
    severity: AlertSeverity = AlertSeverity.MEDIUM


class IdentityLink(BaseModel):
    """A probabilistic link between two accounts on different platforms."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    account_a_uuid: str
    account_b_uuid: str
    account_a_platform: str = ""
    account_b_platform: str = ""
    confidence: float = Field(ge=0.0, le=1.0)
    methods: list[dict] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    verified: bool = False


class MigrationEvent(BaseModel):
    """A detected cross-platform narrative migration."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    narrative_id: str
    from_platform: str
    to_platform: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    migration_time_seconds: int = 0
    bridge_content_ids: list[str] = Field(default_factory=list)
    bridge_account_ids: list[str] = Field(default_factory=list)


class BehavioralSignalAlert(BaseModel):
    """Alert for detected behavioral change in a community."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: str  # language_shift, engagement_shift, migration
    community_id: str | None = None
    narrative_id: str | None = None
    severity: AlertSeverity = AlertSeverity.MEDIUM
    details: dict = Field(default_factory=dict)
    detected_at: datetime = Field(default_factory=datetime.utcnow)
