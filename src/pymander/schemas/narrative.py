"""Narrative schema models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from pymander.schemas.enums import NarrativeStatus


class NarrativeSnapshot(BaseModel):
    """A point-in-time measurement of narrative strength."""

    timestamp: datetime
    content_count: int = 0
    actor_count: int = 0
    velocity: float = 0.0
    sentiment_avg: float | None = None


class NarrativeObject(BaseModel):
    """A detected narrative cluster with lifecycle tracking."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    title: str
    description: str | None = None
    status: NarrativeStatus = NarrativeStatus.EMERGING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    keywords: list[str] = Field(default_factory=list)
    representative_content_ids: list[uuid.UUID] = Field(default_factory=list)
    snapshots: list[NarrativeSnapshot] = Field(default_factory=list)

    parent_narrative_id: uuid.UUID | None = None
    related_narrative_ids: list[uuid.UUID] = Field(default_factory=list)
