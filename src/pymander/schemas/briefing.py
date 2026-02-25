"""Briefing and prediction schema models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from pymander.schemas.enums import BriefingType


class Briefing(BaseModel):
    """An auto-generated intelligence briefing."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: BriefingType = BriefingType.DAILY
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    period_start: datetime | None = None
    period_end: datetime | None = None
    content: str = ""
    sections: dict = Field(default_factory=dict)
    data_snapshot: dict = Field(default_factory=dict)
    model_used: str = ""
    token_cost: int = 0
    status: str = "generated"


class PatternMatch(BaseModel):
    """A historical narrative pattern match."""

    matched_narrative_id: str
    matched_narrative_summary: str = ""
    similarity_score: float = Field(ge=0.0, le=1.0)
    outcome_summary: str = ""
    matched_lifecycle: dict = Field(default_factory=dict)
    similarity_dimensions: dict = Field(default_factory=dict)


class Prediction(BaseModel):
    """A heuristic prediction for a narrative."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    narrative_id: str
    prediction_type: str  # platform_migration, peak_timing, behavioral_impact, coordination_risk
    description: str = ""
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    basis: list[PatternMatch] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    predicted_at: datetime = Field(default_factory=datetime.utcnow)
    predicted_timeframe_hours: float | None = None
    outcome: str | None = None  # filled in later when prediction can be evaluated
