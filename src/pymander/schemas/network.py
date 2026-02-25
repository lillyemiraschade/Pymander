"""Network graph schema models."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class NetworkNode(BaseModel):
    """A node in the influence/coordination network."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    label: str
    node_type: str  # "actor", "narrative", "topic", "entity"
    platform: str | None = None
    properties: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class NetworkEdge(BaseModel):
    """An edge connecting two network nodes."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    source_id: uuid.UUID
    target_id: uuid.UUID
    edge_type: str  # "amplifies", "replies_to", "co_occurs", "mentions"
    weight: float = 1.0
    properties: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
