"""Narrative ORM model."""

from __future__ import annotations

from sqlalchemy import String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from pymander.db.models.base import Base, TimestampMixin, UUIDMixin


class Narrative(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "narratives"

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="emerging")
    keywords: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    representative_content_ids: Mapped[list] = mapped_column(
        ARRAY(UUID(as_uuid=True)), server_default="{}"
    )
    parent_narrative_id: Mapped[str | None] = mapped_column(UUID(as_uuid=True))
    related_narrative_ids: Mapped[list] = mapped_column(
        ARRAY(UUID(as_uuid=True)), server_default="{}"
    )
    snapshots: Mapped[dict | None] = mapped_column(JSONB)
