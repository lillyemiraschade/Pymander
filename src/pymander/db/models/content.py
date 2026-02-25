"""Content records ORM model."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from pymander.db.models.base import Base, TimestampMixin, UUIDMixin


class ContentRecord(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "content_records"

    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    content_type: Mapped[str] = mapped_column(String(32), nullable=False)
    platform_content_id: Mapped[str] = mapped_column(String(256), nullable=False)
    content_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    text: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(String(512))
    url: Mapped[str | None] = mapped_column(String(2048))
    language: Mapped[str | None] = mapped_column(String(8))

    parent_id: Mapped[str | None] = mapped_column(String(256))
    root_id: Mapped[str | None] = mapped_column(String(256))
    conversation_id: Mapped[str | None] = mapped_column(String(256))

    actor: Mapped[dict] = mapped_column(JSONB, nullable=False)
    engagement: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    geo: Mapped[dict | None] = mapped_column(JSONB)
    nlp: Mapped[dict] = mapped_column(JSONB, server_default="{}")

    hashtags: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    narrative_ids: Mapped[list] = mapped_column(ARRAY(UUID(as_uuid=True)), server_default="{}")

    raw_payload: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_content_platform_id", "platform", "platform_content_id", unique=True),
        Index("ix_content_created_at", "content_created_at"),
        Index("ix_content_platform", "platform"),
    )
