"""Abstract pipeline stage interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pymander.schemas.content import UnifiedContentRecord


class PipelineStage(ABC):
    """A single processing step in the enrichment pipeline."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def process(self, record: UnifiedContentRecord) -> UnifiedContentRecord:
        """Process a content record and return the enriched version."""
        ...
