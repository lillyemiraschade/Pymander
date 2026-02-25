"""Abstract source adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator

from pymander.schemas.content import UnifiedContentRecord


class AbstractSourceAdapter(ABC):
    """Base class for all platform-specific ingestion adapters."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly disconnect from the data source."""

    @abstractmethod
    async def fetch(self, **kwargs) -> AsyncGenerator[UnifiedContentRecord, None]:
        """Yield unified content records from the source."""
        yield  # type: ignore[misc]
