"""Tests for mock adapter generating valid records."""

from __future__ import annotations

from pymander.ingestion.adapters.mock import MockSourceAdapter
from pymander.schemas.content import UnifiedContentRecord


class TestMockAdapter:
    async def test_generates_valid_records(self):
        adapter = MockSourceAdapter()
        await adapter.connect()
        records = []
        async for record in adapter.fetch(count=5):
            records.append(record)
        await adapter.disconnect()

        assert len(records) == 5
        for record in records:
            assert isinstance(record, UnifiedContentRecord)
            assert record.platform is not None
            assert record.actor.username is not None

    async def test_records_serialize(self):
        adapter = MockSourceAdapter()
        await adapter.connect()
        async for record in adapter.fetch(count=1):
            data = record.model_dump(mode="json")
            restored = UnifiedContentRecord.model_validate(data)
            assert restored.id == record.id
        await adapter.disconnect()
