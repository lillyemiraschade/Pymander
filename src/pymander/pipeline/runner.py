"""Pipeline orchestrator (stub)."""

from __future__ import annotations

import structlog

from pymander.pipeline.stage import PipelineStage
from pymander.schemas.content import UnifiedContentRecord

logger = structlog.get_logger()


class PipelineRunner:
    """Runs content records through a sequence of enrichment stages."""

    def __init__(self, stages: list[PipelineStage] | None = None) -> None:
        self.stages = stages or []

    def add_stage(self, stage: PipelineStage) -> None:
        self.stages.append(stage)

    async def run(self, record: UnifiedContentRecord) -> UnifiedContentRecord:
        for stage in self.stages:
            logger.debug("pipeline_stage_start", stage=stage.name)
            record = await stage.process(record)
            logger.debug("pipeline_stage_done", stage=stage.name)
        return record
