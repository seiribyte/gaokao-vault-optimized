from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from pathlib import Path

from gaokao_vault.constants import TaskType
from gaokao_vault.scheduler.orchestrator import Orchestrator

logger = logging.getLogger(__name__)

_CATALOG_STAGES = (
    (TaskType.SCHOOLS.value, TaskType.MAJORS.value),
    (TaskType.SCHOOL_MAJORS.value,),
)
_LIAONING_STAGES = (
    (TaskType.SCORE_SEGMENTS.value, TaskType.MAJOR_STRENGTH_SIGNALS.value),
    (TaskType.ENROLLMENT_PLANS.value, TaskType.MAJOR_ADMISSION_RESULTS.value, TaskType.CHARTERS.value),
    (TaskType.DXSBB_ADMISSION_RESULTS.value,),
)


async def run_liaoning_profile(
    orchestrator: Orchestrator,
    *,
    refresh_catalog: bool = True,
    reference_path: Path | None = None,
) -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}
    if reference_path is not None:
        from gaokao_vault.scheduler.reference_catalog import sync_reference_schools

        async with orchestrator.db_pool.acquire() as conn:
            results["reference_catalog"] = await sync_reference_schools(conn, reference_path)
    stages: Sequence[Sequence[str]] = (*(_CATALOG_STAGES if refresh_catalog else ()), *_LIAONING_STAGES)
    for stage_number, task_types in enumerate(stages, start=1):
        logger.info("辽宁专项抓取阶段 %d: %s", stage_number, task_types)
        stage_results = await asyncio.gather(*(orchestrator.run_single(task_type) for task_type in task_types))
        for task_type, stats in zip(task_types, stage_results, strict=True):
            results[task_type] = stats
            if stats.get("failed", 0):
                logger.warning("辽宁专项任务存在失败项 task_type=%s stats=%s", task_type, stats)
    return results
