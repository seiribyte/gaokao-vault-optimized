from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gaokao_vault.config import CrawlConfig
from gaokao_vault.constants import PHASE2_TYPES, PHASE3_TYPES
from gaokao_vault.scheduler.orchestrator import Orchestrator

NO_ACTIVE_CRAWL_MESSAGE = "No active crawl to stop"
CHECKPOINT_FAILURE_MESSAGE = "checkpoint failure"


class _CheckpointExceptionGroup(Exception):
    def __init__(self) -> None:
        super().__init__(CHECKPOINT_FAILURE_MESSAGE)
        self.exceptions = [FileExistsError("checkpoint replace failed")]

    def split(self, _predicate):
        return self, None


class _PausableTimeoutSpider:
    name = "pausable_timeout_spider"

    def __init__(self, **_kwargs) -> None:
        self._stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
        self._stop_event: asyncio.Event | None = None
        self.active = False
        self.pause_called_while_active = False

    async def stream(self):
        self._stop_event = asyncio.Event()
        self.active = True
        try:
            await self._stop_event.wait()
        finally:
            self.active = False

        if False:
            yield None

    def pause(self) -> None:
        self.pause_called_while_active = self.active
        if self._stop_event is None or not self.active:
            raise RuntimeError(NO_ACTIVE_CRAWL_MESSAGE)
        self._stop_event.set()


def test_run_single_pauses_active_spider_before_cancelling_timeout() -> None:
    created_spiders: list[_PausableTimeoutSpider] = []

    class _Spider(_PausableTimeoutSpider):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            created_spiders.append(self)

    config = CrawlConfig(spider_timeout=1)
    orch = Orchestrator(db_pool=MagicMock(), config=config, mode="full")
    orch.task_manager = MagicMock()
    orch.task_manager.start_task = AsyncMock(return_value=99)
    orch.task_manager.finish_task = AsyncMock()

    with patch("gaokao_vault.scheduler.orchestrator.SPIDER_MAP", {"schools": _Spider}):
        stats = asyncio.run(orch.run_single("schools"))

    assert stats["failed"] == 1
    assert created_spiders[0].pause_called_while_active is True
    orch.task_manager.finish_task.assert_awaited_once()


def test_run_spider_stream_propagates_checkpoint_errors() -> None:
    class _CheckpointFailSpider:
        name = "checkpoint_fail_spider"

        async def stream(self):
            raise _CheckpointExceptionGroup
            yield None

    with pytest.raises(_CheckpointExceptionGroup, match=CHECKPOINT_FAILURE_MESSAGE):
        asyncio.run(Orchestrator._run_spider_stream(_CheckpointFailSpider()))


def test_run_all_skips_phase3_when_phase2_has_failures() -> None:
    orch = Orchestrator(db_pool=MagicMock(), mode="full")
    phase2 = [{"failed": 1}] + [{"failed": 0}] * (len(PHASE2_TYPES) - 1)
    phase3 = [{"failed": 0}] * len(PHASE3_TYPES)

    with patch.object(orch, "_run_phase", new=AsyncMock(side_effect=[phase2, phase3])) as run_phase:
        outcome = asyncio.run(orch.run_all())

    run_phase.assert_awaited_once_with([t.value for t in PHASE2_TYPES])
    assert outcome.total == len(PHASE2_TYPES)
    assert outcome.failed == 1
    assert outcome.completed is True
    assert outcome.phase3_skipped is True
    assert outcome.successful is False
