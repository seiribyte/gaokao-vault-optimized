from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from conftest import make_mock_pool_and_conn

from gaokao_vault.scheduler.task_manager import TaskManager


def test_task_manager_delegates_start_and_finish() -> None:
    pool, _, _ = make_mock_pool_and_conn()
    manager = TaskManager(pool)
    stats = {"new": 1, "updated": 0, "unchanged": 2, "failed": 0}

    with (
        patch("gaokao_vault.scheduler.task_manager.create_task", new=AsyncMock(return_value=7)) as create,
        patch("gaokao_vault.scheduler.task_manager.update_task_stats", new=AsyncMock()) as update,
    ):
        assert asyncio.run(manager.start_task("schools", {"mode": "full"})) == 7
        asyncio.run(manager.finish_task(7, stats))

    create.assert_awaited_once_with(pool, "schools", {"mode": "full"})
    update.assert_awaited_once_with(pool, 7, stats, None)


def test_task_manager_reads_status_and_recent_rows() -> None:
    pool, conn, _ = make_mock_pool_and_conn()
    manager = TaskManager(pool)
    conn.fetchrow.return_value = {"id": 7, "status": "success"}
    conn.fetch.return_value = [{"id": 7, "task_type": "schools"}]

    assert asyncio.run(manager.get_task_status(7)) == {"id": 7, "status": "success"}
    assert asyncio.run(manager.list_recent_tasks(limit=3)) == [{"id": 7, "task_type": "schools"}]
    conn.fetch.assert_awaited_once()
    assert conn.fetch.await_args.args[-1] == 3
