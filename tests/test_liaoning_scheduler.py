from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from gaokao_vault.scheduler.liaoning import run_liaoning_profile


def test_liaoning_profile_runs_dependency_stages_in_order() -> None:
    orchestrator = MagicMock()
    orchestrator.run_single = AsyncMock(return_value={"new": 1, "updated": 0, "unchanged": 0, "failed": 0})

    with patch(
        "gaokao_vault.scheduler.reference_catalog.sync_gaokao_school_index",
        new=AsyncMock(return_value={"index_schools": 3000, "already_present": 2000, "added": 1000}),
    ):
        results = asyncio.run(run_liaoning_profile(orchestrator, refresh_catalog=True))

    assert orchestrator.run_single.await_args_list == [
        call("schools"),
        call("majors"),
        call("school_majors"),
        call("score_segments"),
        call("major_strength_signals"),
        call("enrollment_plans"),
        call("major_admission_results"),
        call("charters"),
        call("dxsbb_admission_results"),
    ]
    assert set(results) == {
        "schools",
        "majors",
        "school_majors",
        "score_segments",
        "major_strength_signals",
        "enrollment_plans",
        "major_admission_results",
        "charters",
        "dxsbb_admission_results",
        "gaokao_school_index",
    }


def test_liaoning_profile_can_reuse_catalog() -> None:
    orchestrator = MagicMock()
    orchestrator.run_single = AsyncMock(return_value={"new": 0, "updated": 0, "unchanged": 1, "failed": 0})

    with patch(
        "gaokao_vault.scheduler.reference_catalog.sync_gaokao_school_index",
        new=AsyncMock(return_value={"index_schools": 3000, "already_present": 3000, "added": 0}),
    ) as sync_index:
        results = asyncio.run(run_liaoning_profile(orchestrator, refresh_catalog=False))

    calls = [args.args[0] for args in orchestrator.run_single.await_args_list]
    sync_index.assert_not_awaited()
    assert "gaokao_school_index" not in results
    assert "schools" not in calls
    assert "majors" not in calls
    assert "school_majors" not in calls
    assert calls[0] == "score_segments"


def test_liaoning_profile_stops_after_failed_stage() -> None:
    orchestrator = MagicMock()

    async def run_single(task_type: str) -> dict[str, int]:
        return {"new": 0, "updated": 0, "unchanged": 0, "failed": int(task_type == "score_segments")}

    orchestrator.run_single = AsyncMock(side_effect=run_single)

    with pytest.raises(RuntimeError, match="score_segments"):
        asyncio.run(run_liaoning_profile(orchestrator, refresh_catalog=False))

    assert [args.args[0] for args in orchestrator.run_single.await_args_list] == [
        "score_segments",
        "major_strength_signals",
    ]
