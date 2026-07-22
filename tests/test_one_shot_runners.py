from __future__ import annotations

import asyncio
import runpy
from pathlib import Path
from unittest.mock import AsyncMock, patch

from conftest import make_mock_pool_and_conn


def test_all_one_shot_entity_writes_use_canonical_pipeline() -> None:
    scripts_dir = Path("scripts")
    runner_files = sorted(scripts_dir.glob("crawl_*_once.py"))

    assert len(runner_files) == 7
    for path in runner_files:
        source = path.read_text(encoding="utf-8")
        assert "deduplicate_and_persist_on_connection" in source
        assert "validate_item" in source


def test_all_one_shot_runners_import_without_starting_network_work() -> None:
    for path in sorted(Path("scripts").glob("crawl_*_once.py")):
        namespace = runpy.run_path(str(path))
        assert callable(namespace["main"])


def test_enrollment_runner_passes_full_canonical_identity() -> None:
    _, conn, _ = make_mock_pool_and_conn()
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    item = {
        "school_id": 1,
        "province_id": 2,
        "year": 2026,
        "subject_category_id": None,
        "batch": "本科",
        "school_code_raw": "S1",
        "major_group_code": "G1",
        "major_code_raw": "M1",
        "major_name": "数学",
    }
    runner_globals = runpy.run_path("scripts/crawl_enrollment_plans_once.py")
    persist_plan = runner_globals["persist_plan"]
    persist = AsyncMock(return_value="updated")

    with patch.dict(persist_plan.__globals__, {"deduplicate_and_persist_on_connection": persist}):
        asyncio.run(persist_plan(conn, item, task_id=9, stats=stats))

    assert stats["updated"] == 1
    unique_keys = persist.await_args.kwargs["unique_keys"]
    assert unique_keys == {
        "school_id": 1,
        "province_id": 2,
        "year": 2026,
        "subject_category_id": None,
        "batch": "本科",
        "school_code_raw": "S1",
        "major_group_code": "G1",
        "major_code_raw": "M1",
        "major_name": "数学",
    }


def test_resume_guards_are_removed_from_full_one_shot_scans() -> None:
    assert "SCH_ID_END = 5000" in Path("scripts/crawl_schools_once.py").read_text(encoding="utf-8")
    for name in ("crawl_charters_once.py", "crawl_enrollment_plans_once.py", "crawl_school_majors_once.py"):
        source = Path("scripts", name).read_text(encoding="utf-8")
        assert "NOT EXISTS" not in source
