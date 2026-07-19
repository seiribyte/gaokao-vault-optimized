from __future__ import annotations

import asyncio
import json
import re
from typing import Any, cast

from gaokao_vault.db.queries.enrollment import upsert_enrollment_plan, upsert_provincial_announcement


class _FakeConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args: tuple[object, ...] = ()

    async def fetchrow(self, query: str, *args: object) -> dict[str, int]:
        self.query = query
        self.args = args
        return {"id": 77}


def test_upsert_enrollment_plan_preserves_rule_and_quality_fields() -> None:
    conn = _FakeConnection()

    plan_id = asyncio.run(
        upsert_enrollment_plan(
            cast(Any, conn),
            {
                "school_id": 1,
                "school_code_raw": "0001",
                "province_id": 7,
                "year": 2025,
                "subject_category_id": 3,
                "batch": "本科批",
                "batch_code": "regular",
                "batch_category": "普通批",
                "batch_segment": None,
                "major_name": "计算机科学与技术",
                "program_type": None,
                "eligibility_requirements": None,
                "physical_exam_or_political_review": None,
                "political_review_requirement": None,
                "service_obligation": None,
                "major_group_code": "01",
                "major_code_raw": "080901",
                "campus": "主校区",
                "education_location": "长春市",
                "selection_requirement": "物理+化学",
                "physical_exam_limit": "不招色盲",
                "single_subject_limit": "英语单科不低于110分",
                "adjustment_rule": "服从专业调剂",
                "data_source": "gaokao.chsi.com.cn",
                "source_url": "https://gaokao.chsi.com.cn/test-plan",
                "quality_flags": [],
            },
        )
    )

    assert plan_id == 77
    assert "enrollment_plans" in conn.query
    assert "school_code_raw" in conn.query
    assert "0001" in conn.args
    assert "major_group_code" in conn.query
    assert "selection_requirement" in conn.query
    assert "batch_code" in conn.query
    assert "batch_category" in conn.query
    assert "program_type" in conn.query
    assert "source_url" in conn.query
    assert "physical_exam_or_political_review" in conn.query
    assert "political_review_requirement" in conn.query
    normalized_query = " ".join(conn.query.split())
    assert (
        "ON CONFLICT ( school_id, province_id, year, subject_category_id, batch, "
        "school_code_raw, major_group_code, major_code_raw, major_name )"
    ) in normalized_query
    assert "WITH updated AS" not in conn.query
    assert "WHERE NOT EXISTS (SELECT 1 FROM updated)" not in conn.query
    assert "regular" in conn.args
    assert "01" in conn.args
    assert "普通批" in conn.args
    assert "物理+化学" in conn.args
    assert "https://gaokao.chsi.com.cn/test-plan" in conn.args
    assert json.dumps([], ensure_ascii=False) in conn.args
    assert len(conn.args) == 34
    assert max(int(value) for value in re.findall(r"\$(\d+)", conn.query)) == len(conn.args)


def test_upsert_provincial_announcement_persists_official_source_fields() -> None:
    conn = _FakeConnection()

    announcement_id = asyncio.run(
        upsert_provincial_announcement(
            cast(Any, conn),
            {
                "province_id": 7,
                "year": 2025,
                "title": "吉林省2025年普通高校招生录取工作安排",
                "content": "普通高校招生录取工作安排正文。",
                "announcement_type": "admission",
                "publish_date": "2025-07-01",
                "source_url": "https://www.jleea.com.cn/2025/0701/notice.html",
                "content_hash": "hash",
                "crawl_task_id": 99,
            },
        )
    )

    assert announcement_id == 77
    assert "INSERT INTO provincial_announcements" in conn.query
    assert "ON CONFLICT (province_id, title, source_url) DO UPDATE SET" in conn.query
    assert "announcement_type=EXCLUDED.announcement_type" in conn.query
    assert conn.args == (
        7,
        2025,
        "吉林省2025年普通高校招生录取工作安排",
        "普通高校招生录取工作安排正文。",
        "admission",
        "2025-07-01",
        "https://www.jleea.com.cn/2025/0701/notice.html",
        "hash",
        99,
    )
