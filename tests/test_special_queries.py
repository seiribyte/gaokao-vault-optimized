from __future__ import annotations

import asyncio
import json
from typing import Any, cast

from gaokao_vault.db.queries.special import upsert_special_enrollment


class _FakeConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args: tuple[object, ...] = ()

    async def fetchrow(self, query: str, *args: object) -> dict[str, int]:
        self.query = query
        self.args = args
        return {"id": 66}


def test_upsert_special_enrollment_preserves_strong_base_fields() -> None:
    conn = _FakeConnection()

    special_id = asyncio.run(
        upsert_special_enrollment(
            cast(Any, conn),
            {
                "enrollment_type": "强基计划",
                "year": 2025,
                "title": "测试大学2025年强基计划招生简章",
                "application_url": "https://bm.chsi.com.cn/jcxkzs/sch/10001",
                "registration_start": "2025-04-10",
                "registration_end": "2025-04-30",
                "selection_rule": "按高考成绩确定入围名单",
                "admission_rule": "按综合成绩择优录取",
                "eligible_majors": ["数学类", "物理学类"],
                "quality_flags": [],
            },
        )
    )

    assert special_id == 66
    assert "special_enrollments" in conn.query
    assert "application_url" in conn.query
    assert "eligible_majors" in conn.query
    assert "https://bm.chsi.com.cn/jcxkzs/sch/10001" in conn.args
    assert json.dumps(["数学类", "物理学类"], ensure_ascii=False) in conn.args
