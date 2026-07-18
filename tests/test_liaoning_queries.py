from __future__ import annotations

import asyncio
from typing import Any, cast

from gaokao_vault.db.queries.liaoning import fetch_liaoning_historical_plans


class _FakeConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args: tuple[object, ...] = ()

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.query = query
        self.args = args
        return []


def test_historical_plan_query_keeps_raw_codes_for_variant_matching() -> None:
    conn = _FakeConnection()

    rows = asyncio.run(
        fetch_liaoning_historical_plans(
            cast(Any, conn),
            years=(2025, 2024, 2023, 2022),
            subject="物理",
        )
    )

    assert rows == []
    assert "ep.id AS historical_plan_id" in conn.query
    assert "ep.school_code_raw" in conn.query
    assert "ep.major_group_code" in conn.query
    assert "ep.major_code_raw" in conn.query
    assert conn.args == ("辽宁", [2025, 2024, 2023, 2022], "物理")
