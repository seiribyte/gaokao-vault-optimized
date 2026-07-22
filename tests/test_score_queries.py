from __future__ import annotations

import asyncio
from typing import Any, cast

from gaokao_vault.db.queries.scores import find_score_segment_rank, upsert_score_segment


class _FakeConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args: tuple[object, ...] = ()
        self.row: dict[str, object] | None = None

    async def fetchrow(self, query: str, *args: object):
        self.query = query
        self.args = args
        return self.row


def test_find_score_segment_rank_uses_score_floor_lookup():
    conn = _FakeConnection()
    conn.row = {"score": 500, "cumulative_count": 23145}

    row = asyncio.run(find_score_segment_rank(cast(Any, conn), 7, 2025, 3, 500))

    assert row == {"score": 500, "cumulative_count": 23145}
    assert "score <= $4" in conn.query
    assert "ORDER BY score DESC" in conn.query
    assert conn.args == (7, 2025, 3, 500)


def test_upsert_score_segment_uses_null_safe_identity_conflict() -> None:
    conn = _FakeConnection()
    conn.row = {"id": 41}

    entity_id = asyncio.run(
        upsert_score_segment(
            cast(Any, conn),
            {
                "province_id": 7,
                "year": 2025,
                "subject_category_id": None,
                "score": 600,
                "segment_count": 10,
                "cumulative_count": 100,
                "content_hash": "hash",
                "crawl_task_id": 9,
            },
        )
    )

    assert entity_id == 41
    assert "ON CONFLICT (province_id, year, subject_category_id, score)" in conn.query
    assert "RETURNING id" in conn.query
    assert conn.args[2] is None
