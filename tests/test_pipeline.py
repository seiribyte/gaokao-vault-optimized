"""Tests for the data pipeline: hasher, validator, and dedup logic."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest
from conftest import make_mock_pool_and_conn

from gaokao_vault.models.school import SchoolItem
from gaokao_vault.models.score import ScoreLineItem
from gaokao_vault.pipeline.dedup import (
    deduplicate_and_persist,
    deduplicate_and_persist_on_connection,
    deduplicate_score_segment_batch,
)
from gaokao_vault.pipeline.hasher import compute_content_hash
from gaokao_vault.pipeline.validator import validate_item


class TestContentHash:
    def test_deterministic(self):
        item = {"name": "Test School", "sch_id": 1, "city": "Beijing"}
        h1 = compute_content_hash(item)
        h2 = compute_content_hash(item)
        assert h1 == h2

    def test_excludes_meta_fields(self):
        item1 = {"name": "Test", "sch_id": 1}
        item2 = {"name": "Test", "sch_id": 1, "id": 99, "created_at": "2024-01-01", "content_hash": "abc"}
        assert compute_content_hash(item1) == compute_content_hash(item2)

    def test_different_content_different_hash(self):
        item1 = {"name": "School A", "sch_id": 1}
        item2 = {"name": "School B", "sch_id": 1}
        assert compute_content_hash(item1) != compute_content_hash(item2)

    def test_key_order_independent(self):
        item1 = {"name": "Test", "sch_id": 1, "city": "Beijing"}
        item2 = {"city": "Beijing", "sch_id": 1, "name": "Test"}
        assert compute_content_hash(item1) == compute_content_hash(item2)


class TestValidator:
    def test_valid_school_item(self):
        data = {"sch_id": 1, "name": "Test University"}
        result = validate_item(SchoolItem, data)
        assert result is not None
        assert result["sch_id"] == 1
        assert result["name"] == "Test University"
        assert result["is_211"] is False

    def test_invalid_school_item_missing_required(self):
        data = {"sch_id": 1}  # missing 'name'
        result = validate_item(SchoolItem, data)
        assert result is None

    def test_valid_score_line(self):
        data = {
            "province_id": 1,
            "year": 2024,
            "batch": "本科一批",
            "score": 530,
        }
        result = validate_item(ScoreLineItem, data)
        assert result is not None
        assert result["year"] == 2024

    def test_invalid_score_line_year(self):
        data = {
            "province_id": 1,
            "year": 1999,  # below 2000 minimum
            "batch": "本科一批",
        }
        result = validate_item(ScoreLineItem, data)
        assert result is None

    def test_invalid_item_log_redacts_raw_input_values(self, caplog):
        secret = "=".join(("token", "super" + "-secret-value"))
        with caplog.at_level(logging.WARNING, logger="gaokao_vault.pipeline.validator"):
            result = validate_item(ScoreLineItem, {"province_id": secret, "year": 2024, "batch": "本科一批"})

        assert result is None
        assert secret not in caplog.text
        assert "ScoreLineItem" in caplog.text
        assert "province_id" in caplog.text
        assert "int_parsing" in caplog.text

    def test_school_defaults(self):
        data = {"sch_id": 42, "name": "Test U"}
        result = validate_item(SchoolItem, data)
        assert result is not None
        assert result["is_985"] is False
        assert result["is_double_first"] is False
        assert result["province_id"] is None


class TestDedupPersistence:
    def test_persists_within_transaction(self):
        pool, conn, transaction_context = make_mock_pool_and_conn()
        conn.fetchrow.side_effect = [None, {"id": 1}]
        upsert_fn = AsyncMock(return_value=123)

        result = asyncio.run(
            deduplicate_and_persist(
                db_pool=pool,
                entity_type="schools",
                item={"sch_id": 1, "name": "Test"},
                content_hash="abc",
                unique_keys={"sch_id": 1},
                crawl_task_id=1,
                upsert_fn=upsert_fn,
            )
        )

        assert result == "new"
        conn.transaction.assert_called_once_with()
        transaction_context.__aenter__.assert_awaited_once_with()
        transaction_context.__aexit__.assert_awaited_once()

    def test_rejects_upsert_returning_invalid_entity_id(self):
        pool, conn, _ = make_mock_pool_and_conn()
        conn.fetchrow.return_value = None

        result = asyncio.run(
            deduplicate_and_persist(
                db_pool=pool,
                entity_type="schools",
                item={"sch_id": 1, "name": "Test"},
                content_hash="abc",
                unique_keys={"sch_id": 1},
                crawl_task_id=1,
                upsert_fn=AsyncMock(return_value=0),
            )
        )

        assert result == "failed"

    def test_connection_owned_dedup_rolls_back_when_snapshot_fails(self):
        _, conn, transaction_context = make_mock_pool_and_conn()

        with (
            patch("gaokao_vault.pipeline.dedup.find_latest_hash", new=AsyncMock(return_value=(None, None))),
            patch(
                "gaokao_vault.pipeline.dedup.insert_snapshot",
                new=AsyncMock(side_effect=RuntimeError("snapshot failed")),
            ),
            pytest.raises(RuntimeError, match="snapshot failed"),
        ):
            asyncio.run(
                deduplicate_and_persist_on_connection(
                    conn,
                    entity_type="schools",
                    item={"sch_id": 1, "name": "Test"},
                    content_hash="abc",
                    unique_keys={"sch_id": 1},
                    crawl_task_id=1,
                    upsert_fn=AsyncMock(return_value=123),
                )
            )

        transaction_context.__aexit__.assert_awaited_once()
        assert transaction_context.__aexit__.await_args.args[0] is RuntimeError

    def test_score_segment_batch_records_three_state_snapshots_in_one_transaction(self):
        _, conn, transaction_context = make_mock_pool_and_conn()
        rows = [
            {
                "province_id": 7,
                "year": 2025,
                "subject_category_id": None,
                "score": score,
                "segment_count": 1,
                "cumulative_count": 1,
                "content_hash": content_hash,
            }
            for score, content_hash in [(600, "new-hash"), (599, "same-hash"), (598, "changed-hash")]
        ]

        with (
            patch(
                "gaokao_vault.pipeline.dedup.find_latest_hash",
                new=AsyncMock(side_effect=[(None, None), (22, "same-hash"), (23, "old-hash")]),
            ),
            patch(
                "gaokao_vault.pipeline.dedup.upsert_score_segment",
                new=AsyncMock(side_effect=[21, 23]),
            ) as upsert,
            patch("gaokao_vault.pipeline.dedup.insert_snapshot", new=AsyncMock(return_value=1)) as snapshot,
        ):
            conn.fetchrow.return_value = {"id": 23, "content_hash": "old-hash", "score": 598}
            counts = asyncio.run(deduplicate_score_segment_batch(conn, rows, crawl_task_id=99))

        assert counts == {"new": 1, "updated": 1, "unchanged": 1, "failed": 0}
        assert upsert.await_count == 2
        assert [call.args[5] for call in snapshot.await_args_list] == ["new", "unchanged", "updated"]
        assert all(row["crawl_task_id"] == 99 for row in rows)
        transaction_context.__aenter__.assert_awaited_once_with()
        transaction_context.__aexit__.assert_awaited_once()
