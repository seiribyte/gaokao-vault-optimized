from __future__ import annotations

import asyncio

from conftest import make_mock_pool_and_conn

from gaokao_vault.db.migrate import run_migrations


def test_run_migrations_applies_schema_then_reference_seeds() -> None:
    pool, conn, _ = make_mock_pool_and_conn()

    asyncio.run(run_migrations(pool))

    assert conn.execute.await_count == 3
    schema_sql, province_sql, category_sql = (call.args[0] for call in conn.execute.await_args_list)
    assert "CREATE TABLE IF NOT EXISTS provinces" in schema_sql
    assert "INSERT INTO provinces" in province_sql
    assert "ON CONFLICT (name) DO UPDATE" in province_sql
    assert "INSERT INTO subject_categories" in category_sql
