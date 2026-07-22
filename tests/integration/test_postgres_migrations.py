from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

from gaokao_vault.db.migrate import run_migrations


def test_postgres_schema_and_seed_replay_smoke() -> None:
    dsn = os.environ.get("GAOKAO_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("GAOKAO_TEST_POSTGRES_DSN is not configured")

    async def exercise() -> None:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        try:
            await run_migrations(pool)
            async with pool.acquire() as conn:
                table_count = await conn.fetchval(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
                )
                assert table_count == 24
                await conn.execute("UPDATE provinces SET code = 'broken' WHERE name = '北京'")

            await run_migrations(pool)
            async with pool.acquire() as conn:
                assert await conn.fetchval("SELECT code FROM provinces WHERE name = '北京'") == "11"
        finally:
            await pool.close()

    asyncio.run(exercise())
