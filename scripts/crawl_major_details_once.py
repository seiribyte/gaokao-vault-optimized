"""Backfill major detail fields from 专业知识库 specialityDetail API.

Fills majors.description / graduate_directions (and degree when present)
for every major that already has a source_id.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any

import asyncpg
from scrapling.fetchers import AsyncStealthySession

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.majors import upsert_major
from gaokao_vault.pipeline.hasher import compute_content_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_major_details_once")

BASE = "https://gaokao.chsi.com.cn/zyk/zybk"


async def fetch_detail(session: AsyncStealthySession, source_id: str, retries: int = 3) -> dict[str, Any] | None:
    url = f"{BASE}/specialityDetail/{source_id}"
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = await session.fetch(url)
            raw = resp.body or b""
            if resp.status != 200 or not raw:
                raise RuntimeError(f"HTTP {resp.status} empty={not raw}")
            data = json.loads(raw)
            msg = data.get("msg") if isinstance(data, dict) else None
            if not isinstance(msg, dict):
                raise RuntimeError("msg is not dict")
            return msg
        except Exception as exc:
            last_err = exc
            logger.warning("detail failed attempt=%s source_id=%s err=%s", attempt, source_id, exc)
            await asyncio.sleep(1.2 * attempt)
    logger.error("detail permanently failed source_id=%s err=%s", source_id, last_err)
    return None


def _extract_fields(msg: dict[str, Any]) -> dict[str, str | None]:
    zyjs = msg.get("zyjs") or {}
    description = None
    if isinstance(zyjs, dict):
        description = (zyjs.get("desc") or zyjs.get("versionDesc") or None)
        if isinstance(description, str):
            description = description.strip() or None

    directions: list[str] = []
    jyfx = msg.get("jyfxInfo") or {}
    if isinstance(jyfx, dict):
        for item in jyfx.get("jyfxList") or []:
            if isinstance(item, dict):
                name = str(item.get("jyfx") or "").strip()
                if name:
                    directions.append(name)
    graduate_directions = "、".join(directions) if directions else None

    degree = None
    xlcc = str(msg.get("xlcc") or "").strip()
    if xlcc:
        degree = xlcc

    return {
        "description": description,
        "graduate_directions": graduate_directions,
        "degree": degree,
    }


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('majors', 'running', '{"mode":"full","runner":"crawl_major_details_once"}'::jsonb)
        RETURNING id
        """
    )
    return int(row["id"])


async def finish_task(conn: asyncpg.Connection, task_id: int, stats: dict[str, int], error: str | None = None) -> None:
    status = "failed" if error else "success"
    await conn.execute(
        """
        UPDATE crawl_tasks
        SET status=$2, finished_at=NOW(), total_items=$3, new_items=$4,
            updated_items=$5, unchanged_items=$6, failed_items=$7, error_message=$8
        WHERE id=$1
        """,
        task_id,
        status,
        stats["new"] + stats["updated"] + stats["unchanged"] + stats["failed"],
        stats["new"],
        stats["updated"],
        stats["unchanged"],
        stats["failed"],
        error,
    )


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    try:
        task_id = await ensure_task(conn)
        rows = await conn.fetch(
            """
            SELECT id, source_id, category_id, subcategory_id, code, name, education_level,
                   duration, degree, description, employment_rate, graduate_directions, content_hash
            FROM majors
            WHERE source_id IS NOT NULL AND source_id <> ''
            ORDER BY id
            """
        )
        logger.info("task_id=%s majors_with_source_id=%d", task_id, len(rows))

        async with AsyncStealthySession(
            headless=True,
            google_search=False,
            network_idle=False,
            timeout=120000,
            wait=2500,
            extra_headers={"Referer": "https://gaokao.chsi.com.cn/"},
        ) as session:
            warm = await session.fetch(f"{BASE}/")
            logger.info("warmup status=%s", warm.status)

            for idx, row in enumerate(rows, start=1):
                msg = await fetch_detail(session, row["source_id"])
                if msg is None:
                    stats["failed"] += 1
                    continue

                extracted = _extract_fields(msg)
                item = {
                    "source_id": row["source_id"],
                    "category_id": row["category_id"],
                    "subcategory_id": row["subcategory_id"],
                    "code": row["code"],
                    "name": row["name"],
                    "education_level": row["education_level"],
                    "duration": row["duration"],
                    "degree": extracted["degree"] or row["degree"],
                    "description": extracted["description"] or row["description"],
                    "employment_rate": row["employment_rate"],
                    "graduate_directions": extracted["graduate_directions"] or row["graduate_directions"],
                }
                content_hash = compute_content_hash(item)
                item["content_hash"] = content_hash
                item["crawl_task_id"] = task_id

                if row["content_hash"] == content_hash and row["description"] and row["graduate_directions"]:
                    stats["unchanged"] += 1
                else:
                    await upsert_major(conn, item)
                    if row["content_hash"] is None:
                        stats["new"] += 1
                        change = "new"
                    else:
                        stats["updated"] += 1
                        change = "updated"
                    await conn.execute(
                        """
                        INSERT INTO crawl_snapshots
                            (entity_type, entity_id, content_hash, change_type, crawl_task_id, snapshot_data)
                        VALUES ('majors', $1, $2, $3, $4, $5::jsonb)
                        """,
                        row["id"],
                        content_hash,
                        change,
                        task_id,
                        json.dumps(item, ensure_ascii=False, default=str),
                    )

                if idx % 25 == 0 or idx == len(rows):
                    logger.info("progress %d/%d stats=%s", idx, len(rows), stats)

        await finish_task(conn, task_id, stats)
        filled = await conn.fetchrow(
            """
            SELECT
              count(*) FILTER (WHERE description IS NOT NULL AND description <> '') AS with_desc,
              count(*) FILTER (WHERE graduate_directions IS NOT NULL AND graduate_directions <> '') AS with_dirs,
              count(*) FILTER (WHERE degree IS NOT NULL AND degree <> '') AS with_degree,
              count(*) AS total
            FROM majors
            """
        )
        logger.info("DONE stats=%s filled=%s", stats, dict(filled))
        return 0 if stats["failed"] == 0 else 2
    except Exception as exc:
        logger.exception("fatal")
        if task_id is not None:
            await finish_task(conn, task_id, stats, error=str(exc))
        return 1
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
