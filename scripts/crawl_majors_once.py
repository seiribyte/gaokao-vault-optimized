"""One-shot full crawl of 专业知识库 major catalog via JSON API.

Bypasses scrapling Spider scheduling issues observed on Windows while still
reusing the project's DB upsert helpers and content-hash pipeline.
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
from gaokao_vault.db.queries.majors import (
    upsert_major,
    upsert_major_category,
    upsert_major_subcategory,
)
from gaokao_vault.pipeline.hasher import compute_content_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_majors_once")

BASE = "https://gaokao.chsi.com.cn/zyk/zybk"
CC_LEVELS = {
    "本科": "1050",
    "专科": "1060",
}


async def fetch_json(session: AsyncStealthySession, url: str, retries: int = 3) -> list[dict[str, Any]]:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = await session.fetch(url)
            raw = resp.body or b""
            if resp.status != 200 or not raw:
                raise RuntimeError(f"HTTP {resp.status} empty={not raw} url={url}")
            data = json.loads(raw)
            if not isinstance(data, dict) or not data.get("flag"):
                raise RuntimeError(f"bad payload flag={getattr(data, 'get', lambda *_: None)('flag')} url={url}")
            msg = data.get("msg")
            if not isinstance(msg, list):
                raise RuntimeError(f"msg not list url={url}")
            return msg
        except Exception as exc:
            last_err = exc
            logger.warning("fetch failed attempt=%s url=%s err=%s", attempt, url, exc)
            await asyncio.sleep(1.5 * attempt)
    raise RuntimeError(f"failed after {retries} retries: {url}") from last_err


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('majors', 'running', '{"mode":"full","runner":"crawl_majors_once"}'::jsonb)
        RETURNING id
        """
    )
    return int(row["id"])


async def finish_task(conn: asyncpg.Connection, task_id: int, stats: dict[str, int], error: str | None = None) -> None:
    status = "failed" if error else "success"
    await conn.execute(
        """
        UPDATE crawl_tasks
        SET status = $2,
            finished_at = NOW(),
            total_items = $3,
            new_items = $4,
            updated_items = $5,
            unchanged_items = $6,
            failed_items = $7,
            error_message = $8
        WHERE id = $1
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


async def persist_major(
    conn: asyncpg.Connection,
    item: dict[str, Any],
    task_id: int,
    stats: dict[str, int],
) -> None:
    content_hash = compute_content_hash(item)
    item = {**item, "content_hash": content_hash, "crawl_task_id": task_id}

    existing = await conn.fetchrow(
        "SELECT id, content_hash FROM majors WHERE code = $1 AND education_level = $2",
        item.get("code"),
        item["education_level"],
    )
    await upsert_major(conn, item)
    if existing is None:
        stats["new"] += 1
        change = "new"
    elif existing["content_hash"] == content_hash:
        stats["unchanged"] += 1
        change = "unchanged"
    else:
        stats["updated"] += 1
        change = "updated"

    await conn.execute(
        """
        INSERT INTO crawl_snapshots (entity_type, entity_id, content_hash, change_type, crawl_task_id, snapshot_data)
        VALUES (
            'majors',
            (SELECT id FROM majors WHERE code = $1 AND education_level = $2),
            $3,
            $4,
            $5,
            $6::jsonb
        )
        """,
        item.get("code"),
        item["education_level"],
        content_hash,
        change,
        task_id,
        json.dumps(item, ensure_ascii=False, default=str),
    )


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    task_id: int | None = None

    conn = await asyncpg.connect(dsn)
    try:
        task_id = await ensure_task(conn)
        logger.info("started crawl_tasks.id=%s dsn=%s", task_id, dsn.split("@")[-1])

        async with AsyncStealthySession(
            headless=True,
            google_search=False,
            network_idle=False,
            timeout=120000,
            wait=3000,
            extra_headers={"Referer": "https://gaokao.chsi.com.cn/"},
        ) as session:
            warm = await session.fetch(f"{BASE}/")
            logger.info("warmup status=%s", warm.status)

            for level_label, cc_key in CC_LEVELS.items():
                ml_items = await fetch_json(session, f"{BASE}/mlCategory/{cc_key}")
                logger.info("level=%s categories=%d", level_label, len(ml_items))

                for ml in ml_items:
                    ml_key = str(ml.get("key") or "")
                    ml_name = str(ml.get("name") or "")
                    if not ml_key or not ml_name:
                        continue

                    cat_id = await upsert_major_category(
                        conn,
                        {"name": ml_name, "education_level": level_label, "code": ml_key},
                    )

                    xk_items = await fetch_json(session, f"{BASE}/xkCategory/{ml_key}")
                    logger.info("  category=%s(%s) subcategories=%d", ml_name, ml_key, len(xk_items))

                    for xk in xk_items:
                        xk_key = str(xk.get("key") or "")
                        xk_name = str(xk.get("name") or "")
                        if not xk_key or not xk_name:
                            continue

                        sub_id = await upsert_major_subcategory(
                            conn,
                            {"category_id": cat_id, "name": xk_name, "code": xk_key},
                        )

                        try:
                            specs = await fetch_json(session, f"{BASE}/specialityesByCategory/{xk_key}")
                        except Exception:
                            logger.exception("specialty fetch failed subcategory=%s", xk_key)
                            stats["failed"] += 1
                            continue

                        for spec in specs:
                            name = str(spec.get("zymc") or "").strip()
                            code = str(spec.get("zydm") or "").strip()
                            source_id = str(spec.get("specId") or "").strip() or None
                            if not name:
                                continue
                            item = {
                                "source_id": source_id,
                                "category_id": cat_id,
                                "subcategory_id": sub_id,
                                "name": name,
                                "code": code or None,
                                "education_level": level_label,
                            }
                            try:
                                await persist_major(conn, item, task_id, stats)
                            except Exception:
                                logger.exception("persist failed major=%s code=%s", name, code)
                                stats["failed"] += 1

                        total = stats["new"] + stats["updated"] + stats["unchanged"]
                        if total and total % 100 == 0:
                            logger.info("progress stats=%s", stats)

                    logger.info(
                        "  done category=%s stats=%s",
                        ml_name,
                        stats,
                    )

        await finish_task(conn, task_id, stats)
        logger.info("DONE stats=%s", stats)

        row = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM major_categories) AS categories,
              (SELECT count(*) FROM major_subcategories) AS subcategories,
              (SELECT count(*) FROM majors) AS majors,
              (SELECT count(*) FROM majors WHERE education_level='本科') AS benke,
              (SELECT count(*) FROM majors WHERE education_level='专科') AS zhuanke
            """
        )
        logger.info("db totals: %s", dict(row))
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
