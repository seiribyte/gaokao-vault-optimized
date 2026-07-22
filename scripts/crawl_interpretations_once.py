"""Crawl major interpretation articles via JSON list API + detail pages."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from typing import Any

import asyncpg
from scrapling.fetchers import AsyncStealthySession

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.majors import upsert_major_interpretation
from gaokao_vault.models.major import MajorInterpretationItem
from gaokao_vault.pipeline.dedup import deduplicate_and_persist_on_connection
from gaokao_vault.pipeline.hasher import compute_content_hash
from gaokao_vault.pipeline.validator import validate_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("crawl_interpretations_once")

BASE = "https://gaokao.chsi.com.cn"
LIST_URL = f"{BASE}/zyk/zybk/zyjd/listInfo"
DETAIL_URL = f"{BASE}/zyk/zybk/zyjd/viewPage/{{zyjd_id}}"
# listInfo returns 9 items per page (totalCount=161, pageCount=9).
PAGE_SIZE = 9


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_content(html: str, fallback: str | None = None) -> str:
    patterns = [
        r'class="text-content UEditor"([\s\S]*?)</div>\s*<div',
        r'class="text-content UEditor"([\s\S]*?)</div>',
        r'class="content"([\s\S]*?)</div>',
    ]
    for pat in patterns:
        m = re.search(pat, html)
        if not m:
            continue
        text = _clean(re.sub(r"<[^>]+>", " ", m.group(1)))
        # drop short nav-like junk / section headers
        if len(text) >= 40:
            return text[:10000]
    # list API already returns a short summary; use it when detail body is thin
    summary = _clean(fallback)
    return summary[:10000] if summary else ""


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('interpretations', 'running', '{"mode":"full","runner":"crawl_interpretations_once"}'::jsonb)
        RETURNING id
        """
    )
    return int(row["id"])


async def finish_task(conn: asyncpg.Connection, task_id: int, stats: dict[str, int], error: str | None = None) -> None:
    status = "failed" if error else "success"
    total = stats["new"] + stats["updated"] + stats["unchanged"] + stats["failed"]
    await conn.execute(
        """
        UPDATE crawl_tasks
        SET status=$2, finished_at=NOW(), total_items=$3, new_items=$4,
            updated_items=$5, unchanged_items=$6, failed_items=$7, error_message=$8
        WHERE id=$1
        """,
        task_id,
        status,
        total,
        stats["new"],
        stats["updated"],
        stats["unchanged"],
        stats["failed"],
        error,
    )


async def fetch_json(session: AsyncStealthySession, url: str) -> dict[str, Any]:
    resp = await session.fetch(url)
    raw = resp.body or b""
    if resp.status != 200 or not raw:
        raise RuntimeError(f"HTTP {resp.status} empty={not raw} url={url}")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError(f"non-dict json url={url}")
    return data


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    try:
        task_id = await ensure_task(conn)
        logger.info("task_id=%s", task_id)

        async with AsyncStealthySession(
            headless=True,
            google_search=False,
            network_idle=False,
            timeout=120000,
            wait=2000,
            extra_headers={"Referer": f"{BASE}/"},
        ) as session:
            await session.fetch(f"{BASE}/zyk/zybk/zyjd/listPage")
            first = await fetch_json(session, f"{LIST_URL}?start=0")
            msg = first.get("msg") or {}
            total = int(msg.get("totalCount") or 0)
            logger.info("total interpretations=%s", total)

            seen_ids: set[str] = set()
            start = 0
            while start < max(total, 1):
                payload = first if start == 0 else await fetch_json(session, f"{LIST_URL}?start={start}")
                items = (payload.get("msg") or {}).get("list") or []
                if not items:
                    break
                for item in items:
                    zyjd_id = str(item.get("zyjdId") or "").strip()
                    title = _clean(item.get("title"))
                    major_name = _clean(item.get("zymc"))
                    if not zyjd_id or not title or zyjd_id in seen_ids:
                        continue
                    seen_ids.add(zyjd_id)

                    source_url = DETAIL_URL.format(zyjd_id=zyjd_id)
                    try:
                        detail = await session.fetch(source_url)
                        html = (detail.body or b"").decode("utf-8", errors="replace")
                        content = _extract_content(html, fallback=item.get("content"))
                    except Exception:
                        logger.exception("detail failed zyjd_id=%s", zyjd_id)
                        # fall back to list summary
                        content = _clean(item.get("content"))

                    if not content:
                        stats["failed"] += 1
                        continue

                    major_id = None
                    if major_name:
                        row = await conn.fetchrow("SELECT id FROM majors WHERE name=$1 ORDER BY id LIMIT 1", major_name)
                        if row:
                            major_id = row["id"]

                    data = {
                        "major_id": major_id,
                        "title": title[:200],
                        "content": content,
                        "author": None,
                        "publish_date": None,
                        "source_url": source_url[:255],
                    }
                    validated = validate_item(MajorInterpretationItem, data)
                    if validated is None:
                        stats["failed"] += 1
                        continue
                    change = await deduplicate_and_persist_on_connection(
                        conn,
                        entity_type="major_interpretations",
                        item=validated,
                        content_hash=compute_content_hash(validated),
                        unique_keys={"major_id": validated.get("major_id"), "title": validated.get("title")},
                        crawl_task_id=task_id,
                        upsert_fn=upsert_major_interpretation,
                    )
                    stats[change] += 1

                logger.info("progress start=%s seen=%s stats=%s", start, len(seen_ids), stats)
                start += PAGE_SIZE
                if len(items) < 1:
                    break

        await finish_task(conn, task_id, stats)
        total_db = await conn.fetchval("SELECT count(*) FROM major_interpretations")
        logger.info("DONE stats=%s interpretations=%s", stats, total_db)
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
