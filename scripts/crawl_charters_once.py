"""Crawl admission charters from gaokao.chsi.com.cn per-school list pages.

Bypasses scrapling spider checkpoints. Uses sch_id from schools table.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any
from urllib.request import Request, urlopen

import asyncpg

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.enrollment import upsert_charter
from gaokao_vault.pipeline.hasher import compute_content_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("crawl_charters_once")

BASE = "https://gaokao.chsi.com.cn"
LIST_URL = f"{BASE}/zsgs/zhangcheng/listZszc--schId-{{sch_id}}.dhtml"
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": f"{BASE}/zsgs/zhangcheng/",
}
REQUEST_SLEEP_SEC = 0.4
MAX_RETRIES = 3
ITEM_RE = re.compile(
    r"href='(/zsgs/zhangcheng/listVerifedZszc--infoId-(\d+),method-view,schId-(\d+)\.dhtml)'"
    r"[^>]*>([^<]+)</a>\s*<div class=\"zszc-zc-time\">\s*([^<]+)",
    re.S,
)
YEAR_RE = re.compile(r"(20\d{2})")
CONTENT_PATTERNS = [
    r'class="content zszc-content UEditor"([\s\S]*?)</div>\s*<div',
    r'class="content zszc-content UEditor"([\s\S]*?)</div>',
    r'class="[^"]*zszc-content UEditor[^"]*"([\s\S]*?)</div>',
]


def _clean(text: str | None) -> str:
    if not text:
        return ""
    text = text.replace("&nbsp;", " ").replace("&ensp;", " ").replace("&emsp;", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def http_get(url: str) -> str | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_SLEEP_SEC)
            req = Request(url, headers=HTTP_HEADERS)
            with urlopen(req, timeout=40) as resp:  # noqa: S310 — fixed CHSI host
                if resp.status != 200:
                    time.sleep(attempt)
                    continue
                raw = resp.read()
                return raw.decode("utf-8", errors="ignore")
        except Exception as exc:
            logger.debug("http_get failed url=%s err=%s", url, exc)
            time.sleep(attempt)
    return None


def parse_list(html: str, sch_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page_year = None
    ym = re.search(r"(20\d{2})年招生章程", html)
    if ym:
        page_year = int(ym.group(1))

    for href, info_id, sch, title, pub in ITEM_RE.findall(html):
        title = _clean(title)
        pub = _clean(pub)
        year = page_year
        for blob in (title, pub):
            m = YEAR_RE.search(blob or "")
            if m:
                year = int(m.group(1))
                break
        if year is None:
            year = datetime.now().year
        publish_date: date | None = None
        dm = re.match(r"(\d{4})-(\d{2})-(\d{2})", pub or "")
        if dm:
            try:
                publish_date = date(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)))
                if page_year is None:
                    year = publish_date.year
            except ValueError:
                publish_date = None
        items.append(
            {
                "info_id": info_id,
                "sch_id": int(sch) if sch.isdigit() else sch_id,
                "title": title[:200] if title else None,
                "year": year,
                "publish_date": publish_date,
                "source_url": BASE + href,
            }
        )
    return items


def extract_content(html: str) -> str:
    for pat in CONTENT_PATTERNS:
        m = re.search(pat, html)
        if not m:
            continue
        text = _clean(m.group(1))
        if len(text) >= 80:
            return text[:50000]
    return ""


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('charters', 'running', '{"mode":"full","runner":"crawl_charters_once"}'::jsonb)
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


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0, "empty_list": 0, "empty_content": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        task_id = await ensure_task(conn)
        # Prefer schools that still lack any charter.
        schools = await conn.fetch(
            """
            SELECT s.id, s.sch_id, s.name
            FROM schools s
            WHERE s.sch_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM admission_charters c WHERE c.school_id = s.id)
            ORDER BY s.id
            """
        )
        already = await conn.fetchval("SELECT count(*) FROM admission_charters")
        logger.info("task_id=%s remaining_schools=%s already=%s", task_id, len(schools), already)

        loop = asyncio.get_running_loop()
        smoke_html = await loop.run_in_executor(pool, http_get, LIST_URL.format(sch_id=1))
        smoke_items = parse_list(smoke_html or "", 1)
        logger.info("smoke sch_id=1 items=%s", len(smoke_items))
        if not smoke_items:
            raise RuntimeError("smoke charter list returned 0 items — aborting")
        smoke_detail = await loop.run_in_executor(pool, http_get, smoke_items[0]["source_url"])
        smoke_content = extract_content(smoke_detail or "")
        logger.info("smoke detail content_len=%s", len(smoke_content))
        if len(smoke_content) < 80:
            raise RuntimeError("smoke charter detail content too short — aborting")

        for i, school in enumerate(schools, start=1):
            sch_id = int(school["sch_id"])
            list_html = await loop.run_in_executor(pool, http_get, LIST_URL.format(sch_id=sch_id))
            if list_html is None:
                stats["failed"] += 1
                continue
            items = parse_list(list_html, sch_id)
            if not items:
                stats["empty_list"] += 1
                continue

            # One row per year: prefer titles containing 本科, then longest title.
            by_year: dict[int, dict[str, Any]] = {}
            for item in items:
                year = int(item["year"])
                prev = by_year.get(year)
                if prev is None:
                    by_year[year] = item
                    continue
                score = (1 if "本科" in (item.get("title") or "") else 0, len(item.get("title") or ""))
                prev_score = (1 if "本科" in (prev.get("title") or "") else 0, len(prev.get("title") or ""))
                if score > prev_score:
                    by_year[year] = item

            for year, item in sorted(by_year.items()):
                detail_html = await loop.run_in_executor(pool, http_get, item["source_url"])
                content = extract_content(detail_html or "")
                if len(content) < 80:
                    stats["empty_content"] += 1
                    continue
                data = {
                    "school_id": int(school["id"]),
                    "year": year,
                    "title": item.get("title"),
                    "content": content,
                    "publish_date": item.get("publish_date"),
                    "source_url": item["source_url"][:255],
                    "content_hash": compute_content_hash(
                        {
                            "school_id": int(school["id"]),
                            "year": year,
                            "title": item.get("title"),
                            "content": content,
                        }
                    ),
                    "crawl_task_id": task_id,
                }
                try:
                    before = await conn.fetchval(
                        "SELECT content_hash FROM admission_charters WHERE school_id=$1 AND year=$2",
                        data["school_id"],
                        data["year"],
                    )
                    await upsert_charter(conn, data)
                    if before is None:
                        stats["new"] += 1
                    elif before == data["content_hash"]:
                        stats["unchanged"] += 1
                    else:
                        stats["updated"] += 1
                except Exception:
                    logger.exception(
                        "persist failed school=%s year=%s title=%s",
                        school["id"],
                        year,
                        item.get("title"),
                    )
                    stats["failed"] += 1

            if i % 10 == 0 or i == len(schools):
                total_now = await conn.fetchval("SELECT count(*) FROM admission_charters")
                schools_now = await conn.fetchval("SELECT count(DISTINCT school_id) FROM admission_charters")
                logger.info(
                    "progress %s/%s stats=%s db_rows=%s db_schools=%s",
                    i,
                    len(schools),
                    stats,
                    total_now,
                    schools_now,
                )

        await finish_task(conn, task_id, stats)
        total = await conn.fetchval("SELECT count(*) FROM admission_charters")
        schools_with = await conn.fetchval("SELECT count(DISTINCT school_id) FROM admission_charters")
        logger.info("DONE stats=%s charters=%s schools_with=%s", stats, total, schools_with)
        return 0
    except Exception as exc:
        logger.exception("crawl_charters_once failed")
        if task_id is not None:
            await finish_task(conn, task_id, stats, error=str(exc)[:500])
        return 1
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
