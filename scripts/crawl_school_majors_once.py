"""Crawl school-major associations via gaokao.cn static pc_special.json.

Avoids scrapling spider checkpoints on Windows. Maps schools by name index
and majors by exact name / code / code-prefix.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import asyncpg
from scrapling.fetchers import Fetcher

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.majors import upsert_school_major
from gaokao_vault.pipeline.hasher import compute_content_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("crawl_school_majors_once")

SCHOOL_NAME_INDEX_URL = "https://static-data.gaokao.cn/www/2.0/school/name.json"
PC_SPECIAL_URL = "https://static-data.gaokao.cn/www/2.0/school/{school_id}/pc_special.json"
DATA_SOURCE = "gaokao.cn"
HTTP_HEADERS = {
    "Referer": "https://www.gaokao.cn/",
    "Origin": "https://www.gaokao.cn",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}
HTTP_WORKERS = 2
REQUEST_SLEEP_SEC = 0.15
MAX_RETRIES = 4


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _norm_name(name: str) -> str:
    return re.sub(r"\s+", "", name).replace("（", "(").replace("）", ")")


def http_get_json(url: str) -> dict[str, Any] | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_SLEEP_SEC)
            resp = Fetcher.get(url, headers=HTTP_HEADERS, timeout=30)
            raw = resp.body or b""
            status = getattr(resp, "status", 0)
            if status == 404:
                return None
            if status != 200 or not raw:
                time.sleep(attempt)
                continue
            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="ignore")
            data = json.loads(text)
            if not isinstance(data, dict):
                return None
            if str(data.get("code")) == "1069":
                logger.warning("rate limited attempt=%s url=%s", attempt, url)
                time.sleep(10 * attempt)
                continue
            return data
        except Exception as exc:
            logger.debug("http_get_json failed url=%s err=%s", url, exc)
            time.sleep(attempt)
    return None


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('school_majors', 'running', '{"mode":"full","runner":"crawl_school_majors_once"}'::jsonb)
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


def build_school_index(rows: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in rows:
        school_id = _safe_text(row.get("school_id"))
        if not school_id:
            continue
        for key in ("name", "old_name"):
            raw = _safe_text(row.get(key)) or ""
            for part in re.split(r"[,，]", raw):
                name = _norm_name(part)
                if name and name not in index:
                    index[name] = school_id
    return index


def iter_specials(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(item: dict[str, Any]) -> None:
        sid = _safe_text(item.get("special_id")) or _safe_text(item.get("id"))
        name = _safe_text(item.get("special_name")) or _safe_text(item.get("name"))
        key = sid or name or ""
        if not name or key in seen:
            return
        seen.add(key)
        out.append(item)

    special = data.get("special")
    if isinstance(special, list):
        for group in special:
            if not isinstance(group, dict):
                continue
            children = group.get("special")
            if isinstance(children, list):
                for child in children:
                    if isinstance(child, dict):
                        add(child)

    # featured / ranked buckets also carry strength fields
    for key in ("1", "2", "3", "4", "nation_feature"):
        bucket = data.get(key)
        if isinstance(bucket, list):
            for item in bucket:
                if isinstance(item, dict):
                    add(item)

    detail = data.get("special_detail")
    if isinstance(detail, dict):
        for bucket in detail.values():
            if isinstance(bucket, list):
                for item in bucket:
                    if isinstance(item, dict):
                        add(item)

    return out


def build_strength(item: dict[str, Any]) -> dict[str, Any]:
    # API uses "1" for yes on feature flags; "2" commonly means no.
    featured = any(str(item.get(k)).strip() == "1" for k in ("nation_feature", "nation_first_class", "is_important", "province_feature"))

    evidence: list[dict[str, Any]] = []
    for field, label in (
        ("nation_feature", "nation_feature"),
        ("nation_first_class", "nation_first_class"),
        ("province_feature", "province_feature"),
        ("is_important", "is_important"),
        ("xueke_rank", "xueke_rank"),
        ("xueke_rank_score", "xueke_rank_score"),
        ("ruanke_rank", "ruanke_rank"),
        ("ruanke_level", "ruanke_level"),
    ):
        val = item.get(field)
        if val is not None and str(val).strip() not in {"", "0", "2", "null"}:
            evidence.append({"type": label, "value": val, "source": DATA_SOURCE})
    return {
        "is_featured_major": featured,
        "major_strength_rank": _safe_int(item.get("xueke_rank")) or _safe_int(item.get("ruanke_rank")),
        "major_strength_score": None,
        "major_strength_tier": _safe_text(item.get("xueke_rank_score")) or _safe_text(item.get("ruanke_level")),
        "strength_evidence": evidence,
    }


class MajorResolver:
    def __init__(self, rows: list[asyncpg.Record]):
        self.by_name: dict[str, list[int]] = {}
        self.by_code: dict[str, int] = {}
        self.codes: list[str] = []
        for row in rows:
            mid = int(row["id"])
            name = _safe_text(row["name"])
            code = _safe_text(row["code"])
            if name:
                self.by_name.setdefault(name, []).append(mid)
            if code:
                self.by_code[code] = mid
                self.codes.append(code)
        # longest codes first for prefix match
        self.codes.sort(key=len, reverse=True)

    def resolve(self, name: str | None, code: str | None) -> int | None:
        if name:
            ids = self.by_name.get(name)
            if ids and len(ids) == 1:
                return ids[0]
            # strip common suffixes
            bare = re.split(r"[（(]", name, maxsplit=1)[0].strip()
            if bare and bare != name:
                ids = self.by_name.get(bare)
                if ids and len(ids) == 1:
                    return ids[0]
        if code:
            if code in self.by_code:
                return self.by_code[code]
            # pc_special often uses 4-digit codes; majors use 6-digit
            matches = [c for c in self.codes if c.startswith(code)]
            if len(matches) == 1:
                return self.by_code[matches[0]]
        return None


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0, "unmatched_major": 0, "no_index": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    pool = ThreadPoolExecutor(max_workers=HTTP_WORKERS)
    try:
        task_id = await ensure_task(conn)
        schools = await conn.fetch(
            """
            SELECT s.id, s.name
            FROM schools s
            WHERE NOT EXISTS (SELECT 1 FROM school_majors sm WHERE sm.school_id = s.id)
            ORDER BY s.id
            """
        )
        already = await conn.fetchval("SELECT count(DISTINCT school_id) FROM school_majors")
        major_rows = await conn.fetch("SELECT id, name, code FROM majors")
        resolver = MajorResolver(major_rows)
        logger.info(
            "task_id=%s remaining_schools=%s already_have=%s majors=%s",
            task_id,
            len(schools),
            already,
            len(major_rows),
        )

        loop = asyncio.get_running_loop()
        idx_payload = await loop.run_in_executor(pool, http_get_json, SCHOOL_NAME_INDEX_URL)
        if not idx_payload or str(idx_payload.get("code")) not in {"0000", "0"}:
            raise RuntimeError(f"failed to load school name index: {idx_payload}")
        school_index = build_school_index(idx_payload.get("data") or [])
        logger.info("school index size=%s", len(school_index))

        # smoke
        smoke = await loop.run_in_executor(pool, http_get_json, PC_SPECIAL_URL.format(school_id="31"))
        smoke_items = iter_specials(smoke or {})
        logger.info("smoke school_id=31 specials=%s", len(smoke_items))
        if not smoke_items:
            raise RuntimeError("smoke pc_special returned 0 specials — aborting")

        matched_schools = 0
        for i, school in enumerate(schools, start=1):
            name = str(school["name"])
            gaokao_id = school_index.get(_norm_name(name))
            if not gaokao_id:
                stats["no_index"] += 1
                continue
            matched_schools += 1
            url = PC_SPECIAL_URL.format(school_id=gaokao_id)
            payload = await loop.run_in_executor(pool, http_get_json, url)
            if not payload:
                stats["failed"] += 1
                continue
            specials = iter_specials(payload)
            for order, item in enumerate(specials, start=1):
                major_name = _safe_text(item.get("special_name")) or _safe_text(item.get("name"))
                code = _safe_text(item.get("code"))
                major_id = resolver.resolve(major_name, code)
                if major_id is None:
                    stats["unmatched_major"] += 1
                    continue
                strength = build_strength(item)
                data = {
                    "school_id": int(school["id"]),
                    "major_id": major_id,
                    "school_major_display_order": order,
                    **strength,
                    "content_hash": compute_content_hash(
                        {
                            "school_id": int(school["id"]),
                            "major_id": major_id,
                            "special_id": item.get("special_id"),
                            "code": code,
                            "featured": strength["is_featured_major"],
                            "tier": strength["major_strength_tier"],
                        }
                    ),
                    "crawl_task_id": task_id,
                }
                try:
                    before = await conn.fetchval(
                        "SELECT content_hash FROM school_majors WHERE school_id=$1 AND major_id=$2",
                        data["school_id"],
                        data["major_id"],
                    )
                    await upsert_school_major(conn, data)
                    if before is None:
                        stats["new"] += 1
                    elif before == data["content_hash"]:
                        stats["unchanged"] += 1
                    else:
                        stats["updated"] += 1
                except Exception:
                    logger.exception(
                        "persist failed school=%s major=%s code=%s",
                        school["id"],
                        major_name,
                        code,
                    )
                    stats["failed"] += 1

            if i % 20 == 0 or i == len(schools):
                total_now = await conn.fetchval("SELECT count(*) FROM school_majors")
                schools_now = await conn.fetchval("SELECT count(DISTINCT school_id) FROM school_majors")
                logger.info(
                    "progress %s/%s matched_schools=%s stats=%s db_rows=%s db_schools=%s",
                    i,
                    len(schools),
                    matched_schools,
                    stats,
                    total_now,
                    schools_now,
                )

        await finish_task(conn, task_id, stats)
        total = await conn.fetchval("SELECT count(*) FROM school_majors")
        schools_with = await conn.fetchval("SELECT count(DISTINCT school_id) FROM school_majors")
        logger.info(
            "DONE stats=%s school_majors=%s schools_with=%s",
            stats,
            total,
            schools_with,
        )
        return 0
    except Exception as exc:
        logger.exception("crawl_school_majors_once failed")
        if task_id is not None:
            await finish_task(conn, task_id, stats, error=str(exc)[:500])
        return 1
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
