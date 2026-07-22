"""Crawl enrollment plans via gaokao.cn index + zjzw plan API.

Uses scrapling Fetcher.get (sync HTTP) — FetcherSession has no .fetch(),
and Stealth browser is slow/unreliable for pure JSON APIs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

import asyncpg
from scrapling.fetchers import Fetcher

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.enrollment import upsert_enrollment_plan
from gaokao_vault.models.enrollment import EnrollmentPlanItem
from gaokao_vault.pipeline.admission_rules import (
    extract_adjustment_rule,
    extract_eligibility_requirements,
    extract_physical_exam_limit,
    extract_physical_exam_or_political_review,
    extract_political_review_requirement,
    extract_program_type,
    extract_service_obligation,
    extract_single_subject_limit,
)
from gaokao_vault.pipeline.batch_normalizer import normalize_batch
from gaokao_vault.pipeline.dedup import deduplicate_and_persist_on_connection
from gaokao_vault.pipeline.hasher import compute_content_hash
from gaokao_vault.pipeline.quality import missing_field_flags
from gaokao_vault.pipeline.validator import validate_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("crawl_enrollment_plans_once")

SCHOOL_NAME_INDEX_URL = "https://static-data.gaokao.cn/www/2.0/school/name.json"
PLAN_DIC_URL = "https://static-data.gaokao.cn/www/2.0/school/{school_id}/dic/specialplan.json"
# Bare size=50 returns code=0000 with empty data=[]; extra filter params are required.
PLAN_API = (
    "https://api.zjzw.cn/web/api?uri=apidata/api/gkv3/plan/school"
    "&school_id={school_id}&year={year}&local_province_id={province}"
    "&page={page}&size=20&special_group=&local_batch_id=&local_type_id=&keyword="
)
YEAR_START = 2021
DATA_SOURCE = "gaokao.cn"
PAGE_SIZE = 20
HTTP_HEADERS = {
    "Referer": "https://www.gaokao.cn/",
    "Origin": "https://www.gaokao.cn",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}
# Keep request rate low — API returns code 1069 when flooded.
HTTP_WORKERS = 1
REQUEST_SLEEP_SEC = 0.35
RATE_LIMIT_SLEEP_SEC = 20.0
MAX_RATE_LIMIT_RETRIES = 6
_VARCHAR_LIMITS = {
    "batch": 50,
    "batch_code": 30,
    "batch_category": 30,
    "batch_segment": 30,
    "major_name": 100,
    "duration": 20,
    "tuition": 50,
    "note": 500,
    "major_group_code": 50,
    "major_code_raw": 50,
    "campus": 100,
    "education_location": 100,
    "selection_requirement": 255,
    "physical_exam_limit": 255,
    "single_subject_limit": 255,
    "adjustment_rule": 255,
    "program_type": 100,
    "data_source": 100,
    "source_url": 255,
}


def _norm_name(name: str) -> str:
    return "".join((name or "").split())


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clip(value: Any, max_len: int) -> str | None:
    text = _safe_text(value)
    if text is None:
        return None
    return text[:max_len]


def _truncate_item(item: dict[str, Any]) -> dict[str, Any]:
    for field, max_len in _VARCHAR_LIMITS.items():
        if field in item:
            item[field] = _clip(item.get(field), max_len)
    return item


def http_get_json(url: str) -> dict[str, Any] | None:
    import time

    for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
        try:
            time.sleep(REQUEST_SLEEP_SEC)
            resp = Fetcher.get(url, headers=HTTP_HEADERS, timeout=30)
            raw = resp.body or b""
            if getattr(resp, "status", 0) != 200 or not raw:
                return None
            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="ignore")
            data = json.loads(text)
            if not isinstance(data, dict):
                return None
            # 1069 = rate limited
            if str(data.get("code")) == "1069":
                logger.warning(
                    "rate limited attempt=%s sleep=%.0fs url=%s",
                    attempt,
                    RATE_LIMIT_SLEEP_SEC * attempt,
                    url,
                )
                time.sleep(RATE_LIMIT_SLEEP_SEC * attempt)
                continue
            return data
        except Exception as exc:
            logger.debug("http_get_json failed url=%s err=%s", url, exc)
            return None
    logger.error("rate limited permanently url=%s", url)
    return None


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('enrollment_plans', 'running', '{"mode":"full","runner":"crawl_enrollment_plans_once"}'::jsonb)
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


async def resolve_major_id(conn: asyncpg.Connection, major_name: str | None) -> int | None:
    if not major_name:
        return None
    rows = await conn.fetch("SELECT id FROM majors WHERE name=$1 ORDER BY id", major_name)
    return rows[0]["id"] if len(rows) == 1 else None


async def resolve_subject_category(conn: asyncpg.Connection, cache: dict[str, int], text: str | None) -> int | None:
    if not text:
        return None
    if text in cache:
        return cache[text]
    row = await conn.fetchrow(
        """
        INSERT INTO subject_categories (name, category_type)
        VALUES ($1, 'unknown')
        ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
        """,
        text[:20],
    )
    cache[text] = row["id"]
    return row["id"]


async def persist_plan(
    conn: asyncpg.Connection,
    item: dict[str, Any],
    task_id: int,
    stats: dict[str, int],
) -> None:
    validated = validate_item(EnrollmentPlanItem, item)
    if validated is None:
        stats["failed"] += 1
        return
    content_hash = compute_content_hash(validated)
    change = await deduplicate_and_persist_on_connection(
        conn,
        entity_type="enrollment_plans",
        item=validated,
        content_hash=content_hash,
        unique_keys={
            "school_id": validated["school_id"],
            "province_id": validated["province_id"],
            "year": validated["year"],
            "subject_category_id": validated.get("subject_category_id"),
            "batch": validated.get("batch"),
            "school_code_raw": validated.get("school_code_raw"),
            "major_group_code": validated.get("major_group_code"),
            "major_code_raw": validated.get("major_code_raw"),
            "major_name": validated.get("major_name"),
        },
        crawl_task_id=task_id,
        upsert_fn=upsert_enrollment_plan,
    )
    stats[change] += 1


def extract_items(payload: dict[str, Any] | None) -> tuple[list[dict[str, Any]], int]:
    if not payload or str(payload.get("code")) not in {"0000", "0"}:
        return [], 0
    data = payload.get("data")
    if isinstance(data, dict):
        items = data.get("item")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)], _safe_int(data.get("numFound")) or len(items)
        return [], 0
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)], len(data)
    return [], 0


async def crawl_school_province_year(
    pool: ThreadPoolExecutor,
    conn: asyncpg.Connection,
    *,
    school_db_id: int,
    gaokao_school_id: str,
    province_id: int,
    province_code: str,
    year: int,
    task_id: int,
    stats: dict[str, int],
    subject_cache: dict[str, int],
) -> int:
    inserted = 0
    page = 1
    loop = asyncio.get_running_loop()
    while True:
        url = PLAN_API.format(school_id=gaokao_school_id, year=year, province=province_code, page=page)
        payload = await loop.run_in_executor(pool, http_get_json, url)
        items, num_found = extract_items(payload)
        if not items:
            if page == 1 and payload is None:
                stats["failed"] += 1
            break

        for record in items:
            major_name = _safe_text(record.get("spname")) or _safe_text(record.get("sp_name"))
            if not major_name:
                continue
            major_lookup = _safe_text(record.get("sp_name")) or major_name
            major_id = await resolve_major_id(conn, major_lookup.split("（")[0].split("(")[0].strip())
            subject_raw = _safe_text(record.get("local_type_name")) or _safe_text(record.get("type"))
            subject_category_id = await resolve_subject_category(conn, subject_cache, subject_raw)
            batch = _safe_text(record.get("local_batch_name")) or _safe_text(record.get("batch"))
            batch_info = normalize_batch(batch)
            note_parts = [_safe_text(record.get("remark")), _safe_text(record.get("info"))]
            note = "".join(p for p in note_parts if p) or None
            selection = (
                _safe_text(record.get("sg_info"))
                or _safe_text(record.get("sp_info"))
                or _safe_text(record.get("sp_xuanke"))
            )
            item = {
                "school_id": school_db_id,
                "province_id": province_id,
                "year": year,
                "subject_category_id": subject_category_id,
                "batch": batch,
                "batch_code": batch_info.code,
                "batch_category": batch_info.category,
                "batch_segment": batch_info.segment,
                "major_name": major_name,
                "major_id": major_id,
                "plan_count": _safe_int(record.get("num")),
                "duration": _safe_text(record.get("length")),
                "tuition": _safe_text(record.get("tuition")),
                "note": note,
                "major_group_code": _safe_text(record.get("sg_name")) or _safe_text(record.get("special_group")),
                "major_code_raw": _safe_text(record.get("spcode")),
                "campus": _safe_text(record.get("campus")) or _safe_text(record.get("school_area")),
                "education_location": _safe_text(record.get("address")) or _safe_text(record.get("place")),
                "selection_requirement": selection,
                "physical_exam_limit": extract_physical_exam_limit(note),
                "single_subject_limit": extract_single_subject_limit(note),
                "adjustment_rule": extract_adjustment_rule(note),
                "program_type": extract_program_type(batch, note, _safe_text(record.get("zslx_name"))),
                "eligibility_requirements": extract_eligibility_requirements(note),
                "physical_exam_or_political_review": extract_physical_exam_or_political_review(note),
                "political_review_requirement": extract_political_review_requirement(note),
                "service_obligation": extract_service_obligation(note),
                "data_source": DATA_SOURCE,
                "source_url": url,
            }
            item = _truncate_item(item)
            item["quality_flags"] = missing_field_flags(item, ("major_id", "plan_count", "selection_requirement"))
            try:
                await persist_plan(conn, item, task_id, stats)
                inserted += 1
            except Exception:
                logger.exception(
                    "persist failed school=%s province=%s year=%s major=%s",
                    school_db_id,
                    province_code,
                    year,
                    major_name[:80],
                )
                stats["failed"] += 1

        if num_found and page * PAGE_SIZE >= num_found:
            break
        if len(items) < PAGE_SIZE:
            break
        page += 1
    return inserted


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    pool = ThreadPoolExecutor(max_workers=HTTP_WORKERS)
    try:
        task_id = await ensure_task(conn)
        # Resume: skip schools that already have any enrollment plans.
        schools = await conn.fetch(
            """
            SELECT s.id, s.sch_id, s.name
            FROM schools s
            ORDER BY s.id
            """
        )
        already = await conn.fetchval("SELECT count(DISTINCT school_id) FROM enrollment_plans")
        provinces = await conn.fetch("SELECT id, name, code FROM provinces ORDER BY id")
        years = list(range(YEAR_START, datetime.now().year + 1))
        subject_cache = {r["name"]: r["id"] for r in await conn.fetch("SELECT id, name FROM subject_categories")}
        logger.info(
            "task_id=%s remaining_schools=%s already_have_plans=%s provinces=%s years=%s",
            task_id,
            len(schools),
            already,
            len(provinces),
            years,
        )

        loop = asyncio.get_running_loop()
        idx_payload = await loop.run_in_executor(pool, http_get_json, SCHOOL_NAME_INDEX_URL)
        if not idx_payload or str(idx_payload.get("code")) not in {"0000", "0"}:
            raise RuntimeError(f"failed to load school name index: {idx_payload}")
        school_index = build_school_index(idx_payload.get("data") or [])
        logger.info("school index size=%s", len(school_index))

        smoke = await loop.run_in_executor(
            pool,
            http_get_json,
            PLAN_API.format(school_id="31", year=2024, province="11", page=1),
        )
        smoke_items, _ = extract_items(smoke)
        logger.info(
            "smoke school_id=31 year=2024 province=11 items=%s code=%s",
            len(smoke_items),
            (smoke or {}).get("code"),
        )
        if not smoke_items:
            raise RuntimeError(
                "smoke plan API returned 0 items — aborting "
                f"(code={(smoke or {}).get('code')} msg={(smoke or {}).get('message')})"
            )

        matched = 0
        for i, school in enumerate(schools, start=1):
            name = str(school["name"])
            gaokao_id = school_index.get(_norm_name(name))
            if not gaokao_id:
                continue
            matched += 1

            year_map: dict[str, list[int]] = {}
            dic = await loop.run_in_executor(pool, http_get_json, PLAN_DIC_URL.format(school_id=gaokao_id))
            if dic and str(dic.get("code")) in {"0000", "0"} and isinstance(dic.get("data"), dict):
                news = dic["data"].get("newsdata") if isinstance(dic["data"].get("newsdata"), dict) else {}
                raw_year = news.get("year") if isinstance(news, dict) else None
                if isinstance(raw_year, dict):
                    for pcode, ys in raw_year.items():
                        year_map[str(pcode)] = [y for y in (_safe_int(x) for x in (ys or [])) if y]

            before_new = stats["new"]
            for province in provinces:
                pcode = str(province["code"] or province["id"])
                available = year_map.get(pcode)
                use_years = [y for y in years if available is None or y in available]
                for year in use_years:
                    if year > datetime.now().year:
                        continue
                    await crawl_school_province_year(
                        pool,
                        conn,
                        school_db_id=int(school["id"]),
                        gaokao_school_id=gaokao_id,
                        province_id=int(province["id"]),
                        province_code=pcode,
                        year=year,
                        task_id=task_id,
                        stats=stats,
                        subject_cache=subject_cache,
                    )

            if matched % 2 == 0 or i == len(schools):
                total_now = await conn.fetchval("SELECT count(*) FROM enrollment_plans")
                schools_now = await conn.fetchval("SELECT count(DISTINCT school_id) FROM enrollment_plans")
                logger.info(
                    "progress remaining=%s/%s matched=%s inserts_delta=%s stats=%s db_plans=%s db_schools=%s",
                    i,
                    len(schools),
                    matched,
                    stats["new"] - before_new,
                    stats,
                    total_now,
                    schools_now,
                )

        await finish_task(conn, task_id, stats)
        total = await conn.fetchval("SELECT count(*) FROM enrollment_plans")
        schools_with = await conn.fetchval("SELECT count(DISTINCT school_id) FROM enrollment_plans")
        logger.info(
            "DONE stats=%s enrollment_plans=%s schools_with_plans=%s matched_this_run=%s",
            stats,
            total,
            schools_with,
            matched,
        )
        return 0
    except Exception as exc:
        logger.exception("fatal")
        if task_id is not None:
            await finish_task(conn, task_id, stats, error=str(exc))
        return 1
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
