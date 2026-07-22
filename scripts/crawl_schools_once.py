"""Full schools crawl via sequential sch_id detail pages.

The stock SchoolSpider schedules 1..5000 detail requests plus list discovery,
but on Windows scrapling checkpoint rename can terminate early after only a
few dozen schools. This script walks known-valid sch_id ranges directly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from typing import Any

import asyncpg
from scrapling.fetchers import AsyncStealthySession

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.db.queries.schools import upsert_school
from gaokao_vault.models.school import SchoolItem
from gaokao_vault.pipeline.dedup import deduplicate_and_persist_on_connection
from gaokao_vault.pipeline.hasher import compute_content_hash
from gaokao_vault.pipeline.validator import validate_item

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_schools_once")

BASE = "https://gaokao.chsi.com.cn"
DETAIL_URL = f"{BASE}/sch/schoolInfoMain--schId-{{sch_id}}.dhtml"

# Keep the same brute-force range as the canonical SchoolSpider.
SCH_ID_START = 1
SCH_ID_END = 5000
_VARCHAR_LIMITS = {
    "name": 100,
    "city": 50,
    "authority": 100,
    "level": 20,
    "school_type": 30,
    "website": 255,
    "phone": 100,
    "email": 100,
    "address": 255,
    "logo_url": 255,
}


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _truncate_item(item: dict[str, Any]) -> dict[str, Any]:
    for field, max_len in _VARCHAR_LIMITS.items():
        val = item.get(field)
        if isinstance(val, str) and len(val) > max_len:
            item[field] = val[:max_len]
    return item


def _extract_name(html: str) -> str:
    m = re.search(r"<div class=\"content-header\"[^>]*>([\s\S]*?)</div>", html)
    if not m:
        # fallback title: 北京大学_院校信息库_阳光高考
        t = re.search(r"<title>([^_<]+)_", html)
        return _clean(t.group(1)) if t else ""
    texts = [_clean(x) for x in re.findall(r">([^<>]+)<", m.group(1))]
    for t in texts:
        if t and "关注" not in t and not t.isdigit():
            return t
    return ""


def _has_error_page(html: str) -> bool:
    return "错误提示_阳光高考" in html or "您访问的页面不存在" in html


def parse_school_html(html: str, sch_id: int) -> dict[str, Any] | None:
    if _has_error_page(html):
        return None
    name = _extract_name(html)
    if not name:
        return None

    data: dict[str, Any] = {"sch_id": sch_id, "name": name}

    # tags — match both explicit 985/211 labels and common variants
    tag_blob = " ".join(re.findall(r"class=\"sch-level-tag\"[^>]*>([^<]+)", html))
    intro_blob = " ".join(re.findall(r"content-introduction[\s\S]{0,2500}", html))
    blob = f"{tag_blob} {intro_blob}"
    data["is_211"] = bool(re.search(r"\b211\b|211工程", blob))
    data["is_985"] = bool(re.search(r"\b985\b|985工程", blob))
    data["is_double_first"] = "双一流" in blob
    data["is_private"] = "民办" in blob
    data["is_independent"] = "独立学院" in blob
    data["is_sino_foreign"] = "中外合作办学" in blob

    # logo
    logo = re.search(r"yxxx-header-img[\s\S]{0,200}?src=\"([^\"]+)\"", html)
    if not logo:
        logo = re.search(r"https://t1\.chei\.com\.cn/common/xh/\d+\.jpg", html)
        if logo:
            data["logo_url"] = logo.group(0)
    else:
        data["logo_url"] = logo.group(1)

    # simple class-based fields
    for cls, field in {
        "yxszd": "city",
        "txdz": "address",
        "gfdh": "phone",
    }.items():
        m = re.search(rf"class=\"[^\"]*{cls}[^\"]*\"[^>]*>([^<]+)", html)
        if m:
            data[field] = _clean(m.group(1))

    for cls, field in {
        "gfwz": "website",
        "zswz": "recruit_website",
    }.items():
        m = re.search(rf"class=\"[^\"]*{cls}[^\"]*\"[^>]*href=\"([^\"]+)\"", html)
        if m:
            data[field] = _clean(m.group(1))

    # authority / school type from introduction block
    dep = re.search(r"class=\"department\"[\s\S]{0,300}</div>", html)
    if dep:
        spans = [_clean(x) for x in re.findall(r"<span[^>]*>([^<]+)</span>", dep.group(0))]
        spans = [s for s in spans if s]
        if spans:
            data["authority"] = spans[-1]

    yxtx = re.search(r"class=\"yxtx\"[\s\S]{0,500}</div>", html)
    if yxtx:
        types = [_clean(x) for x in re.findall(r"<span[^>]*>([^<]+)</span>", yxtx.group(0))]
        types = [t for t in types if t]
        if types:
            data["school_type"] = " | ".join(types)

    # level tags
    levels = [_clean(x) for x in re.findall(r"class=\"sch-level-tag\"[^>]*>([^<]+)", html)]
    levels = [x for x in levels if x]
    if levels:
        # Prefer 本科/专科 as level; keep the rest in school_type if empty
        for lv in levels:
            if "本科" in lv or "专科" in lv or "高职" in lv:
                data["level"] = lv
                break
        else:
            data["level"] = levels[0]
        if not data.get("school_type"):
            extra = [lv for lv in levels if lv != data.get("level")]
            if extra:
                data["school_type"] = " | ".join(extra)

    # introduction text
    intro = re.search(r"class=\"content-introduction\"([\s\S]{0,5000})</div>", html)
    if intro:
        text = _clean(re.sub(r"<[^>]+>", " ", intro.group(1)))
        if text:
            data["introduction"] = text[:5000]

    return _truncate_item(data)


async def ensure_task(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO crawl_tasks (task_type, status, params)
        VALUES ('schools', 'running', '{"mode":"full","runner":"crawl_schools_once"}'::jsonb)
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


async def resolve_province_id(conn: asyncpg.Connection, province_map: dict[str, int], city: str | None) -> int | None:
    if not city:
        return None
    text = city.strip()
    simplified = (
        text
        .replace("壮族自治区", "")
        .replace("回族自治区", "")
        .replace("维吾尔自治区", "")
        .replace("自治区", "")
        .replace("省", "")
        .replace("市", "")
    )
    for name, pid in province_map.items():
        plain = name.replace("省", "").replace("市", "")
        if name in text or plain in text or plain == simplified:
            return pid
    return None


async def main() -> int:
    dsn = os.environ.get("GAOKAO_DB__DSN") or DatabaseConfig().dsn
    stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}
    task_id: int | None = None
    conn = await asyncpg.connect(dsn)
    try:
        task_id = await ensure_task(conn)
        rows = await conn.fetch("SELECT id, name FROM provinces")
        province_map = {r["name"]: r["id"] for r in rows}

        max_existing = await conn.fetchval("SELECT COALESCE(MAX(sch_id), 0) FROM schools")
        logger.info(
            "task_id=%s scanning complete sch_id range %d..%d (max_existing=%s)",
            task_id,
            SCH_ID_START,
            SCH_ID_END,
            max_existing,
        )

        async with AsyncStealthySession(
            headless=True,
            google_search=False,
            network_idle=False,
            timeout=120000,
            wait=2500,
            extra_headers={"Referer": "https://gaokao.chsi.com.cn/"},
        ) as session:
            warm = await session.fetch(f"{BASE}/sch/search--ss-on,option-qg,searchType-1,start-0.dhtml")
            logger.info("warmup status=%s", warm.status)

            for sch_id in range(SCH_ID_START, SCH_ID_END + 1):
                url = DETAIL_URL.format(sch_id=sch_id)
                try:
                    resp = await session.fetch(url)
                    html = (resp.body or b"").decode("utf-8", errors="replace")
                except Exception:
                    logger.exception("fetch failed sch_id=%s", sch_id)
                    stats["failed"] += 1
                    continue

                try:
                    item = parse_school_html(html, sch_id)
                    if item is None:
                        continue

                    item["province_id"] = await resolve_province_id(conn, province_map, item.get("city"))
                    validated = validate_item(SchoolItem, item)
                    if validated is None:
                        stats["failed"] += 1
                        continue
                    change = await deduplicate_and_persist_on_connection(
                        conn,
                        entity_type="schools",
                        item=validated,
                        content_hash=compute_content_hash(validated),
                        unique_keys={"sch_id": validated["sch_id"]},
                        crawl_task_id=task_id,
                        upsert_fn=upsert_school,
                    )
                    stats[change] += 1
                except Exception:
                    logger.exception("persist failed sch_id=%s", sch_id)
                    stats["failed"] += 1
                    continue

                total = stats["new"] + stats["updated"] + stats["unchanged"]
                if total % 25 == 0:
                    logger.info("progress sch_id=%d stats=%s", sch_id, stats)

        await finish_task(conn, task_id, stats)
        total = await conn.fetchval("SELECT count(*) FROM schools")
        logger.info("DONE stats=%s schools_total=%s", stats, total)
        return 0
    except Exception as exc:
        logger.exception("fatal")
        if task_id is not None:
            await finish_task(conn, task_id, stats, error=str(exc))
        return 1
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
