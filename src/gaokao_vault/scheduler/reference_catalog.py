from __future__ import annotations

import asyncio
import hashlib
import json
import re
from pathlib import Path
from typing import cast
from urllib.parse import quote
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

import asyncpg
from openpyxl import load_workbook

_SEARCH_URL = "https://gaokao.chsi.com.cn/sch/search.do?searchType=1&yxmc={}"
_INFO_URL = "https://gaokao.chsi.com.cn/wap/sch/schinfo/{}"
_GAOKAO_INDEX_URL = "https://static-data.gaokao.cn/www/2.0/school/name.json"
_SCHOOL_ID_RE = re.compile(r"schoolInfo--schId-(\d+)")
_REFERENCE_ALIASES = {
    "中国人民大学(苏州校区)": "中国人民大学",
    "北京交通大学(威海校区)": "北京交通大学",
    "西南大学(荣昌校区)": "西南大学",
}


async def sync_reference_schools(conn: asyncpg.Connection, reference_path: Path) -> dict[str, int]:
    targets = await asyncio.to_thread(_read_reference_schools, reference_path)
    existing_rows = await conn.fetch("SELECT name, sch_id FROM schools")
    existing = {str(row["name"]).strip() for row in existing_rows if row["name"]}
    pending = [target for target in targets if target["name"] not in existing]
    results = await asyncio.gather(*(asyncio.to_thread(_resolve_school, target) for target in pending))
    resolved = [result for result in results if result is not None]
    async with conn.transaction():
        for school in resolved:
            await conn.execute(
                """
                INSERT INTO schools (sch_id, name, province_id, city, authority, level,
                    is_211, is_985, is_double_first, is_private, is_independent, is_sino_foreign,
                    content_hash, crawl_task_id)
                VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, $7, FALSE, FALSE, FALSE, NULL, NULL)
                ON CONFLICT (sch_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    province_id = COALESCE(EXCLUDED.province_id, schools.province_id),
                    city = COALESCE(EXCLUDED.city, schools.city),
                    authority = COALESCE(EXCLUDED.authority, schools.authority),
                    is_double_first = EXCLUDED.is_double_first
                """,
                _safe_catalog_sch_id(school),
                school["name"],
                school["province_id"],
                school["city"],
                school["authority"],
                school["level"],
                school["is_double_first"],
            )
    return {
        "reference_schools": len(targets),
        "already_present": len(targets) - len(pending),
        "resolved": len(resolved),
        "unresolved": len(pending) - len(resolved),
    }


async def sync_gaokao_school_index(conn: asyncpg.Connection) -> dict[str, int]:
    payload = await asyncio.to_thread(_fetch_json, _GAOKAO_INDEX_URL)
    rows = payload.get("data") if isinstance(payload, dict) else None
    targets = [row for row in rows if isinstance(row, dict) and row.get("name")] if isinstance(rows, list) else []
    existing_rows = await conn.fetch("SELECT name, sch_id FROM schools")
    existing = {str(row["name"]).strip() for row in existing_rows if row["name"]}
    occupied_ids = {int(row["sch_id"]) for row in existing_rows}
    pending = [row for row in targets if str(row["name"]).strip() not in existing]
    for row in pending:
        name = str(row["name"]).strip()
        sch_id = _allocate_catalog_sch_id(int(row.get("school_id")), name, occupied_ids)
        await conn.execute(
            """
            INSERT INTO schools (sch_id, name, is_211, is_985, is_double_first,
                is_private, is_independent, is_sino_foreign)
            VALUES ($1, $2, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE)
            ON CONFLICT (sch_id) DO NOTHING
            """,
            sch_id,
            name,
        )
        occupied_ids.add(sch_id)
        existing.add(name)
    return {"index_schools": len(targets), "already_present": len(targets) - len(pending), "added": len(pending)}


def _read_reference_schools(path: Path) -> list[dict[str, str]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        worksheet = workbook.active
        result: dict[str, dict[str, str]] = {}
        for row in worksheet.iter_rows(min_row=3, values_only=True):
            name = str(row[7]).strip() if len(row) > 7 and row[7] else ""
            if name and name not in result:
                result[name] = {
                    "name": name,
                    "province": str(row[41]).strip() if len(row) > 41 and row[41] else "",
                    "city": str(row[42]).strip() if len(row) > 42 and row[42] else "",
                }
        return list(result.values())
    finally:
        workbook.close()


def _resolve_school(target: dict[str, str]) -> dict[str, object] | None:
    try:
        expected_name = _REFERENCE_ALIASES.get(target["name"], target["name"])
        html = _fetch(_SEARCH_URL.format(quote(expected_name)))
        candidates = _SCHOOL_ID_RE.findall(html)
        for sch_id in dict.fromkeys(candidates):
            payload = _fetch_json(_INFO_URL.format(sch_id))
            msg = payload.get("msg") if payload.get("flag") else None
            official_name = str(msg.get("yxmc", "")).strip() if isinstance(msg, dict) else ""
            if not isinstance(msg, dict) or official_name != expected_name:
                continue
            return {
                "sch_id": int(sch_id),
                "name": target["name"],
                "official_name": official_name,
                "province_id": None,
                "city": target["city"] or msg.get("yxszd"),
                "authority": msg.get("zgbmmc"),
                "level": None,
                "is_double_first": bool(msg.get("syl")),
            }
    except OSError:
        return None
    return None


def _safe_catalog_sch_id(school: dict[str, object]) -> int:
    sch_id = int(cast(int, school["sch_id"]))
    if sch_id <= 2_147_483_647 and school.get("official_name") == school.get("name"):
        return sch_id
    return _synthetic_catalog_sch_id(str(school["name"]))


def _allocate_catalog_sch_id(source_id: int, name: str, occupied_ids: set[int]) -> int:
    if source_id <= 2_147_483_647 and source_id not in occupied_ids:
        return source_id
    candidate = _synthetic_catalog_sch_id(name)
    while candidate in occupied_ids:
        candidate -= 1
    return candidate


def _synthetic_catalog_sch_id(name: str) -> int:
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return -(int.from_bytes(digest[:4], "big") % 2_000_000_000 + 1)


def _fetch(url: str) -> str:
    request = UrlRequest(url, headers={"User-Agent": "Mozilla/5.0"})  # noqa: S310
    with urlopen(request, timeout=20) as response:  # noqa: S310
        return response.read().decode("utf-8", errors="replace")


def _fetch_json(url: str) -> dict:
    try:
        return json.loads(_fetch(url))
    except (OSError, ValueError):
        return {}
