from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from openpyxl import Workbook

from gaokao_vault.scheduler.reference_catalog import (
    _allocate_catalog_sch_id,
    _read_reference_schools,
    _resolve_school,
    sync_gaokao_school_index,
    sync_reference_schools,
)


class _Transaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _CatalogConnection:
    def __init__(self, schools: list[dict] | None = None, provinces: list[dict] | None = None) -> None:
        self.schools = schools or []
        self.provinces = provinces or []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.next_id = 100

    async def fetch(self, query: str):
        if "FROM schools" in query:
            return self.schools
        if "FROM provinces" in query:
            return self.provinces
        return []

    async def execute(self, query: str, *args: object):
        self.execute_calls.append((query, args))
        return "UPDATE 1"

    async def fetchrow(self, query: str, *args: object):
        self.fetchrow_calls.append((query, args))
        return {"id": self.next_id}

    def transaction(self):
        return _Transaction()


def test_read_reference_schools_deduplicates_names(tmp_path: Path) -> None:
    path = tmp_path / "reference.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append([None] * 65)
    sheet.append([None] * 65)
    for city in ("海淀区", "北京"):
        row = [None] * 65
        row[7] = "测试大学"
        row[40] = "北京"
        row[41] = city
        sheet.append(row)
    workbook.save(path)

    schools = _read_reference_schools(path)

    assert schools == [{"name": "测试大学", "province": "北京", "city": "海淀区"}]


def test_resolve_school_requires_exact_official_name() -> None:
    target = {"name": "三亚学院", "province": "海南", "city": "三亚"}
    payload = {
        "flag": True,
        "msg": {"yxmc": "三亚学院", "yxszd": "海南", "zgbmmc": "海南省教育厅", "syl": False},
    }

    with (
        patch(
            "gaokao_vault.scheduler.reference_catalog._fetch",
            return_value='<a href="/sch/schoolInfo--schId-402800.dhtml">三亚学院</a>',
        ),
        patch("gaokao_vault.scheduler.reference_catalog._fetch_json", return_value=payload),
    ):
        school = _resolve_school(target)

    assert school is not None
    assert school["sch_id"] == 402800
    assert school["name"] == "三亚学院"
    assert school["city"] == "三亚"


def test_catalog_id_collision_uses_stable_negative_id() -> None:
    occupied = {2131}

    allocated = _allocate_catalog_sch_id("菏泽家政职业学院", occupied)

    assert allocated < 0
    assert allocated not in occupied
    assert allocated == _allocate_catalog_sch_id("菏泽家政职业学院", occupied)


def test_gaokao_school_index_uses_negative_placeholder_and_separate_source_id() -> None:
    conn = _CatalogConnection()
    payload = {"data": [{"name": "测试大学", "school_id": 118}]}

    with patch("gaokao_vault.scheduler.reference_catalog._fetch_json", return_value=payload):
        result = asyncio.run(sync_gaokao_school_index(cast(Any, conn)))

    assert result["added"] == 1
    assert len(conn.fetchrow_calls) == 1
    query, args = conn.fetchrow_calls[0]
    assert "gaokao_school_id" in query
    assert int(cast(int, args[0])) < 0
    assert args[1:] == (118, "测试大学")


def test_gaokao_school_index_migrates_legacy_source_id_placeholder() -> None:
    legacy = {
        "id": 7,
        "name": "测试大学",
        "sch_id": 118,
        "gaokao_school_id": None,
        "province_id": None,
        "city": None,
        "authority": None,
        "level": None,
        "is_211": False,
        "is_985": False,
        "is_double_first": False,
        "is_private": False,
        "is_independent": False,
        "is_sino_foreign": False,
        "school_type": None,
        "website": None,
        "phone": None,
        "email": None,
        "address": None,
        "introduction": None,
        "logo_url": None,
        "content_hash": None,
        "crawl_task_id": None,
    }
    conn = _CatalogConnection(schools=[legacy])
    payload = {"data": [{"name": "测试大学", "school_id": 118}]}

    with patch("gaokao_vault.scheduler.reference_catalog._fetch_json", return_value=payload):
        result = asyncio.run(sync_gaokao_school_index(cast(Any, conn)))

    assert result["migrated"] == 1
    query, args = conn.execute_calls[0]
    assert "SET sch_id = $2, gaokao_school_id = $3" in query
    assert args[0] == 7
    assert int(cast(int, args[1])) < 0
    assert args[2] == 118


def test_gaokao_school_index_does_not_reassign_owned_source_id() -> None:
    owner = {
        "id": 7,
        "name": "甲大学",
        "sch_id": 34,
        "gaokao_school_id": 118,
        "crawl_task_id": 1,
    }
    conn = _CatalogConnection(schools=[owner])
    payload = {"data": [{"name": "乙大学", "school_id": 118}]}

    with patch("gaokao_vault.scheduler.reference_catalog._fetch_json", return_value=payload):
        result = asyncio.run(sync_gaokao_school_index(cast(Any, conn)))

    assert result["conflicts"] == 1
    assert conn.execute_calls == []
    assert conn.fetchrow_calls == []


def test_reference_catalog_collision_does_not_rename_existing_school(tmp_path: Path) -> None:
    conn = _CatalogConnection(
        schools=[{"id": 7, "name": "甲大学", "sch_id": 34}],
        provinces=[{"id": 1, "name": "北京"}],
    )
    resolved = {
        "sch_id": 34,
        "name": "乙大学",
        "official_name": "乙大学",
        "province": "北京",
        "province_id": None,
        "city": "北京",
        "authority": "教育部",
        "level": None,
        "is_double_first": False,
    }

    with (
        patch(
            "gaokao_vault.scheduler.reference_catalog._read_reference_schools",
            return_value=[{"name": "乙大学", "province": "北京", "city": "北京"}],
        ),
        patch("gaokao_vault.scheduler.reference_catalog._resolve_school", return_value=resolved),
    ):
        asyncio.run(sync_reference_schools(cast(Any, conn), tmp_path / "reference.xlsx"))

    insert_query, insert_args = conn.execute_calls[0]
    assert "ON CONFLICT (sch_id) DO NOTHING" in insert_query
    assert int(cast(int, insert_args[0])) < 0
    assert insert_args[1] == "乙大学"
