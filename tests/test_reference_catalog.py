from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from gaokao_vault.scheduler.reference_catalog import (
    _allocate_catalog_sch_id,
    _read_reference_schools,
    _resolve_school,
)


def test_read_reference_schools_deduplicates_names(tmp_path: Path) -> None:
    path = tmp_path / "reference.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append([None] * 65)
    sheet.append([None] * 65)
    for city in ("海淀区", "北京"):
        row = [None] * 65
        row[7] = "测试大学"
        row[41] = "北京"
        row[42] = city
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

    allocated = _allocate_catalog_sch_id(2131, "菏泽家政职业学院", occupied)

    assert allocated < 0
    assert allocated not in occupied
    assert allocated == _allocate_catalog_sch_id(2131, "菏泽家政职业学院", occupied)
