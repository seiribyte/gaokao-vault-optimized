from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.models.major import MajorItem
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.major_spider import MajorSpider


def _make_spider() -> MajorSpider:
    return MajorSpider(
        db_config=DatabaseConfig(
            dsn="postgresql://test:test@localhost:5432/test_db",
            pool_min=1,
            pool_max=2,
        ),
        crawl_task_id=1,
    )


def test_major_model_rejects_missing_or_blank_code() -> None:
    assert validate_item(MajorItem, {"name": "数学", "education_level": "本科"}) is None
    assert validate_item(MajorItem, {"code": "   ", "name": "数学", "education_level": "本科"}) is None


def test_parse_specialities_skips_missing_code_without_persisting() -> None:
    spider = _make_spider()
    response = SimpleNamespace(
        request=SimpleNamespace(meta={"education_level": "本科", "category_id": 1, "subcategory_id": 2}),
        status=200,
        url="https://example.invalid/api",
        body=json.dumps({
            "flag": True,
            "msg": [
                {"zydm": "", "zymc": "无代码专业"},
                {"zydm": "  030101 ", "zymc": " 法学 "},
            ],
        }).encode(),
    )

    async def collect():
        return [item async for item in spider.parse_specialities(cast(Any, response))]

    with patch.object(spider, "process_item", new=AsyncMock(return_value="new")) as process_item:
        items = asyncio.run(collect())

    assert len(items) == 1
    assert items[0]["code"] == "030101"
    process_item.assert_awaited_once()
