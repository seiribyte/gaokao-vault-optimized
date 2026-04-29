from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from scrapling.parser import Adaptor

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.spiders.special_spider import SpecialSpider


def _make_spider() -> SpecialSpider:
    db_config = DatabaseConfig(
        dsn="postgresql://test:test@localhost:5432/test_db",
        pool_min=1,
        pool_max=2,
    )
    return SpecialSpider(db_config=db_config, crawl_task_id=1)


def _make_response(html: str, url: str, meta: dict | None = None) -> MagicMock:
    adaptor = Adaptor(content=html, url=url)
    response = MagicMock()
    response.status = 200
    response.url = url
    response.css = adaptor.css
    response.request = MagicMock()
    response.request.meta = meta or {}
    response.request.url = url
    return response


async def _collect(async_gen) -> list:
    items = []
    async for item in async_gen:
        items.append(item)
    return items


_STRONG_BASE_HTML = """
<html>
  <body>
    <div class="article-content">
      <p>报名网址: https://bm.chsi.com.cn/jcxkzs/sch/10001</p>
      <p>报名时间: 2025年4月10日至2025年4月30日.</p>
      <p>招生专业: 数学类,物理学类.</p>
      <p>入围规则: 按高考成绩确定入围名单.</p>
      <p>录取规则: 按综合成绩择优录取.</p>
    </div>
  </body>
</html>
"""


def test_parse_detail_extracts_strong_base_structured_fields() -> None:
    spider = _make_spider()
    response = _make_response(
        _STRONG_BASE_HTML,
        "https://gaokao.chsi.com.cn/gkxx/qjjh/test.html",
        {
            "item_data": {
                "enrollment_type": "强基计划",
                "year": 2025,
                "title": "测试大学2025年强基计划招生简章",
                "source_url": "https://gaokao.chsi.com.cn/gkxx/qjjh/test.html",
            }
        },
    )

    with patch.object(spider, "process_item", new=AsyncMock(return_value="new")):
        items = asyncio.run(_collect(spider.parse_detail(response)))

    assert len(items) == 1
    assert items[0]["application_url"] == "https://bm.chsi.com.cn/jcxkzs/sch/10001"
    assert str(items[0]["registration_start"]) == "2025-04-10"
    assert str(items[0]["registration_end"]) == "2025-04-30"
    assert items[0]["eligible_majors"] == ["数学类", "物理学类"]
    assert items[0]["selection_rule"] == "按高考成绩确定入围名单"
    assert items[0]["admission_rule"] == "按综合成绩择优录取"
    assert items[0]["quality_flags"] == []
