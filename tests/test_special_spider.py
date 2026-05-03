from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from scrapling.parser import Adaptor
from scrapling.spiders import Request

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.spiders.special_spider import SpecialSpider, _extract_registration_dates


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
      <p>校测规则: 学校考核包括笔试和面试.</p>
      <p>录取规则: 按综合成绩择优录取.</p>
      <p>综合成绩公式: 综合成绩=高考成绩*85%+校测成绩*15%.</p>
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
                "province_code": "11",
                "year": 2025,
                "title": "测试大学2025年强基计划招生简章",
                "source_url": "https://gaokao.chsi.com.cn/gkxx/qjjh/test.html",
            }
        },
    )

    with (
        patch(
            "gaokao_vault.spiders.special_spider.find_school_by_name",
            new=AsyncMock(return_value={"id": 10001}),
        ),
        patch.object(spider, "_get_pool", new=AsyncMock(return_value=_FakePool(AsyncMock()))),
        patch.object(spider, "process_item", new=AsyncMock(return_value="new")),
    ):
        items = asyncio.run(_collect(spider.parse_detail(response)))

    assert len(items) == 1
    assert items[0]["school_id"] == 10001
    assert items[0]["special_admission_type"] == "strong_foundation"
    assert items[0]["province_code"] == "11"
    assert items[0]["application_url"] == "https://bm.chsi.com.cn/jcxkzs/sch/10001"
    assert items[0]["registration_window"] == {"start": "2025-04-10", "end": "2025-04-30"}
    assert str(items[0]["registration_start"]) == "2025-04-10"
    assert str(items[0]["registration_end"]) == "2025-04-30"
    assert items[0]["eligible_majors"] == ["数学类", "物理学类"]
    assert items[0]["shortlist_rule"] == "按高考成绩确定入围名单"
    assert items[0]["selection_rule"] == "按高考成绩确定入围名单"
    assert items[0]["school_assessment"] == "学校考核包括笔试和面试"
    assert items[0]["school_exam_rule"] == "学校考核包括笔试和面试"
    assert items[0]["composite_score_formula"] == "综合成绩=高考成绩*85%+校测成绩*15%"
    assert items[0]["admission_rule"] == "按综合成绩择优录取"
    assert items[0]["quality_flags"] == []


def test_parse_dxsbb_list_yields_special_article_requests() -> None:
    spider = _make_spider()
    response = _make_response(
        """
        <div class="listBox">
          <a href="/news/70169.html" target="_blank">
            <div class="b"><h3>强基计划招生程序及管理要求</h3><p class="time">2026-4-7</p></div>
          </a>
          <a href="/news/1978.html" target="_blank">
            <div class="b"><h3>平行志愿录取规则流程</h3><p class="time">2025-5-15</p></div>
          </a>
        </div>
        """,
        "https://www.dxsbb.com/news/list_130.html",
        {"enrollment_type": "强基计划", "special_admission_type": "strong_foundation"},
    )

    results = asyncio.run(_collect(spider.parse_dxsbb_list(response)))

    assert len(results) == 1
    assert isinstance(results[0], Request)
    assert results[0].url == "https://www.dxsbb.com/news/70169.html"
    assert results[0].meta["title"] == "强基计划招生程序及管理要求"
    assert results[0].meta["enrollment_type"] == "强基计划"
    assert results[0].meta["special_admission_type"] == "strong_foundation"


def test_parse_dxsbb_article_persists_special_enrollment_content() -> None:
    spider = _make_spider()
    response = _make_response(
        """
        <div id="article">
          <h1>强基计划招生程序及管理要求</h1>
          <div class="update">更新:2026-4-7 10:20:00&nbsp;&nbsp;发布:大学生必备网</div>
          <div class="content">
            <p>报名时间: 2026年4月10日至2026年4月30日.</p>
            <p>招生专业: 数学类,物理学类.</p>
            <p>录取规则: 按综合成绩择优录取.</p>
          </div>
        </div>
        """,
        "https://www.dxsbb.com/news/70169.html",
        {
            "enrollment_type": "强基计划",
            "special_admission_type": "strong_foundation",
            "title": "强基计划招生程序及管理要求",
        },
    )

    with patch.object(spider, "process_item", new=AsyncMock(return_value="new")) as process_item:
        items = asyncio.run(_collect(spider.parse_dxsbb_article(response)))

    assert len(items) == 1
    assert items[0]["enrollment_type"] == "强基计划"
    assert items[0]["special_admission_type"] == "strong_foundation"
    assert items[0]["year"] == 2026
    assert items[0]["title"] == "强基计划招生程序及管理要求"
    assert items[0]["source_url"] == "https://www.dxsbb.com/news/70169.html"
    assert str(items[0]["publish_date"]) == "2026-04-07"
    assert str(items[0]["registration_start"]) == "2026-04-10"
    assert str(items[0]["registration_end"]) == "2026-04-30"
    assert items[0]["eligible_majors"] == ["数学类", "物理学类"]
    process_item.assert_awaited_once()


def test_extract_registration_dates_returns_empty_values_for_invalid_dates() -> None:
    assert _extract_registration_dates("报名时间: 2024年13月1日至2024年13月31日") == (None, None)


class _Acquire:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)
