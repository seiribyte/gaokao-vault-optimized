from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from scrapling.parser import Adaptor
from scrapling.spiders import Request

from gaokao_vault.config import DatabaseConfig
from gaokao_vault.spiders.provincial_announcement_spider import ProvincialAnnouncementSpider


def _make_spider() -> ProvincialAnnouncementSpider:
    db_config = DatabaseConfig(
        dsn="postgresql://test:test@localhost:5432/test_db",
        pool_min=1,
        pool_max=2,
    )
    return ProvincialAnnouncementSpider(db_config=db_config, crawl_task_id=1)


def _make_response(html: str, url: str, meta: dict | None = None) -> MagicMock:
    adaptor = Adaptor(content=html, url=url)
    response = MagicMock()
    response.status = 200
    response.url = url
    response.css = adaptor.css
    response.request = MagicMock()
    response.request.meta = meta or {}
    response.request.url = url
    response.urljoin = adaptor.urljoin
    return response


async def _collect(async_gen) -> list:
    items = []
    async for item in async_gen:
        items.append(item)
    return items


def test_start_requests_targets_jilin_official_announcement_lists() -> None:
    spider = _make_spider()

    requests = asyncio.run(_collect(spider.start_requests()))

    assert requests
    assert all(isinstance(request, Request) for request in requests)
    assert all("jleea.com.cn" in request.url for request in requests)
    assert all(request.meta["province_id"] == 7 for request in requests)
    assert all(request.meta["source_name"] == "吉林省教育考试院" for request in requests)


def test_parse_jilin_list_yields_official_detail_requests() -> None:
    spider = _make_spider()
    response = _make_response(
        """
        <html><body>
          <ul class="news-list">
            <li>
              <a href="/2025/0701/notice.html">吉林省2025年普通高校招生录取工作安排</a>
              <span>2025-07-01</span>
            </li>
            <li><a href="https://example.com/outside.html">外站链接</a></li>
          </ul>
        </body></html>
        """,
        "https://www.jleea.com.cn/ptgxzs/",
        {"province_id": 7, "source_name": "吉林省教育考试院"},
    )

    requests = asyncio.run(_collect(spider.parse(response)))

    assert len(requests) == 1
    assert requests[0].url == "https://www.jleea.com.cn/2025/0701/notice.html"
    assert requests[0].callback == spider.parse_detail
    assert requests[0].meta["province_id"] == 7
    assert requests[0].meta["title"] == "吉林省2025年普通高校招生录取工作安排"
    assert requests[0].meta["publish_date"] == "2025-07-01"


def test_parse_detail_persists_provincial_announcement() -> None:
    spider = _make_spider()
    response = _make_response(
        """
        <html><body>
          <h1>吉林省2025年普通高校招生录取工作安排</h1>
          <div class="date">2025-07-01</div>
          <div class="content">
            <p>现将2025年普通高校招生录取工作安排公告如下。</p>
            <p>本科批录取按规定时间进行。</p>
          </div>
        </body></html>
        """,
        "https://www.jleea.com.cn/2025/0701/notice.html",
        {
            "province_id": 7,
            "title": "吉林省2025年普通高校招生录取工作安排",
            "publish_date": "2025-07-01",
        },
    )

    with patch.object(spider, "process_item", new=AsyncMock(return_value="new")) as process_item:
        items = asyncio.run(_collect(spider.parse_detail(response)))

    assert items == [
        {
            "province_id": 7,
            "year": 2025,
            "title": "吉林省2025年普通高校招生录取工作安排",
            "content": "现将2025年普通高校招生录取工作安排公告如下。 本科批录取按规定时间进行。",
            "announcement_type": "admission",
            "publish_date": items[0]["publish_date"],
            "source_url": "https://www.jleea.com.cn/2025/0701/notice.html",
        }
    ]
    assert str(items[0]["publish_date"]) == "2025-07-01"
    process_item.assert_awaited_once()
