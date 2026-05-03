from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urljoin

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response

from gaokao_vault.constants import TaskType
from gaokao_vault.db.queries.enrollment import upsert_timeline
from gaokao_vault.models.enrollment import TimelineItem
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.base import BaseGaokaoSpider

logger = logging.getLogger(__name__)

DXSBB_BASE_URL = "https://www.dxsbb.com"
DXSBB_TIMELINE_LIST_URL = f"{DXSBB_BASE_URL}/news/list_916.html"

PROVINCES = (
    (1, "北京"),
    (2, "天津"),
    (3, "河北"),
    (4, "山西"),
    (5, "内蒙古"),
    (6, "辽宁"),
    (7, "吉林"),
    (8, "黑龙江"),
    (9, "上海"),
    (10, "江苏"),
    (11, "浙江"),
    (12, "安徽"),
    (13, "福建"),
    (14, "江西"),
    (15, "山东"),
    (16, "河南"),
    (17, "湖北"),
    (18, "湖南"),
    (19, "广东"),
    (20, "广西"),
    (21, "海南"),
    (22, "重庆"),
    (23, "四川"),
    (24, "贵州"),
    (25, "云南"),
    (26, "西藏"),
    (27, "陕西"),
    (28, "甘肃"),
    (29, "青海"),
    (30, "宁夏"),
    (31, "新疆"),
)
PROVINCE_NAME_TO_ID = {name: province_id for province_id, name in PROVINCES}


class TimelineSpider(BaseGaokaoSpider):
    """Crawl volunteer fill timelines by province."""

    name: str = "timeline_spider"
    task_type: str = TaskType.TIMELINES
    allowed_domains = {"www.dxsbb.com", "dxsbb.com"}  # noqa: RUF012

    def configure_sessions(self, manager) -> None:
        manager.add("http", FetcherSession())

    async def start_requests(self):
        yield Request(DXSBB_TIMELINE_LIST_URL, callback=self.parse_dxsbb_list)

    async def parse(self, response: Response):
        if response.request is None:
            return
        province_id = response.request.meta.get("province_id")
        year = response.request.meta.get("year")

        if not province_id or not year:
            return

        for row in response.css("table.timeline-table tr"):
            cells = row.css("td")
            if len(cells) < 2:
                continue

            batch = cells[0].css("::text").get("").strip()
            if not batch:
                continue

            start_text = cells[1].css("::text").get("").strip() if len(cells) > 1 else ""
            end_text = cells[2].css("::text").get("").strip() if len(cells) > 2 else ""
            note_text = cells[3].css("::text").get("").strip() if len(cells) > 3 else None

            start_time = _parse_datetime(start_text)
            end_time = _parse_datetime(end_text)

            data = {
                "province_id": province_id,
                "year": year,
                "batch": batch,
                "start_time": start_time,
                "end_time": end_time,
                "note": note_text if note_text else None,
            }

            item = validate_item(TimelineItem, data)
            if item:
                yield item
                await self.process_item(
                    item,
                    entity_type="timelines",
                    unique_keys={
                        "province_id": province_id,
                        "year": year,
                        "batch": batch,
                    },
                    upsert_fn=upsert_timeline,
                )

    async def parse_dxsbb_list(self, response: Response):
        if response.status == 404:
            return

        seen_urls: set[str] = set()
        for link in response.css(".listBox a[href^='/news/'], .listBox2news a[href^='/news/']"):
            href = link.attrib.get("href", "").strip()
            title = _link_title(link)
            article_meta = _timeline_article_meta(title)
            if not href or article_meta is None:
                continue

            url = urljoin(DXSBB_BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            province_id, province_name, year = article_meta
            yield Request(
                url,
                callback=self.parse_dxsbb_article,
                meta={
                    "province_id": province_id,
                    "province_name": province_name,
                    "year": year,
                    "title": title,
                },
            )

        for link in response.css(".listNav a[href]"):
            href = link.attrib.get("href", "").strip()
            link_text = "".join(link.css("img::attr(alt), ::text").getall())
            if href and "下一页" in link_text:
                yield Request(urljoin(DXSBB_BASE_URL, href), callback=self.parse_dxsbb_list)

    async def parse_dxsbb_article(self, response: Response):
        if response.request is None or response.status == 404:
            return

        province_id = response.request.meta.get("province_id")
        year = response.request.meta.get("year")
        if not province_id or not year:
            return

        collection_mode = False
        for row in response.css("#article .content table tr"):
            cells = row.css("td, th")
            cell_texts = [_node_text(cell) for cell in cells]
            if len(cell_texts) < 2:
                if cell_texts:
                    collection_mode = "征集" in cell_texts[0]
                continue

            batch = cell_texts[0]
            time_text = cell_texts[1]
            if not batch or batch in {"批次", "类别", "科类", "填报阶段"} or "时段" in time_text:
                continue

            time_range = _parse_dxsbb_time_range(time_text, int(year))
            if time_range is None:
                continue

            start_time, end_time = time_range
            if collection_mode and "征集" not in batch:
                batch = f"{batch}(征集志愿)"

            data = {
                "province_id": province_id,
                "year": year,
                "batch": batch,
                "start_time": start_time,
                "end_time": end_time,
            }

            item = validate_item(TimelineItem, data)
            if item:
                yield item
                await self.process_item(
                    item,
                    entity_type="timelines",
                    unique_keys={
                        "province_id": province_id,
                        "year": year,
                        "batch": batch,
                    },
                    upsert_fn=upsert_timeline,
                )


def _parse_datetime(text: str) -> datetime | None:
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y年%m月%d日 %H:%M", "%Y年%m月%d日"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _link_title(link) -> str:
    for selector in ("h3::text", "img::attr(alt)"):
        value = link.css(selector).get()
        if value and value.strip():
            return value.strip()
    return " ".join(part.strip() for part in link.css("::text").getall() if part.strip())


def _timeline_article_meta(title: str) -> tuple[int, str, int] | None:
    if "高考志愿填报时间" not in title:
        return None

    year_match = re.search(r"(20\d{2})", title)
    if year_match is None:
        return None

    for province_name in sorted(PROVINCE_NAME_TO_ID.keys(), key=lambda value: len(value), reverse=True):
        if province_name in title:
            return PROVINCE_NAME_TO_ID[province_name], province_name, int(year_match.group(1))
    return None


def _node_text(node) -> str:
    return _clean_text("".join(node.css("::text").getall()))


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", "", text.replace("\xa0", " ").replace("\uff1a", ":")).strip()


def _parse_dxsbb_time_range(text: str, year: int) -> tuple[datetime, datetime] | None:
    value = _clean_text(text)
    match = re.search(
        r"(?:(?P<start_year>\d{4})年)?"
        r"(?P<start_month>\d{1,2})月(?P<start_day>\d{1,2})日?"
        r"(?P<start_hour>\d{1,2})(?::(?P<start_minute>\d{1,2}))?"
        r"(?:至|到|-|\u2014|\uff0d|~|\uff5e)"
        r"(?:(?P<end_year>\d{4})年)?"
        r"(?:(?P<end_month>\d{1,2})月(?:(?P<end_day>\d{1,2})日?)?)?"
        r"(?P<end_hour>\d{1,2})(?::(?P<end_minute>\d{1,2}))?",
        value,
    )
    if match is None:
        return None

    start_year = int(match.group("start_year") or year)
    start_month = int(match.group("start_month"))
    start_day = int(match.group("start_day"))
    end_year = int(match.group("end_year") or start_year)
    end_month = int(match.group("end_month") or start_month)
    end_day = int(match.group("end_day") or start_day)

    try:
        return (
            datetime(
                start_year,
                start_month,
                start_day,
                int(match.group("start_hour")),
                int(match.group("start_minute") or 0),
            ),
            datetime(
                end_year,
                end_month,
                end_day,
                int(match.group("end_hour")),
                int(match.group("end_minute") or 0),
            ),
        )
    except ValueError:
        return None
