from __future__ import annotations

import logging
import re
from datetime import datetime
from html import unescape
from typing import ClassVar

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response

from gaokao_vault.constants import TaskType
from gaokao_vault.db.queries.scores import batch_upsert_score_segments
from gaokao_vault.models.score import ScoreSegmentItem
from gaokao_vault.pipeline.hasher import compute_content_hash
from gaokao_vault.pipeline.sink import BatchSink
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.base import BaseGaokaoSpider
from gaokao_vault.spiders.response_utils import response_text
from gaokao_vault.spiders.scope import iter_crawl_years, load_province_targets

logger = logging.getLogger(__name__)

YEAR_START = 2018
YEAR_END = datetime.now().year
EOL_SEGMENT_INDEX_URL = "https://www.eol.cn/e_html/gk/gkfsd/"
EOL_SEGMENT_YEAR_INDEX_URL_TEMPLATE = "https://www.eol.cn/e_html/gk/gkfsd/{year}.shtml"
EOL_DATA_SOURCE = "gaokao.eol.cn"
_SEGMENT_LINK_KEYWORDS = ("一分一段", "一分段", "成绩分段", "成绩分数段", "成绩分布", "分段表")
_SUBJECT_HINTS = ("物理类", "历史类", "理科", "文科", "综合", "艺术类", "体育类")


class ScoreSegmentSpider(BaseGaokaoSpider):
    """Crawl score segment tables (一分一段表). Uses BatchSink for large volumes."""

    name: str = "score_segment_spider"
    task_type: str = TaskType.SCORE_SEGMENTS
    allowed_domains: ClassVar[set[str]] = {"www.eol.cn", "gaokao.eol.cn"}

    concurrent_requests = 3
    download_delay = 2.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sink: BatchSink | None = None

    def configure_sessions(self, manager) -> None:
        manager.add(
            "http",
            FetcherSession(
                timeout=30,
                headers={
                    "Referer": "https://www.eol.cn/e_html/gk/gkfsd/",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            ),
        )

    async def on_start(self, resuming: bool = False):
        pool = await self._get_pool()
        self._sink = BatchSink(
            pool=pool,
            flush_fn=self._flush_batch,
            batch_size=500,
        )

    @staticmethod
    async def _flush_batch(conn, rows):
        return await batch_upsert_score_segments(conn, rows)

    async def start_requests(self):
        provinces = await load_province_targets(await self._get_pool())
        years = list(iter_crawl_years(mode=self.mode, full_start_year=YEAR_START, current_year=YEAR_END))
        province_meta = [
            {"id": province.id, "name": province.name, "code": province.url_value} for province in provinces
        ]

        index_urls = [EOL_SEGMENT_INDEX_URL]
        latest_index_year = _latest_eol_index_year()
        for year in years:
            if year < latest_index_year:
                index_urls.append(EOL_SEGMENT_YEAR_INDEX_URL_TEMPLATE.format(year=year))

        for url in dict.fromkeys(index_urls):
            yield Request(
                url,
                callback=self.parse_index,
                meta={"provinces": province_meta, "years": years},
            )

    async def parse_index(self, response: Response):
        if response.request is None:
            return

        meta = response.request.meta
        province_by_name = {province["name"]: province for province in meta.get("provinces") or []}
        allowed_years = set(meta.get("years") or [])

        for province_name, links in _index_blocks(response):
            province = province_by_name.get(province_name)
            if province is None:
                continue

            for href, title in links:
                if not href or "/e_html/gk/gkfsd/" in href:
                    continue
                if not _looks_like_segment_link(title):
                    continue

                year = _extract_year(f"{title} {href}")
                if year is None or year not in allowed_years:
                    continue

                yield Request(
                    href,
                    callback=self.parse,
                    meta={
                        "province_id": province["id"],
                        "province_name": province_name,
                        "province_code": province["code"],
                        "year": year,
                        "subject_hint": _extract_subject_hint(title),
                        "data_source": EOL_DATA_SOURCE,
                    },
                )

    async def parse(self, response: Response):
        if response.request is None:
            return
        province_id = response.request.meta.get("province_id")
        year = response.request.meta.get("year")

        if not province_id or not year:
            return

        parsed_eol_rows = 0
        async for item in self._parse_eol_article(response, province_id=province_id, year=year):
            parsed_eol_rows += 1
            yield item
        if parsed_eol_rows:
            return

        for row in response.css("table.segment-table tr"):
            cells = row.css("td")
            if len(cells) < 3:
                continue

            score_text = cells[0].css("::text").get("").strip()
            seg_text = cells[1].css("::text").get("").strip()
            cum_text = cells[2].css("::text").get("").strip()

            if not score_text or not score_text.isdigit():
                continue

            subject_text = cells[3].css("::text").get("").strip() if len(cells) > 3 else ""
            subject_category_id = await self._resolve_subject_category(subject_text)

            data = {
                "province_id": province_id,
                "year": year,
                "subject_category_id": subject_category_id,
                "score": int(score_text),
                "segment_count": int(seg_text) if seg_text.isdigit() else 0,
                "cumulative_count": int(cum_text) if cum_text.isdigit() else 0,
            }

            item = validate_item(ScoreSegmentItem, data)
            if item:
                yield item
                await self._add_to_sink(item)

    async def _parse_eol_article(self, response: Response, *, province_id: int, year: int):
        title = _node_text(response.css("div.title")).strip() or _node_text(response.css("title")).strip()
        meta = response.request.meta if response.request else {}
        subject_hint = meta.get("subject_hint") or _extract_subject_hint(title)
        subject_category_id = await self._resolve_subject_category(subject_hint or "")

        for table in _segment_tables(response):
            if not _looks_like_segment_table(table):
                continue
            for row in table.css("tr"):
                cells = row.css("td, th")
                if len(cells) < 3:
                    continue
                values = [_node_text(cell).strip() for cell in cells[:3]]
                if not values or "分数" in values[0]:
                    continue

                score = _parse_score(values[0])
                segment_count = _parse_count(values[1])
                cumulative_count = _parse_count(values[2])
                if score is None or segment_count is None or cumulative_count is None:
                    continue

                data = {
                    "province_id": province_id,
                    "year": year,
                    "subject_category_id": subject_category_id,
                    "score": score,
                    "segment_count": segment_count,
                    "cumulative_count": cumulative_count,
                }
                item = validate_item(ScoreSegmentItem, data)
                if item:
                    yield item
                    await self._add_to_sink(item)

    async def _add_to_sink(self, item: dict) -> None:
        item["content_hash"] = compute_content_hash(item)
        item["crawl_task_id"] = self.crawl_task_id
        if self._sink:
            before = self._sink.total_flushed
            await self._sink.add(item)
            self._stats["updated"] += self._sink.total_flushed - before

    async def on_close(self) -> None:
        if self._sink:
            before = self._sink.total_flushed
            await self._sink.flush()
            self._stats["updated"] += self._sink.total_flushed - before
        await super().on_close()


def _latest_eol_index_year() -> int:
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def _node_text(node) -> str:
    return "".join(part.strip() for part in node.css("::text").getall() if part.strip())


def _segment_tables(response: Response):
    editor_tables = response.css("div.TRS_Editor table")
    if editor_tables:
        return editor_tables
    return response.css("table")


def _index_blocks(response: Response) -> list[tuple[str, list[tuple[str, str]]]]:
    blocks = []
    for block in response.css("div.chengshi"):
        province_name = _node_text(block.css(".chengshi-head span")).strip()
        links = [(link.attrib.get("href", "").strip(), _node_text(link)) for link in block.css("a")]
        blocks.append((province_name, links))
    if blocks:
        return blocks
    return _index_blocks_from_html(response_text(response))


def _index_blocks_from_html(html: str) -> list[tuple[str, list[tuple[str, str]]]]:
    blocks = []
    for chunk in html.split('<div class="chengshi">')[1:]:
        province_match = re.search(r"<div class=\"chengshi-head\">.*?<span>(.*?)</span>", chunk, re.S)
        if province_match is None:
            continue
        province_name = _strip_tags(province_match.group(1))
        links = [
            (unescape(match.group(1).strip()), _strip_tags(match.group(2)))
            for match in re.finditer(r"<a\s+[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>", chunk, re.S)
        ]
        blocks.append((province_name, links))
    return blocks


def _strip_tags(value: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", value)).strip()


def _looks_like_segment_link(text: str) -> bool:
    return any(keyword in text for keyword in _SEGMENT_LINK_KEYWORDS)


def _looks_like_segment_table(table) -> bool:
    first_row_text = _node_text(table.css("tr")).strip()
    return "分数" in first_row_text and ("人数" in first_row_text or "累计" in first_row_text)


def _extract_year(text: str) -> int | None:
    match = re.search(r"20[12]\d", text)
    return int(match.group(0)) if match else None


def _extract_subject_hint(text: str) -> str | None:
    for hint in _SUBJECT_HINTS:
        if hint in text:
            return hint
    return None


def _parse_score(text: str) -> int | None:
    match = re.search(r"\d+", text.replace(",", ""))
    return int(match.group(0)) if match else None


def _parse_count(text: str) -> int | None:
    normalized = text.replace(",", "").strip()
    return int(normalized) if normalized.isdigit() else None
