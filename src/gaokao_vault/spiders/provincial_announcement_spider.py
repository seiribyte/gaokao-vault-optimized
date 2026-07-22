from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar
from urllib.parse import urlparse

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response

from gaokao_vault.constants import TaskType
from gaokao_vault.db.queries.enrollment import upsert_provincial_announcement
from gaokao_vault.models.enrollment import ProvincialAnnouncementItem
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.base import BaseGaokaoSpider


@dataclass(frozen=True, slots=True)
class ProvincialAnnouncementSource:
    province_id: int
    source_name: str
    list_urls: tuple[str, ...]
    allowed_domains: frozenset[str]


PROVINCIAL_ANNOUNCEMENT_SOURCES = {
    "jilin": ProvincialAnnouncementSource(
        province_id=7,
        source_name="吉林省教育考试院",
        list_urls=(
            "https://www.jleea.com.cn/ptgxzs/",
            "https://www.jleea.com.cn/ptgxzs/zhxx/",
        ),
        allowed_domains=frozenset({"www.jleea.com.cn", "jleea.com.cn"}),
    ),
}


class ProvincialAnnouncementSpider(BaseGaokaoSpider):
    """Crawl official provincial exam authority announcements."""

    name: str = "provincial_announcement_spider"
    task_type: str = TaskType.PROVINCIAL_ANNOUNCEMENTS
    allowed_domains: ClassVar[set[str]] = {
        domain for source in PROVINCIAL_ANNOUNCEMENT_SOURCES.values() for domain in source.allowed_domains
    }

    def configure_sessions(self, manager) -> None:
        manager.add("http", FetcherSession())
        self._add_stealth_session(manager)

    async def start_requests(self):
        for source_key, source in PROVINCIAL_ANNOUNCEMENT_SOURCES.items():
            for url in source.list_urls:
                yield Request(
                    url,
                    callback=self.parse,
                    meta={
                        "province_id": source.province_id,
                        "source_name": source.source_name,
                        "source_key": source_key,
                        "allowed_domains": source.allowed_domains,
                    },
                )

    async def parse(self, response: Response):
        if response.status == 404 or response.request is None:
            return

        province_id = response.request.meta.get("province_id")
        if not province_id:
            return
        allowed_domains = frozenset(response.request.meta.get("allowed_domains") or self.allowed_domains)

        seen: set[str] = set()
        for link in response.css("a[href]"):
            title = _clean_text(" ".join(link.css("::text").getall()))
            href = link.attrib.get("href", "").strip()
            if not title or not href or not _looks_like_announcement(title):
                continue

            detail_url = response.urljoin(href)
            if not _is_allowed_source_url(detail_url, allowed_domains) or detail_url in seen:
                continue
            seen.add(detail_url)

            yield Request(
                detail_url,
                callback=self.parse_detail,
                meta={
                    "province_id": province_id,
                    "source_key": response.request.meta.get("source_key"),
                    "source_name": response.request.meta.get("source_name"),
                    "allowed_domains": allowed_domains,
                    "title": title,
                    "publish_date": _extract_date_near_link(link),
                },
            )

    async def parse_detail(self, response: Response):
        if response.status == 404 or response.request is None:
            return

        meta = response.request.meta
        province_id = meta.get("province_id")
        if not province_id:
            return

        title = _clean_text(str(meta.get("title") or "")) or _extract_title(response)
        content = _extract_content(response)
        if not title or not content:
            return

        publish_date = _parse_date(str(meta.get("publish_date") or "")) or _extract_publish_date(response)
        year = publish_date.year if publish_date else _infer_year(title)
        data = {
            "province_id": province_id,
            "year": year,
            "title": title,
            "content": content,
            "announcement_type": _classify_announcement(title, content),
            "publish_date": publish_date,
            "source_url": response.url,
        }

        item = validate_item(ProvincialAnnouncementItem, data)
        if item:
            yield item
            await self.process_item(
                item,
                entity_type="provincial_announcements",
                unique_keys={
                    "province_id": province_id,
                    "title": title,
                    "source_url": response.url,
                },
                upsert_fn=upsert_provincial_announcement,
            )


def _clean_text(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _looks_like_announcement(title: str) -> bool:
    keywords = ("高考", "普通高校", "招生", "录取", "志愿", "考试", "公告", "通知", "安排")
    return any(keyword in title for keyword in keywords)


def _is_allowed_source_url(url: str, allowed_domains: frozenset[str]) -> bool:
    host = urlparse(url).netloc.lower()
    return host in allowed_domains


def _extract_date_near_link(link) -> str:
    texts = [link.attrib.get("title", "")]
    parent = link.parent
    if parent is not None:
        texts.extend(parent.css("::text").getall())
    value = " ".join(texts)
    match = re.search(r"20\d{2}[-./年]\d{1,2}[-./月]\d{1,2}", value)
    return match.group(0) if match else ""


def _extract_title(response: Response) -> str:
    for selector in ("h1::text", ".article-title::text", ".title::text", "title::text"):
        title = _clean_text(response.css(selector).get(""))
        if title:
            return title
    return ""


def _extract_content(response: Response) -> str:
    for selector in ("div.content", ".article-content", ".article", "#article", ".TRS_Editor", ".main"):
        content_el = response.css(selector)
        if not content_el:
            continue
        text = _clean_text(" ".join(content_el[0].css("::text").getall()))
        if text:
            return text[:20000]
    return ""


def _parse_date(text: str) -> date | None:
    value = _clean_text(text)
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y年%m月%d日"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _extract_publish_date(response: Response) -> date | None:
    text = _clean_text(" ".join(response.css(".date::text, .time::text, .source::text, .info::text").getall()))
    match = re.search(r"20\d{2}[-./年]\d{1,2}[-./月]\d{1,2}", text)
    return _parse_date(match.group(0)) if match else None


def _infer_year(title: str) -> int | None:
    match = re.search(r"20\d{2}", title)
    return int(match.group(0)) if match else None


def _classify_announcement(title: str, content: str) -> str:
    text = f"{title} {content}"
    if "录取" in text:
        return "admission"
    if "志愿" in text:
        return "volunteer"
    if "招生" in text:
        return "enrollment"
    if "考试" in text:
        return "exam"
    return "notice"
