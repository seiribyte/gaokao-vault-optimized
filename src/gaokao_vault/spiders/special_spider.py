from __future__ import annotations

import logging
import re
from datetime import date, datetime
from urllib.parse import urljoin

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response

from gaokao_vault.constants import BASE_URL, TaskType
from gaokao_vault.db.queries.schools import find_school_by_name
from gaokao_vault.db.queries.special import upsert_special_enrollment
from gaokao_vault.models.special import SpecialEnrollmentItem
from gaokao_vault.pipeline.quality import missing_field_flags
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.base import BaseGaokaoSpider

logger = logging.getLogger(__name__)

ENROLLMENT_TYPES = [
    "自主招生",
    "高校专项计划",
    "国家专项计划",
    "地方专项计划",
    "保送生",
    "艺术类",
    "体育类",
    "强基计划",
    "综合评价",
]

MAX_PAGES = 20
DXSBB_BASE_URL = "https://www.dxsbb.com"
DXSBB_SPECIAL_LISTS = [
    {
        "url": f"{DXSBB_BASE_URL}/news/list_130.html",
        "enrollment_type": "强基计划",
        "special_admission_type": "strong_foundation",
    },
    {
        "url": f"{DXSBB_BASE_URL}/news/list_976.html",
        "enrollment_type": "专项计划",
        "special_admission_type": "special_plan",
    },
]


class SpecialSpider(BaseGaokaoSpider):
    """Crawl special enrollment types."""

    name: str = "special_spider"
    task_type: str = TaskType.SPECIAL
    allowed_domains = {"www.dxsbb.com", "dxsbb.com"}  # noqa: RUF012

    def configure_sessions(self, manager) -> None:
        manager.add("http", FetcherSession())

    async def start_requests(self):
        for source in DXSBB_SPECIAL_LISTS:
            yield Request(
                source["url"],
                callback=self.parse_dxsbb_list,
                meta={
                    "enrollment_type": source["enrollment_type"],
                    "special_admission_type": source["special_admission_type"],
                },
            )

    async def parse(self, response: Response):
        if response.request is None:
            return
        etype = response.request.meta.get("enrollment_type")
        current_page = response.request.meta.get("page", 1)
        items_found = False

        for item_el in response.css("ul.news-list li"):
            items_found = True

            link = item_el.css("a")
            if not link:
                continue

            title = link.css("::text").get("").strip()
            href = link[0].attrib.get("href", "")
            date_text = item_el.css("span.date::text").get("").strip()
            year_text = item_el.css("span.year::text").get("").strip()

            if not title:
                continue

            publish_date = _parse_date(date_text)
            year = (
                int(year_text) if year_text.isdigit() else (publish_date.year if publish_date else datetime.now().year)
            )
            source_url = response.urljoin(href) if href else None

            data = {
                "enrollment_type": etype,
                "year": year,
                "title": title,
                "publish_date": publish_date,
                "source_url": source_url,
            }

            if href:
                yield Request(
                    response.urljoin(href),
                    callback=self.parse_detail,
                    meta={"item_data": data},
                )
            else:
                item = validate_item(SpecialEnrollmentItem, data)
                if item:
                    yield item
                    await self.process_item(
                        item,
                        entity_type="special_enrollments",
                        unique_keys={
                            "enrollment_type": etype,
                            "school_id": None,
                            "year": year,
                            "title": title,
                        },
                        upsert_fn=upsert_special_enrollment,
                    )

        if items_found and current_page < MAX_PAGES:
            next_page = current_page + 1
            url = f"{BASE_URL}/gkxx/tsbm/?type={etype}&page={next_page}"
            yield Request(
                url,
                callback=self.parse,
                meta={"enrollment_type": etype, "page": next_page},
            )

    async def parse_dxsbb_list(self, response: Response):
        if response.request is None or response.status == 404:
            return

        enrollment_type = response.request.meta.get("enrollment_type")
        special_admission_type = response.request.meta.get("special_admission_type")
        seen_urls: set[str] = set()

        for link in response.css(".listBox a[href^='/news/'], .listBox2news a[href^='/news/']"):
            href = link.attrib.get("href", "").strip()
            title = _link_title(link)
            if not href or not title or not _special_title_matches(title, str(enrollment_type or "")):
                continue

            url = urljoin(DXSBB_BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            yield Request(
                url,
                callback=self.parse_dxsbb_article,
                meta={
                    "enrollment_type": _infer_enrollment_type(title, str(enrollment_type or "")),
                    "special_admission_type": _infer_special_admission_type(title, str(special_admission_type or "")),
                    "title": title,
                },
            )

        for link in response.css(".listNav a[href]"):
            href = link.attrib.get("href", "").strip()
            link_text = "".join(link.css("img::attr(alt), ::text").getall())
            if href and "下一页" in link_text:
                yield Request(
                    urljoin(DXSBB_BASE_URL, href),
                    callback=self.parse_dxsbb_list,
                    meta={
                        "enrollment_type": enrollment_type,
                        "special_admission_type": special_admission_type,
                    },
                )

    async def parse_dxsbb_article(self, response: Response):
        if response.request is None or response.status == 404:
            return

        data = dict(response.request.meta)
        title = str(data.get("title") or _first_text(response, "#article h1::text") or "").strip()
        publish_date = _parse_dxsbb_publish_date(_first_text(response, "#article .update::text") or "")
        content_el = response.css("#article .content")
        content_text = ""
        if content_el:
            data["content"] = content_el.get("").strip()[:10000]
            content_text = "\n".join(part.strip() for part in content_el.css("::text").getall() if part.strip())

        data["title"] = title
        data["year"] = _extract_year(title) or (publish_date.year if publish_date else datetime.now().year)
        data["publish_date"] = publish_date
        data["source_url"] = response.url

        if data.get("enrollment_type") == "强基计划":
            data.update(_extract_strong_base_fields(content_text))

        data["quality_flags"] = missing_field_flags(
            data,
            ("application_url", "registration_window", "eligible_majors"),
        )

        item = validate_item(SpecialEnrollmentItem, data)
        if item:
            yield item
            await self.process_item(
                item,
                entity_type="special_enrollments",
                unique_keys={
                    "enrollment_type": data.get("enrollment_type"),
                    "school_id": data.get("school_id"),
                    "year": data.get("year"),
                    "title": data.get("title", ""),
                },
                upsert_fn=upsert_special_enrollment,
            )

    async def parse_detail(self, response: Response):
        if response.request is None:
            return
        data = response.request.meta.get("item_data", {})
        if data.get("school_id") is None:
            school_name = _extract_school_name_from_title(str(data.get("title") or ""))
            if school_name:
                pool = await self._get_pool()
                async with pool.acquire() as conn:
                    school = await find_school_by_name(conn, school_name)
                if school:
                    data["school_id"] = school["id"]

        content_el = response.css("div.article-content")
        content_text = ""
        if content_el:
            data["content"] = content_el.get("").strip()[:10000]
            content_text = "\n".join(part.strip() for part in content_el.css("::text").getall() if part.strip())

        if data.get("enrollment_type") == "强基计划":
            data.update(_extract_strong_base_fields(content_text))

        data["quality_flags"] = missing_field_flags(
            data,
            ("application_url", "registration_window", "eligible_majors"),
        )

        item = validate_item(SpecialEnrollmentItem, data)
        if item:
            yield item
            await self.process_item(
                item,
                entity_type="special_enrollments",
                unique_keys={
                    "enrollment_type": data.get("enrollment_type"),
                    "school_id": data.get("school_id"),
                    "year": data.get("year"),
                    "title": data.get("title", ""),
                },
                upsert_fn=upsert_special_enrollment,
            )


def _parse_date(text: str) -> date | None:
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y年%m月%d日", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_dxsbb_publish_date(text: str) -> date | None:
    match = re.search(r"(\d{4}-\d{1,2}-\d{1,2})", text)
    if match is None:
        return None
    return _parse_date(match.group(1))


def _extract_year(text: str) -> int | None:
    match = re.search(r"(20\d{2})", text)
    if match is None:
        return None
    return int(match.group(1))


def _link_title(link) -> str:
    for selector in ("h3::text", "img::attr(alt)"):
        value = link.css(selector).get()
        if value and value.strip():
            return value.strip()
    return " ".join(part.strip() for part in link.css("::text").getall() if part.strip())


def _first_text(response: Response, selector: str) -> str | None:
    value = response.css(selector).get()
    return value.strip() if value else None


def _special_title_matches(title: str, enrollment_type: str) -> bool:
    if enrollment_type == "强基计划":
        return "强基" in title
    if enrollment_type == "专项计划":
        return "专项" in title
    return enrollment_type in title


def _infer_enrollment_type(title: str, default: str) -> str:
    if "强基" in title:
        return "强基计划"
    if "高校专项" in title:
        return "高校专项计划"
    if "国家专项" in title:
        return "国家专项计划"
    if "地方专项" in title:
        return "地方专项计划"
    return default


def _infer_special_admission_type(title: str, default: str) -> str | None:
    if "强基" in title:
        return "strong_foundation"
    if "专项" in title:
        return "special_plan"
    return default or None


def _extract_school_name_from_title(title: str) -> str | None:
    match = re.match(
        r"(?P<school>.+?)(?:\d{4}年)?(?:强基计划|高校专项计划|综合评价|保送生|招生简章|招生章程)",
        title,
    )
    if match is None:
        return None
    value = match.group("school").strip()
    return value or None


def _extract_strong_base_fields(text: str) -> dict:
    registration_start, registration_end = _extract_registration_dates(text)
    shortlist_rule = _extract_labeled_sentence(text, "入围规则")
    school_assessment = _extract_labeled_sentence(text, "校测规则") or _extract_labeled_sentence(text, "学校考核")
    return {
        "special_admission_type": "strong_foundation",
        "application_url": _extract_application_url(text),
        "registration_window": _registration_window(registration_start, registration_end),
        "registration_start": registration_start,
        "registration_end": registration_end,
        "shortlist_rule": shortlist_rule,
        "selection_rule": shortlist_rule,
        "school_assessment": school_assessment,
        "school_exam_rule": school_assessment,
        "composite_score_formula": _extract_labeled_sentence(text, "综合成绩公式")
        or _extract_labeled_sentence(text, "综合成绩"),
        "admission_rule": _extract_labeled_sentence(text, "录取规则"),
        "eligible_majors": _extract_eligible_majors(text),
    }


def _extract_application_url(text: str) -> str | None:
    match = re.search(r"https://bm\.chsi\.com\.cn/[^\s<]+", text)
    if match is None:
        return None
    return match.group(0).rstrip(".")


def _extract_registration_dates(text: str) -> tuple[date | None, date | None]:
    match = re.search(
        r"报名时间\s*[:\uFF1A]\s*(\d{4})年(\d{1,2})月(\d{1,2})日?\s*至\s*(\d{4})年(\d{1,2})月(\d{1,2})日?",
        text,
    )
    if match is None:
        return None, None
    try:
        start = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        end = date(int(match.group(4)), int(match.group(5)), int(match.group(6)))
    except ValueError:
        return None, None
    return start, end


def _registration_window(registration_start: date | None, registration_end: date | None) -> dict[str, str | None]:
    if registration_start is None and registration_end is None:
        return {}
    return {
        "start": registration_start.isoformat() if registration_start else None,
        "end": registration_end.isoformat() if registration_end else None,
    }


def _extract_labeled_sentence(text: str, label: str) -> str | None:
    match = re.search(rf"{label}\s*[:\uFF1A]\s*([^\n\u3002.]+)", text)
    if match is None:
        return None
    value = match.group(1).strip()
    return value or None


def _extract_eligible_majors(text: str) -> list[str]:
    match = re.search(r"招生专业\s*[:\uFF1A]\s*([^\n\u3002.]+)", text)
    if match is None:
        return []
    return [part.strip() for part in re.split(r"[\u3001,\uFF0C;\uFF1B]", match.group(1)) if part.strip()]
