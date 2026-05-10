from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from urllib.parse import urljoin

from scrapling.spiders import Response

from gaokao_vault.config import AppConfig
from gaokao_vault.models.score import ScoreSegmentItem
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.dxsbb import DXSBB_BASE_URL
from gaokao_vault.vision.analyzer import VisionAnalyzer

logger = logging.getLogger(__name__)

DXSBB_SEGMENT_INDEX_URL = f"{DXSBB_BASE_URL}/news/list_223.html"
DXSBB_DATA_SOURCE = "dxsbb.com"

_SEGMENT_LINK_KEYWORDS = ("一分一段", "一分段", "成绩分段", "成绩分数段", "成绩分布", "分段表")
_SUBJECT_HINTS = ("物理类", "历史类", "理科", "文科", "综合", "艺术类", "体育类")

ResolveSubjectCategory = Callable[[str], Awaitable[int | None]]


@dataclass(frozen=True)
class DxsbbSegmentTarget:
    url: str
    is_index: bool
    meta: dict


@dataclass(frozen=True)
class DxsbbArticleContext:
    province_id: int
    province_name: str
    year: int
    subject_hint: str | None


@dataclass(frozen=True)
class DxsbbSegmentRecord:
    category: str
    score: int
    segment_count: int
    cumulative_count: int


class DxsbbScoreSegmentParser:
    """Parse DXSBB score segment index/article pages and image-backed tables."""

    def __init__(self, app_config: AppConfig | None) -> None:
        self._app_config = app_config

    def iter_index_targets(
        self,
        response: Response,
        *,
        provinces: list[dict],
        allowed_years: set[int],
        index_meta: dict,
    ) -> list[DxsbbSegmentTarget]:
        province_by_name = {province["name"]: province for province in provinces}
        seen_urls: set[str] = set()
        targets: list[DxsbbSegmentTarget] = []

        for link in response.css("a[href]"):
            href = link.attrib.get("href", "").strip()
            title = _node_text(link).strip()
            if not href or not title:
                continue

            province = _find_province_for_text(title, province_by_name)
            if province is None:
                continue

            url = urljoin(DXSBB_BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            if "/news/list_" in url:
                targets.append(DxsbbSegmentTarget(url=url, is_index=True, meta=index_meta))
                continue

            if not _looks_like_segment_link(title):
                continue

            year = _extract_year(f"{title} {href}")
            if year is None or year not in allowed_years:
                continue

            targets.append(
                DxsbbSegmentTarget(
                    url=url,
                    is_index=False,
                    meta={
                        "province_id": province["id"],
                        "province_name": province["name"],
                        "province_code": province["code"],
                        "year": year,
                        "subject_hint": _extract_subject_hint(title),
                        "data_source": DXSBB_DATA_SOURCE,
                        "title": title,
                    },
                )
            )

        return targets

    def article_context(self, response: Response) -> DxsbbArticleContext | None:
        if response.request is None:
            return None

        meta = response.request.meta
        province_id = meta.get("province_id")
        year = meta.get("year")
        if not province_id or not year:
            return None

        subject_hint = meta.get("subject_hint") or _extract_subject_hint(
            meta.get("title") or _node_text(response.css("#article h1, h1, title"))
        )
        return DxsbbArticleContext(
            province_id=province_id,
            province_name=meta.get("province_name") or "",
            year=year,
            subject_hint=subject_hint,
        )

    def table_records(self, response: Response, *, subject_hint: str | None) -> list[DxsbbSegmentRecord]:
        records: list[DxsbbSegmentRecord] = []
        for table in response.css("#article .content table, #article table, .content table"):
            records.extend(DxsbbSegmentTableParser(subject_hint=subject_hint).parse(table))
        return records

    def image_urls(self, response: Response) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for image in response.css("#article .content img[src], #article img[src], .content img[src]"):
            src = image.attrib.get("src", "").strip()
            alt = image.attrib.get("alt", "").strip()
            if not src:
                continue
            if not (_looks_like_segment_link(alt) or _looks_like_segment_link(src) or "uploads" in src):
                continue
            url = urljoin(DXSBB_BASE_URL, src)
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    async def analyze_segment_image(
        self,
        image_url: str,
        *,
        province_name: str,
        year: int,
        subject_hint: str | None,
    ) -> list[DxsbbSegmentRecord]:
        if self._app_config is None:
            logger.warning(
                "No OpenAI config available; skipping dxsbb image segment extraction for %s %d", province_name, year
            )
            return []

        prompt = _score_segment_prompt_template().format(
            province_name=province_name,
            year=year,
            subject_hint=subject_hint or "",
        )
        analyzer = VisionAnalyzer(self._app_config.openai)
        records = await analyzer.analyze_image_url(
            image_url,
            prompt=prompt,
            province_name=province_name,
            year=year,
        )
        return [
            parsed
            for record in records
            if record and (parsed := _record_from_mapping(record, fallback_category=subject_hint)) is not None
        ]

    async def build_item(
        self,
        record: DxsbbSegmentRecord,
        *,
        province_id: int,
        year: int,
        resolve_subject_category: ResolveSubjectCategory,
    ) -> dict | None:
        subject_category_id = await resolve_subject_category(record.category)
        return validate_item(
            ScoreSegmentItem,
            {
                "province_id": province_id,
                "year": year,
                "subject_category_id": subject_category_id,
                "score": record.score,
                "segment_count": record.segment_count,
                "cumulative_count": record.cumulative_count,
            },
        )


class DxsbbSegmentTableParser:
    """Small table parser with a narrow score-segment row API."""

    def __init__(self, *, subject_hint: str | None) -> None:
        self._subject_hint = subject_hint

    def parse(self, table) -> list[DxsbbSegmentRecord]:
        if not _looks_like_segment_table(table):
            return []

        header_map: dict[str, int] | None = None
        records: list[DxsbbSegmentRecord] = []
        for row in table.css("tr"):
            values = self._row_values(row)
            if len(values) < 3:
                continue

            if _looks_like_segment_header(values):
                header_map = {value: index for index, value in enumerate(values) if value}
                continue

            record = self._record_from_values(values, header_map)
            if record is not None:
                records.append(record)

        return records

    @staticmethod
    def _row_values(row) -> list[str]:
        return [_node_text(cell).strip() for cell in row.css("td, th")]

    def _record_from_values(
        self,
        values: list[str],
        header_map: dict[str, int] | None,
    ) -> DxsbbSegmentRecord | None:
        columns = SegmentColumns.from_header(header_map)
        score = _parse_score(_cell_text(values, columns.score))
        segment_count = _parse_count(_cell_text(values, columns.segment_count))
        cumulative_count = _parse_count(_cell_text(values, columns.cumulative_count))
        if score is None or segment_count is None or cumulative_count is None:
            return None

        category = _cell_text(values, columns.category) or self._subject_hint or ""
        return DxsbbSegmentRecord(
            category=category,
            score=score,
            segment_count=segment_count,
            cumulative_count=cumulative_count,
        )


@dataclass(frozen=True)
class SegmentColumns:
    score: int
    segment_count: int
    cumulative_count: int
    category: int

    @classmethod
    def from_header(cls, header_map: dict[str, int] | None) -> SegmentColumns:
        return cls(
            score=_column_index(header_map, ("分数", "分数段"), 0),
            segment_count=_column_index(header_map, ("本段人数", "人数", "同分人数"), 1),
            cumulative_count=_column_index(header_map, ("累计人数", "累计", "位次"), 2),
            category=_column_index(header_map, ("科类", "类别"), -1),
        )


def _record_from_mapping(record: dict, *, fallback_category: str | None) -> DxsbbSegmentRecord | None:
    score = _parse_score(str(record.get("score") or ""))
    segment_count = _parse_count(str(record.get("segment_count") or ""))
    cumulative_count = _parse_count(str(record.get("cumulative_count") or ""))
    if score is None or segment_count is None or cumulative_count is None:
        return None

    return DxsbbSegmentRecord(
        category=str(record.get("category") or fallback_category or ""),
        score=score,
        segment_count=segment_count,
        cumulative_count=cumulative_count,
    )


@cache
def _score_segment_prompt_template() -> str:
    prompt_path = Path(__file__).parents[1] / "vision" / "prompts" / "score_segment_extract.txt"
    return prompt_path.read_text(encoding="utf-8")


def _node_text(node) -> str:
    return "".join(part.strip() for part in node.css("::text").getall() if part.strip())


def _find_province_for_text(text: str, province_by_name: dict[str, dict]) -> dict | None:
    matches = [province for name, province in province_by_name.items() if name and name in text]
    return matches[0] if matches else None


def _looks_like_segment_link(text: str) -> bool:
    return any(keyword in text for keyword in _SEGMENT_LINK_KEYWORDS)


def _looks_like_segment_table(table) -> bool:
    first_row_text = _node_text(table.css("tr")).strip()
    return "分数" in first_row_text and ("人数" in first_row_text or "累计" in first_row_text)


def _looks_like_segment_header(values: list[str]) -> bool:
    joined = "|".join(values)
    return "分数" in joined and ("人数" in joined or "累计" in joined or "位次" in joined)


def _column_index(header_map: dict[str, int] | None, names: tuple[str, ...], default: int) -> int:
    if header_map is None:
        return default
    for name in names:
        for header, index in header_map.items():
            if name in header:
                return index
    return default


def _cell_text(values: list[str], index: int) -> str:
    if index < 0 or index >= len(values):
        return ""
    return values[index]


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
