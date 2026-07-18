from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, ClassVar

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response

from gaokao_vault.constants import TaskType
from gaokao_vault.db.queries.enrollment import upsert_enrollment_plan
from gaokao_vault.db.queries.majors import find_majors_by_name
from gaokao_vault.models.enrollment import EnrollmentPlanItem
from gaokao_vault.pipeline.admission_rules import (
    extract_adjustment_rule,
    extract_eligibility_requirements,
    extract_physical_exam_limit,
    extract_physical_exam_or_political_review,
    extract_political_review_requirement,
    extract_program_type,
    extract_service_obligation,
    extract_single_subject_limit,
)
from gaokao_vault.pipeline.batch_normalizer import normalize_batch
from gaokao_vault.pipeline.quality import missing_field_flags
from gaokao_vault.pipeline.validator import validate_item
from gaokao_vault.spiders.base import BaseGaokaoSpider
from gaokao_vault.spiders.response_utils import response_json
from gaokao_vault.spiders.scope import load_province_targets
from gaokao_vault.spiders.table_candidates import candidate_tables

logger = logging.getLogger(__name__)

YEAR_START = 2020
DATA_SOURCE = "gaokao.cn"
CHSI_DATA_SOURCE = "gaokao.chsi.com.cn"
GAOKAO_STATIC_BASE_URL = "https://static-data.gaokao.cn"
GAOKAO_WEB_ORIGIN = "https://www.gaokao.cn"
PLAN_API_RATE_LIMIT_CODE = "1069"
PLAN_API_BACKOFF_SECONDS = (60.0, 180.0, 540.0, 900.0)
PLAN_API_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
SCHOOL_NAME_INDEX_URL = f"{GAOKAO_STATIC_BASE_URL}/www/2.0/school/name.json"
# Correct dictionary path (legacy /yk/school/... is 404).
PLAN_DICTIONARY_URL_TEMPLATE = f"{GAOKAO_STATIC_BASE_URL}/www/2.0/school/{{school_id}}/dic/specialplan.json"
# Static plan JSON keys are currently 404; zjzw API is the live data source.
PLAN_URL_TEMPLATE = (
    "https://api.zjzw.cn/web/api?uri=apidata/api/gkv3/plan/school"
    "&school_id={school_id}&year={year}&local_province_id={province}"
    "&page=1&size=20&special_group=&local_batch_id=&local_type_id=&keyword="
)
PLAN_API_PAGE_URL_TEMPLATE = (
    "https://api.zjzw.cn/web/api?uri=apidata/api/gkv3/plan/school"
    "&school_id={school_id}&year={year}&local_province_id={province}"
    "&page={page}&size=20&special_group=&local_batch_id=&local_type_id=&keyword="
)
_GAOKAO_TYPE_NAMES = {
    "1": "理科",
    "2": "文科",
    "3": "综合",
    "4": "艺术类",
    "5": "体育类",
    "2073": "物理类",
    "2074": "历史类",
    "2292": "艺术类(历史)",
    "2293": "艺术类(物理)",
    "2294": "体育类(历史)",
    "2295": "体育类(物理)",
}
_PLAN_TABLE_HEADERS = (
    "院校代码",
    "学校代码",
    "专业名称",
    "专业",
    "科类",
    "选科",
    "批次",
    "计划数",
    "学制",
    "学费",
    "备注",
    "说明",
    "院校专业组",
    "专业组",
    "专业组代码",
    "专业代码",
    "选科要求",
    "再选科目",
    "校区",
    "办学地点",
    "就读地点",
)
_SCHOOL_NAME_ALIASES = {
    "复旦大学医学院": "复旦大学上海医学院",
    "山东大学威海分校": "山东大学(威海)",
    "电子科技大学(沙河校区)": "电子科技大学",
    "西南大学(荣昌校区)": "西南大学",
}


class EnrollmentPlanSpider(BaseGaokaoSpider):
    """Crawl enrollment plans: school x province x year."""

    name: str = "enrollment_plan_spider"
    task_type: str = TaskType.ENROLLMENT_PLANS

    allowed_domains: ClassVar[set[str]] = {"static-data.gaokao.cn", "api.zjzw.cn"}
    concurrent_requests = 8
    concurrent_requests_per_domain = 4
    download_delay = 0.2
    max_blocked_retries = 6

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.concurrent_requests = min(self.concurrent_requests, 2)
        self.concurrent_requests_per_domain = 1
        self.download_delay = max(self.download_delay, 1.5)
        self._plan_api_cooldown_until = 0.0
        self._consecutive_plan_api_limits = 0

    def configure_sessions(self, manager) -> None:
        manager.add(
            "http",
            FetcherSession(
                timeout=30,
                impersonate="chrome",
                headers={
                    "Referer": f"{GAOKAO_WEB_ORIGIN}/",
                    "Origin": GAOKAO_WEB_ORIGIN,
                    "Accept": "application/json,text/plain,*/*",
                    "User-Agent": PLAN_API_USER_AGENT,
                },
            ),
        )

    async def is_blocked(self, response: Response) -> bool:
        if str(response.url).startswith("https://api.zjzw.cn/"):
            payload = response_json(response)
            if payload is not None:
                code = str(payload.get("code", ""))
                if code == "0000":
                    self._consecutive_plan_api_limits = 0
                    return False
                if code == PLAN_API_RATE_LIMIT_CODE:
                    return True
        return await super().is_blocked(response)

    async def retry_blocked_request(self, request: Request, response: Response) -> Request:
        request.sid = "http"
        self._consecutive_plan_api_limits += 1
        retry_count = self._consecutive_plan_api_limits
        backoff = PLAN_API_BACKOFF_SECONDS[min(retry_count - 1, len(PLAN_API_BACKOFF_SECONDS) - 1)]
        loop = asyncio.get_running_loop()
        now = loop.time()
        self._plan_api_cooldown_until = max(self._plan_api_cooldown_until, now + backoff)
        cooldown = self._plan_api_cooldown_until - now
        logger.warning(
            "招生计划 API 触发限流 url=%s status=%s retry=%d backoff=%.0fs",
            request.url,
            response.status,
            retry_count,
            cooldown,
        )
        await asyncio.sleep(cooldown)
        return request

    async def start_requests(self):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, sch_id, name FROM schools ORDER BY id")

        provinces = await load_province_targets(pool, self._crawl_config.target_provinces)
        years = _select_plan_years(
            self.mode,
            datetime.now(),
            target_start_year=self._crawl_config.target_year_start,
            target_end_year=self._crawl_config.target_year_end,
        )
        schools = [{"id": int(row["id"]), "sch_id": int(row["sch_id"]), "name": str(row["name"])} for row in rows]
        province_meta = [
            {"id": province.id, "name": province.name, "code": province.url_value} for province in provinces
        ]

        yield Request(
            SCHOOL_NAME_INDEX_URL,
            callback=self.parse_school_name_index,
            meta={"schools": schools, "provinces": province_meta, "years": years},
        )

    async def parse_school_name_index(self, response: Response):
        if response.status == 404 or response.request is None:
            return

        result = response_json(response)
        if result is None or result.get("code") != "0000":
            logger.debug("Invalid gaokao school name index url=%s", response.url)
            return

        school_index = _build_gaokao_school_index(result.get("data"))
        provinces = response.request.meta.get("provinces") or []
        years = response.request.meta.get("years") or []
        for school in response.request.meta.get("schools") or []:
            school_name = _safe_text(school.get("name"))
            lookup_name = _SCHOOL_NAME_ALIASES.get(school_name or "", school_name or "")
            gaokao_school_id = school_index.get(_normalize_school_name(lookup_name))
            if not gaokao_school_id:
                logger.debug("Skipping enrollment plan for unmatched school=%s", school_name)
                continue

            base_meta = {
                "school_id": school["id"],
                "school_name": school_name,
                "gaokao_school_id": gaokao_school_id,
                "provinces": provinces,
                "years": years,
            }
            if len(years) == 1:
                for province in provinces:
                    yield Request(
                        PLAN_URL_TEMPLATE.format(
                            school_id=gaokao_school_id,
                            year=years[0],
                            province=province["code"],
                        ),
                        callback=self.parse,
                        meta={
                            **base_meta,
                            "province_id": province["id"],
                            "province_code": province["code"],
                            "year": years[0],
                            "page": 1,
                        },
                    )
            else:
                yield Request(
                    PLAN_DICTIONARY_URL_TEMPLATE.format(school_id=gaokao_school_id),
                    callback=self.parse_plan_dictionary,
                    meta=base_meta,
                )

    async def parse_plan_dictionary(self, response: Response):
        if response.status == 404 or response.request is None:
            return

        result = response_json(response)
        if result is None or result.get("code") != "0000":
            logger.debug("Invalid gaokao plan dictionary url=%s", response.url)
            return

        data = result.get("data")
        if not isinstance(data, dict):
            return

        # Live dictionary shape: data.newsdata.year = {province_code: [years...]}
        newsdata = data.get("newsdata") if isinstance(data.get("newsdata"), dict) else {}
        available_years_by_province = newsdata.get("year") if isinstance(newsdata, dict) else None
        if not isinstance(available_years_by_province, dict):
            available_years_by_province = data.get("year") if isinstance(data.get("year"), dict) else {}
        if not isinstance(available_years_by_province, dict):
            available_years_by_province = {}

        gaokao_school_id = response.request.meta.get("gaokao_school_id")
        allowed_years = _normalize_year_list(response.request.meta.get("years") or [])

        for province in response.request.meta.get("provinces") or []:
            province_code = str(province.get("code") or "").strip()
            available_years = {_safe_int(year) for year in available_years_by_province.get(province_code, [])}
            # If dictionary has province years, only request those; otherwise fall back to allowed_years.
            years_to_fetch = (
                sorted(year for year in available_years if year is not None and year in set(allowed_years))
                if available_years
                else allowed_years
            )
            for year in years_to_fetch:
                yield Request(
                    PLAN_URL_TEMPLATE.format(school_id=gaokao_school_id, year=year, province=province_code),
                    callback=self.parse,
                    meta={
                        "school_id": response.request.meta.get("school_id"),
                        "school_name": response.request.meta.get("school_name"),
                        "gaokao_school_id": gaokao_school_id,
                        "province_id": province.get("id"),
                        "province_code": province_code,
                        "year": year,
                        "page": 1,
                    },
                )

    async def parse(self, response: Response):
        if response.request is None:
            return
        school_id = response.request.meta.get("school_id")
        province_id = response.request.meta.get("province_id")
        year = response.request.meta.get("year")

        if not school_id or not province_id or not year:
            return

        result = response_json(response)
        if result is not None:
            async for item in self._parse_static_plan_json(response, result):
                yield item

            # Paginate zjzw API responses when more pages remain.
            data = result.get("data") if isinstance(result.get("data"), dict) else {}
            items = data.get("item") if isinstance(data, dict) else None
            num_found = _safe_int(data.get("numFound")) if isinstance(data, dict) else None
            page = _safe_int(response.request.meta.get("page")) or 1
            gaokao_school_id = response.request.meta.get("gaokao_school_id")
            province_code = response.request.meta.get("province_code")
            if (
                gaokao_school_id
                and province_code
                and isinstance(items, list)
                and num_found is not None
                and page * 20 < num_found
            ):
                next_page = page + 1
                yield Request(
                    PLAN_API_PAGE_URL_TEMPLATE.format(
                        school_id=gaokao_school_id,
                        year=year,
                        province=province_code,
                        page=next_page,
                    ),
                    callback=self.parse,
                    meta={
                        **dict(response.request.meta),
                        "page": next_page,
                    },
                )
            return

        async for item in self._parse_html_plan(response):
            yield item

    async def _parse_html_plan(self, response: Response):
        async with (await self._get_pool()).acquire() as conn:
            for table in candidate_tables(response, "plan-table", _PLAN_TABLE_HEADERS):
                header_map: dict[str, int] | None = None
                for row in table.css("tr"):
                    headers = [
                        "".join(part.strip() for part in cell.css("::text").getall() if part.strip())
                        for cell in row.css("th")
                    ]
                    if headers:
                        header_map = {text: idx for idx, text in enumerate(headers)}
                        continue

                    cells = row.css("td")
                    if len(cells) < 3:
                        continue

                    data = await self._build_html_plan_item(conn, response, header_map, cells)
                    if data is None:
                        continue
                    item = validate_item(EnrollmentPlanItem, data)
                    if item:
                        yield item
                        await self._persist_item(item)

    async def _build_html_plan_item(self, conn, response: Response, header_map: dict[str, int] | None, cells):
        if response.request is None:
            return None

        major_name = _cell_text(cells, _column_index(header_map, ("专业名称", "专业"), 0))
        if not major_name:
            return None

        subject_category_raw = _cell_text(cells, _column_index(header_map, ("科类", "选科"), 1))
        batch = _cell_text(cells, _column_index(header_map, ("批次",), 2))
        plan_text = _cell_text(cells, _column_index(header_map, ("计划数",), 3))
        duration = _cell_text(cells, _column_index(header_map, ("学制",), 4))
        tuition = _cell_text(cells, _column_index(header_map, ("学费",), 5))
        note = _cell_text(cells, _column_index(header_map, ("备注", "说明"), 6))
        major_group_code = _cell_text(cells, _column_index(header_map, ("院校专业组", "专业组", "专业组代码"), -1))
        major_code_raw = _cell_text(cells, _column_index(header_map, ("专业代码",), -1))
        selection_requirement = _cell_text(cells, _column_index(header_map, ("选科要求", "再选科目"), -1))
        campus = _cell_text(cells, _column_index(header_map, ("校区",), -1))
        education_location = _cell_text(cells, _column_index(header_map, ("办学地点", "就读地点"), -1))

        major_id = await _resolve_major_id(conn, major_name)
        subject_category_id = await self._resolve_subject_category(subject_category_raw or "")
        batch_info = normalize_batch(batch)
        data = {
            "school_id": response.request.meta.get("school_id"),
            "school_code_raw": _cell_text(cells, _column_index(header_map, ("院校代码", "学校代码"), -1)),
            "province_id": response.request.meta.get("province_id"),
            "year": response.request.meta.get("year"),
            "subject_category_id": subject_category_id,
            "batch": batch,
            "batch_code": batch_info.code,
            "batch_category": batch_info.category,
            "batch_segment": batch_info.segment,
            "major_name": major_name,
            "major_id": major_id,
            "plan_count": int(plan_text) if plan_text and plan_text.isdigit() else None,
            "duration": duration,
            "tuition": tuition,
            "note": note,
            "major_group_code": major_group_code,
            "major_code_raw": major_code_raw,
            "campus": campus,
            "education_location": education_location,
            "selection_requirement": selection_requirement,
            "physical_exam_limit": extract_physical_exam_limit(note),
            "single_subject_limit": extract_single_subject_limit(note),
            "adjustment_rule": extract_adjustment_rule(note),
            "program_type": extract_program_type(batch, note),
            "eligibility_requirements": extract_eligibility_requirements(note),
            "physical_exam_or_political_review": extract_physical_exam_or_political_review(note),
            "political_review_requirement": extract_political_review_requirement(note),
            "service_obligation": extract_service_obligation(note),
            "data_source": CHSI_DATA_SOURCE,
            "source_url": response.url,
        }
        data["quality_flags"] = missing_field_flags(data, ("major_id", "plan_count", "selection_requirement"))
        return data

    async def _parse_static_plan_json(self, response: Response, result: dict[str, Any]):
        if result.get("code") != "0000":
            return

        records = _extract_plan_records(result.get("data"))

        async with (await self._get_pool()).acquire() as conn:
            for record in records:
                if not isinstance(record, dict):
                    continue
                item_data = await self._build_static_plan_item(conn, response, record)
                if item_data is None:
                    continue
                item = validate_item(EnrollmentPlanItem, item_data)
                if item:
                    yield item
                    await self._persist_item(item)

    async def _build_static_plan_item(self, conn, response: Response, record: dict[str, Any]) -> dict[str, Any] | None:
        if response.request is None:
            return None

        school_id = response.request.meta.get("school_id")
        province_id = response.request.meta.get("province_id")
        year = response.request.meta.get("year")
        if not school_id or not province_id or not year:
            return None

        major_name = _first_text(record.get("spname"), record.get("sp_name"))
        if not major_name:
            return None

        major_lookup_name = _first_text(record.get("sp_name"), major_name)
        major_id = await _resolve_major_id(conn, major_lookup_name) if major_lookup_name else None
        subject_category_raw = _gaokao_subject_category(record)
        subject_category_id = await self._resolve_subject_category(subject_category_raw or "")
        batch = _first_text(record.get("local_batch_name"), record.get("batch"))
        batch_info = normalize_batch(batch)
        note = _join_note(record.get("remark"), record.get("info"))
        selection_requirement = _first_text(record.get("sg_info"), record.get("sp_info"), record.get("sp_xuanke"))

        item_data = {
            "school_id": school_id,
            "school_code_raw": _first_text(
                record.get("school_code"),
                record.get("school_code_raw"),
                record.get("local_school_code"),
            ),
            "province_id": province_id,
            "year": year,
            "subject_category_id": subject_category_id,
            "batch": batch,
            "batch_code": batch_info.code,
            "batch_category": batch_info.category,
            "batch_segment": batch_info.segment,
            "major_name": major_name,
            "major_id": major_id,
            "plan_count": _safe_int(record.get("num")),
            "duration": _first_text(record.get("length")),
            "tuition": _first_text(record.get("tuition")),
            "note": note,
            "major_group_code": _first_text(record.get("sg_name"), record.get("special_group")),
            "major_code_raw": _first_text(record.get("spcode")),
            "campus": _first_text(record.get("campus"), record.get("school_area")),
            "education_location": _first_text(record.get("address"), record.get("place")),
            "selection_requirement": selection_requirement,
            "physical_exam_limit": extract_physical_exam_limit(note),
            "single_subject_limit": extract_single_subject_limit(note),
            "adjustment_rule": extract_adjustment_rule(note),
            "program_type": extract_program_type(batch, note, _first_text(record.get("zslx_name"))),
            "eligibility_requirements": extract_eligibility_requirements(note),
            "physical_exam_or_political_review": extract_physical_exam_or_political_review(note),
            "political_review_requirement": extract_political_review_requirement(note),
            "service_obligation": extract_service_obligation(note),
            "data_source": DATA_SOURCE,
            "source_url": response.url,
        }
        item_data["quality_flags"] = missing_field_flags(
            item_data,
            ("major_id", "plan_count", "selection_requirement"),
        )
        return item_data

    async def _persist_item(self, item: dict[str, Any]) -> None:
        await self.process_item(
            item,
            entity_type="enrollment_plans",
            unique_keys={
                "school_id": item["school_id"],
                "province_id": item["province_id"],
                "year": item["year"],
                "subject_category_id": item.get("subject_category_id"),
                "batch": item.get("batch"),
                "major_name": item.get("major_name"),
            },
            upsert_fn=upsert_enrollment_plan,
        )


def _column_index(header_map: dict[str, int] | None, candidates: tuple[str, ...], default: int) -> int:
    if header_map is None:
        return default
    for candidate in candidates:
        if candidate in header_map:
            return header_map[candidate]
    return default


def _extract_plan_records(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    if isinstance(data.get("item"), list):
        return data["item"]

    records: list[Any] = []
    for group in data.values():
        if isinstance(group, dict) and isinstance(group.get("item"), list):
            records.extend(group["item"])
    return records


def _cell_text(cells, index: int) -> str | None:
    if index < 0:
        return None
    if index >= len(cells):
        return None
    text = cells[index].css("::text").get("").strip()
    return text or None


def _build_gaokao_school_index(rows: Any) -> dict[str, str]:
    if not isinstance(rows, list):
        return {}

    school_index: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        school_id = _first_text(row.get("school_id"))
        if not school_id:
            continue
        for name_key in ("name", "old_name"):
            raw_name = _first_text(row.get(name_key)) or ""
            names = [raw_name]
            if raw_name.startswith("中国人民解放军"):
                names.append(raw_name.removeprefix("中国人民解放军"))
            for candidate in names:
                name = _normalize_school_name(candidate)
                if name and name not in school_index:
                    school_index[name] = school_id
    return school_index


def _normalize_school_name(name: str) -> str:
    return "".join(name.split()).translate(str.maketrans({"(": chr(0xFF08), ")": chr(0xFF09)}))


def _gaokao_subject_category(record: dict[str, Any]) -> str | None:
    type_code = _first_text(record.get("type"))
    if type_code and type_code in _GAOKAO_TYPE_NAMES:
        return _GAOKAO_TYPE_NAMES[type_code]
    return _first_text(record.get("local_type_name"), record.get("type"))


def _join_note(*values: Any) -> str | None:
    parts = [_safe_text(value) for value in values]
    text = "".join(part for part in parts if part)
    return text or None


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = _safe_text(value)
        if text:
            return text
    return None


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _resolve_major_id(conn, major_name: str) -> int | None:
    rows = await find_majors_by_name(conn, major_name)
    if len(rows) == 1:
        return rows[0]["id"]
    return None


def _select_plan_years(
    mode: str,
    now: datetime,
    *,
    target_start_year: int | None = None,
    target_end_year: int | None = None,
) -> list[int]:
    if mode != "incremental":
        years = list(range(YEAR_START, now.year + 1))
    elif now.month == 12:
        years = [now.year, now.year - 1, now.year - 2]
    else:
        years = [now.year - 1, now.year - 2, now.year - 3]

    return [
        year
        for year in years
        if year >= YEAR_START
        and (target_start_year is None or year >= target_start_year)
        and (target_end_year is None or year <= target_end_year)
    ]


def _normalize_year_list(years: list[int | str | None]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for year in years:
        value = _safe_int(year)
        if value is None or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized
