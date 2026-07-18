from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass

_INCREMENTAL_YEAR_WINDOW = 3


@dataclass(frozen=True, slots=True)
class ProvinceTarget:
    id: int
    name: str
    url_value: str


def iter_crawl_years(
    *,
    mode: str,
    full_start_year: int,
    current_year: int,
    target_start_year: int | None = None,
    target_end_year: int | None = None,
) -> range:
    start_year = full_start_year
    if mode == "incremental":
        start_year = max(full_start_year, current_year - _INCREMENTAL_YEAR_WINDOW + 1)
    if target_start_year is not None:
        start_year = max(start_year, target_start_year)
    end_year = min(current_year, target_end_year) if target_end_year is not None else current_year
    if end_year < start_year:
        return range(0)
    return range(start_year, end_year + 1)


async def load_province_targets(pool, target_provinces: Collection[str] | None = None) -> list[ProvinceTarget]:
    async with pool.acquire() as conn:
        rows: Iterable = await conn.fetch("SELECT id, name, code FROM provinces ORDER BY id")

    targets = [
        ProvinceTarget(
            id=int(row["id"]),
            name=str(row["name"]),
            url_value=str(row["code"] or row["id"]),
        )
        for row in rows
    ]
    filters = {str(value).strip() for value in target_provinces or () if str(value).strip()}
    if not filters:
        return targets
    return [target for target in targets if target.name in filters or target.url_value in filters]
