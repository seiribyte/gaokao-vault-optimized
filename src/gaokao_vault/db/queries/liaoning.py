from __future__ import annotations

from collections.abc import Sequence

import asyncpg

LIAONING_PROVINCE = "辽宁"


async def fetch_liaoning_export_plans(
    conn: asyncpg.Connection,
    *,
    plan_year: int,
    subject: str | None = None,
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT
            ep.id AS plan_id,
            ep.year,
            ep.school_id,
            ep.school_code_raw,
            ep.major_id,
            ep.major_name AS major_full_name,
            ep.batch,
            ep.batch_code,
            ep.batch_category,
            ep.batch_segment,
            ep.program_type,
            ep.plan_count,
            ep.duration,
            ep.tuition,
            ep.note,
            ep.major_group_code,
            ep.major_code_raw,
            ep.selection_requirement,
            ep.adjustment_rule,
            ep.quality_flags,
            source_subject.name AS subject_category,
            source_province.name AS source_province,
            school_province.name AS school_province,
            s.sch_id,
            s.name AS school_name,
            s.crawl_task_id AS school_crawl_task_id,
            s.city,
            s.authority,
            s.level AS school_level,
            s.school_type,
            s.is_211,
            s.is_985,
            s.is_double_first,
            s.is_private,
            s.is_independent,
            s.is_sino_foreign,
            m.name AS canonical_major_name,
            m.education_level,
            major_category.name AS major_category,
            major_subcategory.name AS major_subcategory,
            sm.major_strength_tier,
            sm.strength_evidence
        FROM enrollment_plans ep
        JOIN provinces source_province ON source_province.id = ep.province_id
        JOIN schools s ON s.id = ep.school_id
        LEFT JOIN provinces school_province ON school_province.id = s.province_id
        LEFT JOIN subject_categories source_subject ON source_subject.id = ep.subject_category_id
        LEFT JOIN majors m ON m.id = ep.major_id
        LEFT JOIN major_subcategories major_subcategory ON major_subcategory.id = m.subcategory_id
        LEFT JOIN major_categories major_category
          ON major_category.id = COALESCE(major_subcategory.category_id, m.category_id)
        LEFT JOIN school_majors sm
          ON sm.school_id = ep.school_id
         AND sm.major_id = ep.major_id
        WHERE source_province.name = $1
          AND ep.year = $2
          AND (
              $3::TEXT IS NULL
              OR REPLACE(COALESCE(source_subject.name, ''), '类', '') = REPLACE($3, '类', '')
          )
        ORDER BY
            CASE REPLACE(COALESCE(source_subject.name, ''), '类', '')
                WHEN '物理' THEN 0
                WHEN '历史' THEN 1
                ELSE 2
            END,
            s.name,
            ep.school_code_raw NULLS LAST,
            ep.major_group_code NULLS LAST,
            ep.major_code_raw NULLS LAST,
            ep.id
        """,
        LIAONING_PROVINCE,
        plan_year,
        subject,
    )
    return [dict(row) for row in rows]


async def fetch_liaoning_export_admissions(
    conn: asyncpg.Connection,
    *,
    years: Sequence[int],
    subject: str | None = None,
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT
            mar.id AS admission_id,
            mar.school_id,
            mar.major_id,
            mar.year,
            mar.batch,
            mar.batch_code,
            mar.batch_category,
            mar.batch_segment,
            mar.admitted_count,
            mar.min_score,
            mar.min_rank,
            mar.plan_count,
            mar.school_code_raw,
            mar.major_code_raw,
            mar.major_name_raw,
            m.name AS canonical_major_name,
            subject.name AS subject_category,
            mar.data_source
        FROM major_admission_results mar
        JOIN provinces province ON province.id = mar.province_id
        LEFT JOIN majors m ON m.id = mar.major_id
        LEFT JOIN subject_categories subject ON subject.id = mar.subject_category_id
        WHERE province.name = $1
          AND mar.year = ANY($2::INTEGER[])
          AND (
              $3::TEXT IS NULL
              OR REPLACE(COALESCE(subject.name, ''), '类', '') = REPLACE($3, '类', '')
          )
        ORDER BY mar.year DESC, mar.school_id, mar.major_id, mar.id
        """,
        LIAONING_PROVINCE,
        list(years),
        subject,
    )
    return [dict(row) for row in rows]


async def fetch_liaoning_historical_plans(
    conn: asyncpg.Connection,
    *,
    years: Sequence[int],
    subject: str | None = None,
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT
            ep.id AS historical_plan_id,
            ep.school_id,
            ep.school_code_raw,
            ep.major_id,
            ep.major_name,
            ep.year,
            ep.batch,
            ep.batch_code,
            ep.batch_category,
            ep.batch_segment,
            ep.plan_count,
            ep.major_group_code,
            ep.major_code_raw,
            m.name AS canonical_major_name,
            subject.name AS subject_category
        FROM enrollment_plans ep
        JOIN provinces province ON province.id = ep.province_id
        LEFT JOIN majors m ON m.id = ep.major_id
        LEFT JOIN subject_categories subject ON subject.id = ep.subject_category_id
        WHERE province.name = $1
          AND ep.year = ANY($2::INTEGER[])
          AND (
              $3::TEXT IS NULL
              OR REPLACE(COALESCE(subject.name, ''), '类', '') = REPLACE($3, '类', '')
          )
        ORDER BY ep.year DESC, ep.school_id, ep.major_id, ep.id
        """,
        LIAONING_PROVINCE,
        list(years),
        subject,
    )
    return [dict(row) for row in rows]


async def fetch_liaoning_school_charters(
    conn: asyncpg.Connection,
    *,
    plan_year: int,
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (charter.school_id)
            charter.school_id,
            charter.year,
            charter.content,
            charter.source_url
        FROM admission_charters charter
        JOIN (
            SELECT DISTINCT ep.school_id
            FROM enrollment_plans ep
            JOIN provinces province ON province.id = ep.province_id
            WHERE province.name = $1
              AND ep.year = $2
        ) target_school ON target_school.school_id = charter.school_id
        WHERE charter.year <= $2
        ORDER BY charter.school_id, charter.year DESC, charter.id DESC
        """,
        LIAONING_PROVINCE,
        plan_year,
    )
    return [dict(row) for row in rows]
