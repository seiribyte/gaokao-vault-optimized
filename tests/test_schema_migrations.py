from __future__ import annotations

from pathlib import Path


def test_enrollment_plans_existing_tables_get_conflict_target_index() -> None:
    schema_sql = Path("src/gaokao_vault/db/schema.sql").read_text()

    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_enrollment_plans_unique_key" in schema_sql
    assert "ON enrollment_plans(school_id, province_id, year, subject_category_id, batch, major_name)" in schema_sql
    assert "NULLS NOT DISTINCT" in schema_sql


def test_special_enrollments_existing_tables_get_null_safe_conflict_target_index() -> None:
    schema_sql = Path("src/gaokao_vault/db/schema.sql").read_text()

    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_special_enrollments_unique_key" in schema_sql
    assert "ON special_enrollments(enrollment_type, school_id, year, title)" in schema_sql
    assert "NULLS NOT DISTINCT" in schema_sql
