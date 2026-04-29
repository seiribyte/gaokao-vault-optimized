from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class SpecialEnrollmentItem(BaseModel):
    enrollment_type: str
    school_id: int | None = None
    year: int = Field(ge=2000, le=2100)
    title: str | None = None
    content: str | None = None
    publish_date: date | None = None
    source_url: str | None = None
    application_url: str | None = None
    registration_start: date | None = None
    registration_end: date | None = None
    selection_rule: str | None = None
    admission_rule: str | None = None
    eligible_majors: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
