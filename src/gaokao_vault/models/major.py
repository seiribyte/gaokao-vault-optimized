from __future__ import annotations

from datetime import date
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

MajorCode = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class MajorCategoryItem(BaseModel):
    name: str
    education_level: str
    code: str | None = None


class MajorSubcategoryItem(BaseModel):
    category_id: int
    name: str
    code: str | None = None


class MajorItem(BaseModel):
    source_id: str | None = None
    category_id: int | None = None
    subcategory_id: int | None = None
    code: MajorCode
    name: str
    education_level: str
    duration: str | None = None
    degree: str | None = None
    description: str | None = None
    employment_rate: str | None = None
    graduate_directions: str | None = None


class SchoolMajorItem(BaseModel):
    school_id: int
    major_id: int
    school_major_display_order: int | None = None
    major_strength_rank: int | None = None
    major_strength_score: float | None = None
    major_strength_tier: str | None = None
    is_featured_major: bool = False
    strength_evidence: list[dict] = Field(default_factory=list)


class SchoolMajorStrengthSignalItem(BaseModel):
    school_id: int
    major_id: int
    signal_type: str
    signal_level: str | None = None
    strength_score: float
    source_url: str | None = None
    evidence_title: str | None = None
    evidence_year: int | None = Field(default=None, ge=2000, le=2100)


class MajorSatisfactionItem(BaseModel):
    major_id: int
    school_id: int | None = None
    overall_score: float | None = None
    vote_count: int | None = None


class MajorInterpretationItem(BaseModel):
    major_id: int | None = None
    title: str | None = None
    content: str
    author: str | None = None
    publish_date: date | None = None
    source_url: str | None = None
