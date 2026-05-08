from __future__ import annotations

from enum import Enum

BASE_URL = "https://gaokao.chsi.com.cn"

META_FIELDS = frozenset({"id", "created_at", "updated_at", "crawl_task_id", "content_hash"})


class TaskType(str, Enum):
    SCHOOLS = "schools"
    SCHOOL_SATISFACTION = "school_satisfaction"
    MAJOR_CATEGORIES = "major_categories"
    MAJORS = "majors"
    SCHOOL_MAJORS = "school_majors"
    MAJOR_STRENGTH_SIGNALS = "major_strength_signals"
    MAJOR_SATISFACTION = "major_satisfaction"
    DXSBB_ADMISSION_RESULTS = "dxsbb_admission_results"
    INTERPRETATIONS = "interpretations"
    SCORE_LINES = "score_lines"
    SCORE_SEGMENTS = "score_segments"
    ENROLLMENT_PLANS = "enrollment_plans"
    MAJOR_ADMISSION_RESULTS = "major_admission_results"
    CHARTERS = "charters"
    TIMELINES = "timelines"
    SPECIAL = "special"
    PROVINCIAL_ANNOUNCEMENTS = "provincial_announcements"


PHASE2_TYPES = [
    TaskType.SCHOOLS,
    TaskType.MAJORS,
    TaskType.SCORE_LINES,
    TaskType.TIMELINES,
]

PHASE3_TYPES = [
    TaskType.SCHOOL_MAJORS,
    TaskType.MAJOR_STRENGTH_SIGNALS,
    TaskType.SCORE_SEGMENTS,
    TaskType.ENROLLMENT_PLANS,
    TaskType.MAJOR_ADMISSION_RESULTS,
    TaskType.DXSBB_ADMISSION_RESULTS,
    TaskType.CHARTERS,
    TaskType.SPECIAL,
    TaskType.SCHOOL_SATISFACTION,
    TaskType.MAJOR_SATISFACTION,
    TaskType.INTERPRETATIONS,
    TaskType.PROVINCIAL_ANNOUNCEMENTS,
]
