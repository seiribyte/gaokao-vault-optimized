"""Tests for spider imports and basic structure."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from gaokao_vault.constants import TaskType
from gaokao_vault.scheduler.orchestrator import SPIDER_MAP
from gaokao_vault.spiders import (
    BaseGaokaoSpider,
    CharterSpider,
    DxsbbAdmissionResultSpider,
    EnrollmentPlanSpider,
    InterpretationSpider,
    MajorAdmissionResultSpider,
    MajorSatisfactionSpider,
    MajorSpider,
    MajorStrengthSignalSpider,
    SchoolMajorSpider,
    SchoolSatisfactionSpider,
    SchoolSpider,
    ScoreLineSpider,
    ScoreSegmentSpider,
    SpecialSpider,
    TimelineSpider,
)
from gaokao_vault.spiders.provincial_announcement_spider import ProvincialAnnouncementSpider


class TestSpiderStructure:
    def test_on_error_increments_failed_stats(self):
        spider = object.__new__(BaseGaokaoSpider)
        spider._stats = {"new": 0, "updated": 0, "unchanged": 0, "failed": 0}

        asyncio.run(spider.on_error(MagicMock(url="https://example.invalid"), RuntimeError("boom")))

        assert spider._stats["failed"] == 1

    def test_all_spiders_have_name(self):
        spiders = [
            SchoolSpider,
            MajorSpider,
            ScoreLineSpider,
            TimelineSpider,
            SchoolMajorSpider,
            ScoreSegmentSpider,
            EnrollmentPlanSpider,
            MajorAdmissionResultSpider,
            CharterSpider,
            SpecialSpider,
            SchoolSatisfactionSpider,
            MajorSatisfactionSpider,
            InterpretationSpider,
            DxsbbAdmissionResultSpider,
            MajorStrengthSignalSpider,
            ProvincialAnnouncementSpider,
        ]
        for cls in spiders:
            assert hasattr(cls, "name"), f"{cls.__name__} missing 'name'"
            assert cls.name != "base", f"{cls.__name__} still has base name"

    def test_all_spiders_have_task_type(self):
        spiders = [
            SchoolSpider,
            MajorSpider,
            ScoreLineSpider,
            TimelineSpider,
        ]
        for cls in spiders:
            assert hasattr(cls, "task_type"), f"{cls.__name__} missing 'task_type'"
            assert cls.task_type != "", f"{cls.__name__} has empty task_type"

    def test_spider_inherits_base(self):
        assert issubclass(SchoolSpider, BaseGaokaoSpider)
        assert issubclass(MajorSpider, BaseGaokaoSpider)

    def test_spider_map_complete(self):
        expected_types = {t.value for t in TaskType if t not in (TaskType.MAJOR_CATEGORIES,)}
        mapped_types = set(SPIDER_MAP.keys())
        assert mapped_types == expected_types, f"Missing: {expected_types - mapped_types}"
