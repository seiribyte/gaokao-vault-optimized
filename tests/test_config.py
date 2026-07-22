"""Tests for configuration and constants."""

from __future__ import annotations

from pathlib import Path

from gaokao_vault.config import CrawlConfig, DatabaseConfig, ProxyConfig, ScheduleConfig
from gaokao_vault.constants import BASE_URL, META_FIELDS, PHASE2_TYPES, PHASE3_TYPES, TaskType


class TestConfig:
    def test_database_config_defaults(self):
        config = DatabaseConfig()
        assert config.pool_min == 5
        assert config.pool_max == 20

    def test_crawl_config_defaults(self):
        config = CrawlConfig()
        assert config.concurrency == 2
        assert config.base_delay > 0
        assert config.spider_timeout == 21600
        assert config.phase_timeout == 21600
        assert config.browser_timeout_ms == 120000
        assert config.school_major_min_ready_schools == 100
        assert config.school_major_min_ready_majors == 100
        assert 0 < config.jitter_ratio < 1
        assert config.effective_year_start == config.year_start

    def test_crawl_config_target_year_overrides_default_lower_bound(self):
        config = CrawlConfig(year_start=2015, target_year_start=2024)
        assert config.effective_year_start == 2024

    def test_env_example_matches_crawl_defaults(self):
        values = {
            key: value
            for line in Path(".env.example").read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
            for key, value in [line.split("=", 1)]
        }
        config = CrawlConfig()

        assert int(values["GAOKAO_CRAWL__CONCURRENCY"]) == config.concurrency
        assert float(values["GAOKAO_CRAWL__BASE_DELAY"]) == config.base_delay
        assert int(values["GAOKAO_CRAWL__BATCH_SIZE"]) == config.batch_size
        assert int(values["GAOKAO_CRAWL__YEAR_START"]) == config.year_start
        assert int(values["GAOKAO_CRAWL__SPIDER_TIMEOUT"]) == config.spider_timeout

    def test_crawl_config_accepts_school_major_readiness_threshold_env(self, monkeypatch):
        monkeypatch.setenv("GAOKAO_CRAWL__SCHOOL_MAJOR_MIN_READY_SCHOOLS", "2800")
        monkeypatch.setenv("GAOKAO_CRAWL__SCHOOL_MAJOR_MIN_READY_MAJORS", "1800")

        config = CrawlConfig()

        assert config.school_major_min_ready_schools == 2800
        assert config.school_major_min_ready_majors == 1800

    def test_proxy_config_defaults(self):
        config = ProxyConfig()
        assert isinstance(config.static_proxies, (list, tuple))

    def test_schedule_config_defaults(self):
        config = ScheduleConfig()
        assert config.cron == "0 23 * * *"
        assert config.mode == "incremental"
        assert config.max_concurrent_types == 3
        assert config.types == []


class TestConstants:
    def test_base_url(self):
        assert "gaokao.chsi.com.cn" in BASE_URL

    def test_meta_fields(self):
        assert "id" in META_FIELDS
        assert "created_at" in META_FIELDS
        assert "content_hash" in META_FIELDS

    def test_task_types_enum(self):
        assert TaskType.SCHOOLS.value == "schools"
        assert TaskType.MAJORS.value == "majors"
        assert TaskType.MAJOR_STRENGTH_SIGNALS.value == "major_strength_signals"
        assert TaskType.PROVINCIAL_ANNOUNCEMENTS.value == "provincial_announcements"
        assert len(TaskType) == 17

    def test_phase_types(self):
        assert len(PHASE2_TYPES) == 4
        assert len(PHASE3_TYPES) == 12
        assert TaskType.SCHOOLS in PHASE2_TYPES
        assert TaskType.SCHOOL_MAJORS in PHASE3_TYPES
        assert TaskType.DXSBB_ADMISSION_RESULTS in PHASE3_TYPES
        assert TaskType.MAJOR_STRENGTH_SIGNALS in PHASE3_TYPES
        assert TaskType.PROVINCIAL_ANNOUNCEMENTS in PHASE3_TYPES
