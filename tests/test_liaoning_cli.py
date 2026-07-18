from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from gaokao_vault.cli import _liaoning_output_path, _liaoning_subject, app
from gaokao_vault.config import AppConfig

runner = CliRunner()


def test_liaoning_commands_are_registered() -> None:
    callback_names = {
        getattr(command.callback, "__name__", None) for command in app.registered_commands if command.callback
    }

    assert {"crawl_liaoning", "export_liaoning"} <= callback_names


def test_liaoning_subject_accepts_supported_labels() -> None:
    assert _liaoning_subject("all") is None
    assert _liaoning_subject("全部") is None
    assert _liaoning_subject("物理类") == "物理"
    assert _liaoning_subject("历史") == "历史"


def test_liaoning_subject_rejects_unknown_label() -> None:
    with pytest.raises(typer.BadParameter, match="只支持"):
        _liaoning_subject("理科")


def test_liaoning_default_output_path_uses_five_year_window() -> None:
    assert _liaoning_output_path(None, plan_year=2026, crawl_dir="crawl_data") == Path(
        "crawl_data/辽宁-2026数据(2022-2026).xlsx"
    )


def test_crawl_liaoning_passes_scoped_config_and_exports_subject() -> None:
    config = AppConfig()
    pool = MagicMock()
    orchestrator = MagicMock()
    export_summary = SimpleNamespace(
        output_path=Path("liaoning.xlsx"),
        row_count=10,
        new_plan_count=2,
        matched_history_counts={2025: 8},
    )

    with (
        patch("gaokao_vault.config.AppConfig", return_value=config),
        patch("gaokao_vault.cli._setup_logging"),
        patch("gaokao_vault.db.connection.create_pool", new=AsyncMock(return_value=pool)),
        patch("gaokao_vault.db.connection.close_pool", new=AsyncMock()),
        patch("gaokao_vault.scheduler.orchestrator.Orchestrator", return_value=orchestrator) as orchestrator_cls,
        patch("gaokao_vault.scheduler.liaoning.run_liaoning_profile", new=AsyncMock()) as run_profile,
        patch(
            "gaokao_vault.exporters.liaoning.export_liaoning_workbook",
            new=AsyncMock(return_value=export_summary),
        ) as export_workbook,
    ):
        result = runner.invoke(
            app,
            [
                "crawl-liaoning",
                "--plan-year",
                "2026",
                "--subject",
                "历史",
                "--reuse-catalog",
                "--output",
                "liaoning.xlsx",
            ],
        )

    assert result.exit_code == 0
    scoped_crawl = orchestrator_cls.call_args.kwargs["config"]
    assert scoped_crawl.target_provinces == ["辽宁"]
    assert scoped_crawl.target_year_start == 2022
    assert scoped_crawl.target_year_end == 2026
    run_profile.assert_awaited_once_with(orchestrator, refresh_catalog=False)
    export_args = export_workbook.await_args
    assert export_args is not None
    assert export_args.kwargs["plan_year"] == 2026
    assert export_args.kwargs["subject"] == "历史"
