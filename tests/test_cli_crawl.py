"""Regression tests for crawl command failure propagation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from gaokao_vault.cli import app
from gaokao_vault.scheduler.orchestrator import CrawlOutcome

runner = CliRunner()


def test_crawl_exits_nonzero_for_unsuccessful_selected_types() -> None:
    orchestrator = MagicMock()
    orchestrator.run_types = AsyncMock(
        return_value=CrawlOutcome(total=1, failed=1, completed=True, error="one task failed")
    )

    with (
        patch("gaokao_vault.config.AppConfig"),
        patch("gaokao_vault.config.CrawlConfig"),
        patch("gaokao_vault.db.connection.create_pool", new=AsyncMock(return_value=MagicMock())),
        patch("gaokao_vault.db.connection.close_pool", new=AsyncMock()),
        patch("gaokao_vault.scheduler.orchestrator.Orchestrator", return_value=orchestrator),
    ):
        result = runner.invoke(app, ["crawl", "--types", "schools"])

    assert result.exit_code == 1
    assert "one task failed" in result.output
