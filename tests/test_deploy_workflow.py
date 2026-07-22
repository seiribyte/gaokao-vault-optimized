from __future__ import annotations

from pathlib import Path

DEPLOY_WORKFLOW = Path(".github/workflows/deploy.yml")
REQUIRED_DEPLOY_SECRETS = (
    "DEPLOY_HOST",
    "DEPLOY_PORT",
    "DEPLOY_USER",
    "DEPLOY_PATH",
    "DEPLOY_SSH_KEY",
)


def test_deploy_workflow_skips_unconfigured_targets() -> None:
    workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

    assert "id: deploy-config" in workflow
    for secret in REQUIRED_DEPLOY_SECRETS:
        assert f"{secret}: ${{{{ secrets.{secret} }}}}" in workflow
    assert "for name in DEPLOY_HOST DEPLOY_PORT DEPLOY_USER DEPLOY_PATH DEPLOY_SSH_KEY" in workflow
    assert 'echo "configured=false" >> "$GITHUB_OUTPUT"' in workflow
    assert workflow.count("if: steps.deploy-config.outputs.configured == 'true'") == 2
