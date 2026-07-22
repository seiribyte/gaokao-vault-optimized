"""Offline contracts for Docker build-context secret boundaries."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKERIGNORE = ROOT / ".dockerignore"
DOCKERFILE = ROOT / "Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"


def _nonempty_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _pattern_matches(path: str, pattern: str) -> bool:
    """Minimal dockerignore matcher for the patterns used by this repository."""
    normalized = path.replace("\\", "/").lstrip("./")
    pattern = pattern.replace("\\", "/").lstrip("./")
    if pattern.endswith("/"):
        prefix = pattern.rstrip("/")
        return normalized == prefix or normalized.startswith(prefix + "/")
    if "*" in pattern or "?" in pattern or "[" in pattern:
        return re.fullmatch(re.escape(pattern).replace(r"\*", ".*").replace(r"\?", "."), normalized) is not None
    return normalized == pattern or normalized.startswith(pattern.rstrip("/") + "/")


def _is_excluded(path: str, patterns: list[str]) -> bool:
    excluded = False
    for pattern in patterns:
        if pattern.startswith("!"):
            if _pattern_matches(path, pattern[1:]):
                excluded = False
            continue
        if _pattern_matches(path, pattern):
            excluded = True
    return excluded


class TestDockerBuildSecretBoundary:
    def test_dockerignore_exists_and_excludes_secrets(self) -> None:
        assert DOCKERIGNORE.exists(), "root .dockerignore is required to keep secrets out of build context"
        patterns = _nonempty_lines(DOCKERIGNORE)
        assert ".env" in patterns
        assert ".env.*" in patterns
        assert "!.env.example" in patterns
        assert ".git/" in patterns
        assert ".venv/" in patterns
        assert "crawl_data/" in patterns

        must_exclude = [
            ".env",
            ".env.local",
            ".env.production",
            ".git/config",
            ".venv/bin/python",
            "crawl_data/logs/crawl.log",
            "tests/test_config.py",
            ".agents/skills/example",
            ".trellis/tasks/example",
            ".codex/config.toml",
        ]
        must_include = [
            ".env.example",
            "Dockerfile",
            "docker-compose.yml",
            "pyproject.toml",
            "uv.lock",
            "README.md",
            "src/gaokao_vault/cli.py",
            "src/gaokao_vault/db/schema.sql",
            "src/gaokao_vault/vision/prompts/score_line_extract.txt",
        ]
        for path in must_exclude:
            assert _is_excluded(path, patterns), f"{path} must be excluded from Docker build context"
        for path in must_include:
            assert not _is_excluded(path, patterns), f"{path} must remain available to Docker build context"

    def test_dockerfile_uses_allowlisted_copy_only(self) -> None:
        text = DOCKERFILE.read_text(encoding="utf-8")
        assert "COPY . /app" not in text
        assert "COPY . ." not in text
        assert "ADD . /app" not in text
        assert "COPY uv.lock pyproject.toml README.md /app/" in text
        assert "COPY src /app/src" in text
        assert "COPY --from=builder /app /app" in text
        assert "COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright" in text
        # Secrets must never be accepted as build args or baked as ENV values.
        for forbidden in ("ARG OPENAI", "ARG GAOKAO", "ENV OPENAI_API_KEY", "ENV GAOKAO_DB__DSN", "COPY .env"):
            assert forbidden not in text

    def test_compose_injects_runtime_env_without_baking_secrets(self) -> None:
        text = COMPOSE.read_text(encoding="utf-8")
        assert "env_file: .env" in text
        assert "build: ." in text
        assert "GAOKAO_DB__DSN:" in text
        # Compose may reference .env at runtime, but must not copy it into image layers.
        assert "COPY .env" not in text
