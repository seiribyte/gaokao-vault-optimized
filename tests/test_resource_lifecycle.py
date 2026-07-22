from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gaokao_vault.config import DatabaseConfig, OpenAIConfig
from gaokao_vault.spiders.base import BaseGaokaoSpider
from gaokao_vault.vision.analyzer import VisionAnalyzer


def _db_config() -> DatabaseConfig:
    return DatabaseConfig(dsn="postgresql://test:test@localhost:5432/test_db", pool_min=1, pool_max=2)


def test_local_pool_creation_is_serialized() -> None:
    pool = MagicMock()
    create_pool = AsyncMock(return_value=pool)
    spider = BaseGaokaoSpider(db_config=_db_config(), crawl_task_id=1)

    with patch("gaokao_vault.spiders.base.create_local_pool", new=create_pool):

        async def exercise() -> None:
            assert await asyncio.gather(spider._get_pool(), spider._get_pool()) == [pool, pool]

        asyncio.run(exercise())

    create_pool.assert_awaited_once()


def test_base_close_closes_pool_when_task_finalization_fails() -> None:
    pool = MagicMock()
    pool.close = AsyncMock()
    spider = BaseGaokaoSpider(db_config=_db_config(), crawl_task_id=1)
    spider._local_pool = pool

    with (
        patch(
            "gaokao_vault.db.queries.crawl_meta.update_task_stats",
            new=AsyncMock(side_effect=RuntimeError("stats failed")),
        ),
        pytest.raises(RuntimeError, match="stats failed"),
    ):
        asyncio.run(spider.on_close())

    pool.close.assert_awaited_once()
    assert spider._local_pool is None


def test_vision_analysis_closes_stream_client_and_temporary_s3_object(tmp_path) -> None:
    image_path = tmp_path / "score.png"
    image_path.write_bytes(b"png")
    stream = AsyncMock()
    stream.__aiter__.return_value = [SimpleNamespace(type="response.output_text.delta", delta="[]")]
    client = MagicMock()
    client.responses.create = AsyncMock(return_value=stream)
    client.close = AsyncMock()
    s3 = MagicMock()
    s3.upload_image.return_value = "screenshots/测试/2026/score.png"
    s3.presigned_url.return_value = "https://example.invalid/image"

    with patch("gaokao_vault.vision.analyzer.create_openai_client", return_value=client):
        analyzer = VisionAnalyzer(OpenAIConfig(api_key="test"), s3=s3)
        result = asyncio.run(analyzer.analyze(image_path, "测试", 2026))
        asyncio.run(analyzer.close())

    assert result == []
    stream.close.assert_awaited_once()
    client.close.assert_awaited_once()
    s3.delete_image.assert_called_once_with("screenshots/测试/2026/score.png")


def test_vision_analysis_deletes_s3_object_when_api_fails(tmp_path) -> None:
    image_path = tmp_path / "score.png"
    image_path.write_bytes(b"png")
    client = MagicMock()
    client.responses.create = AsyncMock(side_effect=RuntimeError("api failed"))
    s3 = MagicMock()
    s3.upload_image.return_value = "screenshots/测试/2026/score.png"
    s3.presigned_url.return_value = "https://example.invalid/image"

    with patch("gaokao_vault.vision.analyzer.create_openai_client", return_value=client):
        analyzer = VisionAnalyzer(OpenAIConfig(api_key="test"), s3=s3)
        assert asyncio.run(analyzer.analyze(image_path, "测试", 2026)) == []

    s3.delete_image.assert_called_once_with("screenshots/测试/2026/score.png")


def test_vision_analysis_deletes_upload_when_presigning_fails(tmp_path) -> None:
    image_path = tmp_path / "score.png"
    image_path.write_bytes(b"png")
    client = MagicMock()
    client.responses.create = AsyncMock(side_effect=RuntimeError("api failed"))
    s3 = MagicMock()
    s3.presigned_url.side_effect = RuntimeError("presign failed")

    with patch("gaokao_vault.vision.analyzer.create_openai_client", return_value=client):
        analyzer = VisionAnalyzer(OpenAIConfig(api_key="test"), s3=s3)
        assert asyncio.run(analyzer.analyze(image_path, "测试", 2026)) == []

    s3.delete_image.assert_called_once_with("screenshots/测试/2026/score.png")
