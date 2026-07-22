from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from conftest import make_mock_pool_and_conn

from gaokao_vault.pipeline.sink import BatchSink


def test_flush_failure_retains_buffer_for_retry() -> None:
    pool, _, _ = make_mock_pool_and_conn()
    flush_fn = AsyncMock(side_effect=[RuntimeError("write failed"), 1])
    sink = BatchSink(pool=pool, flush_fn=flush_fn, batch_size=1)

    with pytest.raises(RuntimeError, match="write failed"):
        asyncio.run(sink.add({"id": 1}))

    assert sink.buffer_size == 1
    assert sink.total_flushed == 0

    assert asyncio.run(sink.flush()) == 1
    assert sink.buffer_size == 0
    assert sink.total_flushed == 1
