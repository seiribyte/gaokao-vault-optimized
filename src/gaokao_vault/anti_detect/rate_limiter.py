from __future__ import annotations

import asyncio
import random

from gaokao_vault.config import CrawlConfig


class AdaptiveRequestThrottle:
    """为单个外部接口预留请求时隙, 并支持服务端限流后的冷却时间。"""

    def __init__(self, minimum_delay: float, jitter_ratio: float = 0.5) -> None:
        self.minimum_delay = max(0.1, float(minimum_delay))
        self.jitter_ratio = max(0.0, float(jitter_ratio))
        self._next_allowed_at = 0.0
        self._lock = asyncio.Lock()

    def _interval(self) -> float:
        jitter = self.minimum_delay * self.jitter_ratio
        # 抖动只向后延长, 保证 minimum_delay 真的是请求间隔下限。
        return random.uniform(self.minimum_delay, self.minimum_delay + jitter)  # noqa: S311

    async def wait(self) -> None:
        """等待到下一个允许时隙; 同一接口的并发调用会自动排队。"""
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                wait_for = max(0.0, self._next_allowed_at - now)
                if wait_for == 0.0:
                    self._next_allowed_at = now + self._interval()
                    return
            await asyncio.sleep(wait_for)

    async def extend_cooldown(self, seconds: float) -> None:
        """把后续请求整体推迟, 避免限流响应后仍有排队请求立即发出。"""
        if seconds <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            self._next_allowed_at = max(self._next_allowed_at, loop.time() + seconds)


async def jittered_delay(config: CrawlConfig | None = None) -> None:
    if config is None:
        config = CrawlConfig()
    base = config.base_delay
    jitter = base * config.jitter_ratio
    delay = base + random.uniform(-jitter, jitter)  # noqa: S311
    await asyncio.sleep(max(0.1, delay))
