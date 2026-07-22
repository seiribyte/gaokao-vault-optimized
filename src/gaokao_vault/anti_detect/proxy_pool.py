from __future__ import annotations

import logging
import random
import time
from typing import TYPE_CHECKING, Any, cast

from gaokao_vault.config import ProxyConfig

if TYPE_CHECKING:
    from scrapling.fetchers import ProxyRotator

logger = logging.getLogger(__name__)

_manager: ProxyPoolManager | None = None
_manager_config: ProxyConfig | None = None


def _sanitize_proxy(proxy: str) -> str:
    if "@" not in proxy:
        return proxy

    if "://" in proxy:
        scheme, remainder = proxy.split("://", 1)
        return f"{scheme}://***@{remainder.rsplit('@', 1)[-1]}"
    return f"***@{proxy.rsplit('@', 1)[-1]}"


class ProxyPoolManager:
    def __init__(self, config: ProxyConfig | None = None):
        if config is None:
            config = ProxyConfig()
        self._paid_proxies: list[str] = list(config.static_proxies)
        self._free_proxies: list[str] = []
        self._use_freeproxy: bool = config.use_freeproxy
        self._refresh_interval_seconds = config.refresh_interval_min * 60
        self._last_refresh: float | None = None

    def refresh_free_proxies(self) -> None:
        self._last_refresh = time.monotonic()
        if not self._use_freeproxy:
            return
        try:
            from freeproxy.freeproxy import ProxiedSessionClient

            client = ProxiedSessionClient(
                init_proxied_session_cfg={
                    "max_pages": 1,
                    "filter_rule": {"country_code": ["CN"]},
                },
                disable_print=True,
            )
            for _ in range(10):
                try:
                    p = client.getrandomproxy(proxy_format="str")
                    if p and isinstance(p, str) and p not in self._free_proxies:
                        self._free_proxies.append(p)
                except Exception:  # noqa: S112
                    continue
            logger.info("Refreshed free proxies, total: %d", len(self._free_proxies))
        except ImportError:
            logger.warning("pyfreeproxy not installed, skipping free proxy refresh")
        except Exception:
            logger.warning("Free proxy refresh failed, continuing without free proxies", exc_info=True)

    def refresh_if_due(self) -> None:
        if self._last_refresh is None or time.monotonic() - self._last_refresh >= self._refresh_interval_seconds:
            self.refresh_free_proxies()

    def get_rotator(self) -> ProxyRotator | None:
        from scrapling.fetchers import ProxyRotator

        all_proxies = self._paid_proxies + self._free_proxies
        if not all_proxies:
            return None

        def random_strategy(proxies, current_index):
            idx = random.randint(0, len(proxies) - 1)  # noqa: S311
            return proxies[idx], idx

        return ProxyRotator(cast(Any, all_proxies), strategy=random_strategy)

    def diagnostics(self) -> dict[str, Any]:
        all_proxies = self._paid_proxies + self._free_proxies
        return {
            "use_freeproxy": self._use_freeproxy,
            "paid_count": len(self._paid_proxies),
            "free_count": len(self._free_proxies),
            "total_count": len(all_proxies),
            "sample_proxies": [_sanitize_proxy(proxy) for proxy in all_proxies[:3]],
        }


def _get_manager(config: ProxyConfig | None = None) -> ProxyPoolManager:
    global _manager, _manager_config
    if _manager is None or (config is not None and config != _manager_config):
        resolved_config = config or ProxyConfig()
        _manager = ProxyPoolManager(resolved_config)
        _manager_config = resolved_config.model_copy(deep=True)
    _manager.refresh_if_due()
    return _manager


def get_proxy_rotator(config: ProxyConfig | None = None) -> ProxyRotator | None:
    return _get_manager(config).get_rotator()


def get_proxy_diagnostics(config: ProxyConfig | None = None) -> dict[str, Any]:
    return _get_manager(config).diagnostics()
