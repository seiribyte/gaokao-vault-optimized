from __future__ import annotations

from unittest.mock import patch

from gaokao_vault.anti_detect import proxy_pool
from gaokao_vault.config import ProxyConfig


def test_proxy_manager_replaced_when_explicit_config_changes(monkeypatch) -> None:
    monkeypatch.setattr(proxy_pool, "_manager", None)
    monkeypatch.setattr(proxy_pool, "_manager_config", None)
    first = ProxyConfig(static_proxies=["http://first.invalid:8080"], use_freeproxy=False)
    second = ProxyConfig(static_proxies=["http://second.invalid:8080"], use_freeproxy=False)

    first_manager = proxy_pool._get_manager(first)
    second_manager = proxy_pool._get_manager(second)

    assert second_manager is not first_manager
    assert second_manager.diagnostics()["sample_proxies"] == ["http://second.invalid:8080"]


def test_proxy_manager_refreshes_only_after_interval() -> None:
    manager = proxy_pool.ProxyPoolManager(ProxyConfig(use_freeproxy=True, refresh_interval_min=1))
    manager._last_refresh = 100.0

    with (
        patch.object(proxy_pool.time, "monotonic", side_effect=[159.0, 160.0]),
        patch.object(manager, "refresh_free_proxies") as refresh,
    ):
        manager.refresh_if_due()
        manager.refresh_if_due()

    refresh.assert_called_once_with()
