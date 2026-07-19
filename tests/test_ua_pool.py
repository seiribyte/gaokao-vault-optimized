from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from gaokao_vault.anti_detect.ua_pool import UAPool


@pytest.mark.parametrize(
    ("browser", "user_agent"),
    [
        ("chrome", "Mozilla/5.0 Chrome/134.0.0.0 Safari/537.36"),
        ("firefox", "Mozilla/5.0 Firefox/137.0"),
        ("safari", "Mozilla/5.0 Version/18.3 Safari/605.1.15"),
        ("edge", "Mozilla/5.0 Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"),
    ],
)
def test_get_ua_for_browser_uses_fake_useragent_canonical_name(browser: str, user_agent: str) -> None:
    pool = UAPool()
    pool._ua = MagicMock()
    pool._ua.getBrowser.return_value = {"useragent": user_agent}

    assert pool.get_ua_for_browser(browser) == user_agent
    pool._ua.getBrowser.assert_called_once_with(browser.title())


def test_get_ua_for_browser_rejects_chrome_fallback_that_is_actually_edge() -> None:
    pool = UAPool()
    pool._ua = MagicMock()
    pool._ua.getBrowser.return_value = {
        "useragent": "Mozilla/5.0 Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    }

    with pytest.raises(RuntimeError, match="mismatched chrome"):
        pool.get_ua_for_browser("chrome")
