from __future__ import annotations

import random

from fake_useragent import UserAgent

IMPERSONATE_LIST: list[str] = [
    "chrome",
    "firefox",
    "safari",
    "edge",
]

BROWSER_TYPES: list[str] = ["chrome", "firefox", "safari", "edge"]
_FAKE_USERAGENT_BROWSER_NAMES = {
    "chrome": "Chrome",
    "firefox": "Firefox",
    "safari": "Safari",
    "edge": "Edge",
}
_BROWSER_UA_MARKERS = {
    "chrome": ("Chrome/", "Edg/"),
    "firefox": ("Firefox/",),
    "safari": ("Safari/", "Chrome/", "Chromium/"),
    "edge": ("Edg/",),
}


class UAPool:
    """User-Agent pool combining fake-useragent for realistic UA strings
    and Scrapling impersonate list for TLS fingerprint selection."""

    def __init__(self) -> None:
        self._ua = UserAgent(browsers=_FAKE_USERAGENT_BROWSER_NAMES.values(), platforms=["desktop"])

    def get_random_ua(self) -> str:
        """Get a random realistic User-Agent string."""
        return self._ua.random

    def get_random_impersonate(self) -> str:
        """Get a random browser name for Scrapling impersonate parameter."""
        return random.choice(IMPERSONATE_LIST)  # noqa: S311

    def get_ua_for_browser(self, browser: str) -> str:
        """Get a User-Agent string for a specific browser type."""
        browser = browser.lower()
        if browser not in BROWSER_TYPES:
            msg = f"Unsupported browser: {browser}. Choose from {BROWSER_TYPES}"
            raise ValueError(msg)
        user_agent = self._ua.getBrowser(_FAKE_USERAGENT_BROWSER_NAMES[browser])["useragent"]
        required_marker, *excluded_markers = _BROWSER_UA_MARKERS[browser]
        if required_marker not in user_agent or any(marker in user_agent for marker in excluded_markers):
            msg = f"fake-useragent returned a mismatched {browser} identifier"
            raise RuntimeError(msg)
        return user_agent


ua_pool = UAPool()
