from __future__ import annotations

from unittest.mock import patch

from gaokao_vault.anti_detect.rate_limiter import AdaptiveRequestThrottle


def test_adaptive_throttle_jitter_never_shortens_minimum_delay() -> None:
    throttle = AdaptiveRequestThrottle(minimum_delay=3.0, jitter_ratio=0.5)

    with patch(
        "gaokao_vault.anti_detect.rate_limiter.random.uniform", side_effect=lambda lower, _upper: lower
    ) as uniform:
        interval = throttle._interval()

    assert interval == 3.0
    uniform.assert_called_once_with(3.0, 4.5)
