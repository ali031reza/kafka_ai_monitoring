import time


class FrameFreshnessPolicy:
    """Determines whether camera frames are too old."""

    def __init__(
        self,
        max_age_ms: int,
        max_future_skew_ms: int = 2000,
    ):
        if max_age_ms <= 0:
            raise ValueError("max_age_ms must be positive")

        self._max_age_ms = max_age_ms
        self._max_future_skew_ms = max_future_skew_ms

    def evaluate(
        self,
        timestamp_ms: int,
    ) -> tuple[bool, int]:

        if timestamp_ms <= 0:
            raise ValueError("Invalid capture timestamp")

        now_ms = time.time_ns() // 1_000_000

        age_ms = now_ms - timestamp_ms

        if age_ms < -self._max_future_skew_ms:
            raise ValueError(
                "Camera timestamp is too far in the future"
            )

        is_stale = age_ms > self._max_age_ms

        return is_stale, age_ms