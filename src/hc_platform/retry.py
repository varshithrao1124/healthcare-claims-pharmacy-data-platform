"""Small retry helper for transient Delta commit conflicts."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
LOGGER = logging.getLogger("hc_platform")


def call_with_retry(
    fn: Callable[[], T],
    *,
    attempts: int,
    backoff_seconds: float,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn`` until it succeeds or ``attempts`` is exhausted.

    ``retry_on`` should be a narrow set. Reconciliation and simulated
    failures are not retried by the caller.
    """

    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:  # type: ignore[misc]
            last_error = exc
            if attempt == attempts:
                raise
            delay = backoff_seconds * attempt
            LOGGER.warning(
                "transient failure on attempt %s/%s: %s; sleeping %.2fs",
                attempt,
                attempts,
                exc,
                delay,
            )
            sleep_fn(delay)
    assert last_error is not None
    raise last_error
