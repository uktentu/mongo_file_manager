"""Retry decorator with exponential backoff for transient MongoDB errors."""

import functools
import logging
import time
from typing import Any, Callable, TypeVar

from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)

from src.errors.exceptions import DatabaseError

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 0.5
DEFAULT_MAX_DELAY = 10.0
DEFAULT_BACKOFF_FACTOR = 2.0

RETRYABLE_EXCEPTIONS = (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)


def retry_on_failure(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    retryable_exceptions: tuple = RETRYABLE_EXCEPTIONS,
) -> Callable[[F], F]:

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exception = exc
                    if attempt < max_retries:
                        delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                        logger.warning(
                            "retry.attempt func=%s attempt=%d/%d delay=%.1fs error=%s",
                            func.__name__, attempt + 1, max_retries, delay, exc,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "retry.exhausted func=%s retries=%d error=%s",
                            func.__name__, max_retries, exc,
                        )

            raise DatabaseError(
                f"Operation '{func.__name__}' failed after {max_retries} retries: "
                f"{last_exception}"
            ) from last_exception

        return wrapper  # type: ignore

    return decorator
