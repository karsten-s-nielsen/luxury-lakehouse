"""Simple TTL cache for database queries — replaces @st.cache_data(ttl=600).

Usage:
    @ttl_cache(ttl=600)
    def fetch_something(arg1, arg2):
        return execute_query(...)
"""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import wraps
from typing import Any

_cache: dict[str, tuple[float, Any]] = {}
_DEFAULT_TTL = 600  # 10 minutes, matching Streamlit


def ttl_cache(ttl: int = _DEFAULT_TTL) -> Callable:
    """Decorator that caches function results with a TTL (seconds).

    Cache key is derived from function name + str(args) + str(kwargs).
    Thread-safe enough for Taipy's single-worker model.
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = f"{fn.__module__}.{fn.__qualname__}:{args}:{kwargs}"
            now = time.time()
            if key in _cache:
                cached_at, value = _cache[key]
                if now - cached_at < ttl:
                    return value
            result = fn(*args, **kwargs)
            _cache[key] = (now, result)
            return result

        return wrapper

    return decorator


def clear_cache() -> None:
    """Clear all cached entries (e.g., on competition change)."""
    _cache.clear()
