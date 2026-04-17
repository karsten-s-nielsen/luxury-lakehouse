"""Bounded, thread-safe, stampede-protected TTL cache for database queries.

Replaces the earlier hand-rolled dict cache that had two structural flaws
caught by the 2026-04-16 post-VAEP-backfill optimization audit:

1. Unbounded growth — per-frame queries on Pitch-Control / Team-Shape pages
   can generate 100K+ distinct keys per match, with ~0% reuse across users.
2. No thread lock — check-then-write is two unsynchronized operations, which
   causes a thundering herd at TTL expiry under Taipy's threaded callback
   model (every concurrent request fires the same DB query).

(The original docstring listed a third flaw — "stale entries accumulate
because clear_cache() is never called." The 2026-04-16 second-pass audit
showed that was a phantom problem: LRU eviction on a bounded cache handles
it, and the earlier "call clear_cache on competition change" remediation
was itself a regression that wiped 13 zero-arg comp-independent functions
on every comp switch. clear_cache() is kept solely for the admin endpoint.)

Implementation:

- `cachetools.TTLCache(maxsize, ttl)` for bounded size + LRU eviction.
- Per-function cache instances so different TTLs don't share a bucket.
- `threading.Lock` per entry (singleflight) so concurrent requests for the
   same expired key result in a single DB call; the rest wait and read the
   computed value.
- `clear_cache()` kept as a public entry point for the /api/cache/clear
   HTTP admin endpoint in admin_api.py. It is NOT used on competition
   change — do not add that back; see audit note above.
- `cache_size()` kept as a public entry point for /api/cache/size.

Usage (unchanged from the old API):
    @ttl_cache()                # default 600s, 2000 max entries
    def fetch_something(arg1, arg2): ...

    @ttl_cache(ttl=60)          # short TTL for volatile data
    def fetch_live_data(): ...
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from functools import wraps
from typing import Any

from cachetools import TTLCache

_DEFAULT_TTL = 600  # 10 minutes, matching the previous default
_DEFAULT_MAXSIZE = 2000  # bounded size — prevents per-frame entry explosion

# Registry of every per-function cache so clear_cache() and cache_size()
# can operate across all decorated functions. Populated lazily by the
# decorator. Guarded by _registry_lock.
_registry_lock = threading.Lock()
_caches: list[TTLCache] = []

# Singleflight state — one event per in-flight (cache, key) pair so that
# concurrent callers for the same expired key share a single computation.
# Shared across all per-function caches because keys are unique (module +
# qualname prefix is part of the key).
_inflight_lock = threading.Lock()
_inflight: dict[tuple[int, str], threading.Event] = {}


def ttl_cache(ttl: int = _DEFAULT_TTL, maxsize: int = _DEFAULT_MAXSIZE) -> Callable:
    """Decorator that caches function results with TTL + bounded size + singleflight.

    Cache key is derived from fn.__qualname__ + str(args) + str(kwargs).
    For mutable arguments (lists, dicts), convert at the call site — e.g.,
    pass tuple(player_ids) instead of list[int].

    Thread-safe: check-then-compute-then-write is guarded by a per-entry
    Event so N concurrent callers for the same expired key produce exactly
    one database call.
    """
    fn_cache: TTLCache = TTLCache(maxsize=maxsize, ttl=ttl)
    with _registry_lock:
        _caches.append(fn_cache)

    def decorator(fn: Callable) -> Callable:
        cache_id = id(fn_cache)

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = f"{fn.__qualname__}:{args}:{kwargs}"

            # Fast path: hit without grabbing the inflight lock.
            try:
                return fn_cache[key]
            except KeyError:
                pass

            # Slow path: coordinate via singleflight.
            inflight_key = (cache_id, key)
            event: threading.Event | None
            am_leader = False
            with _inflight_lock:
                # Re-check under the lock in case a concurrent leader just
                # populated the cache between the fast-path miss and now.
                try:
                    return fn_cache[key]
                except KeyError:
                    pass
                event = _inflight.get(inflight_key)
                if event is None:
                    event = threading.Event()
                    _inflight[inflight_key] = event
                    am_leader = True

            if not am_leader:
                # Follower: wait for the leader to compute, then read.
                # Bound the wait at ttl so a stuck leader can't block us
                # indefinitely. If the timeout elapses we fall through and
                # compute independently — the worst case is one redundant call.
                event.wait(timeout=ttl)
                try:
                    return fn_cache[key]
                except KeyError:
                    # Leader failed or evicted; compute ourselves.
                    return fn(*args, **kwargs)

            # Leader: compute, cache, signal followers.
            try:
                result = fn(*args, **kwargs)
                fn_cache[key] = result
                return result
            finally:
                with _inflight_lock:
                    _inflight.pop(inflight_key, None)
                event.set()

        return wrapper

    return decorator


def clear_cache() -> None:
    """Clear every per-function cache (admin endpoint /api/cache/clear only).

    Do NOT invoke on competition change or any other user event — the
    2026-04-16 audit showed this was a net regression that wiped 13
    zero-arg comp-independent functions on every comp switch.  Bounded
    maxsize + LRU eviction already handles the growth/staleness concerns.
    """
    with _registry_lock:
        for c in _caches:
            c.clear()


def cache_size() -> int:
    """Total cached entries across every per-function cache (for /admin/cache/size)."""
    with _registry_lock:
        return sum(len(c) for c in _caches)
