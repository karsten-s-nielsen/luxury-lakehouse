"""Background DB pool health check — logs connectivity failures before users hit them.

**DISABLED BY DEFAULT since 2026-07-28. Enable with ``LAKEBASE_HEALTH_CHECK=1``.**

This probe is fundamentally incompatible with a scale-to-zero database. It ran a
``SELECT 1`` every 60 s forever, while the Lakebase endpoint suspends only after 300 s
with zero client connections — so the probe alone pinned the endpoint ACTIVE 24/7 at its
0.5 CU floor, whether or not a single user was on the app. Setting
``db._POOL_MIN_CONN = 0`` does not help while this loop runs: it reopens a connection
every minute.

It is kept rather than deleted because its intent is sound (surface DB failures in logs
before a user meets them) and it is the right tool for an always-on database. It is simply
the wrong tool for one that is billed by the second and expected to sleep. Turning it on
re-pins the endpoint — that is the whole trade, and it should be a deliberate choice.

The safety net it provided is not lost: ``db.execute_query`` retries
``psycopg2.OperationalError`` (``_RETRY_MAX_ATTEMPTS``) specifically to absorb
scale-to-zero wake-up latency, and logs on exhaustion. Failures still reach the logs; they
are simply observed on real traffic instead of on a synthetic minute-by-minute poll.

If re-enabled, ``_INTERVAL_SECONDS`` should exceed the endpoint's ``suspend_timeout``
(300 s) — otherwise the endpoint can never suspend at all, which is exactly the bug this
default exists to prevent.
"""

from __future__ import annotations

import logging
import os
import threading

import psycopg2

logger = logging.getLogger(__name__)

#: Longer than the 300s Lakebase suspend timeout, so even when enabled the endpoint gets a
#: window in which it can actually suspend rather than being held permanently awake.
_INTERVAL_SECONDS = 600
_ENABLED_ENV = "LAKEBASE_HEALTH_CHECK"


def is_enabled() -> bool:
    """Opt-in only. Any value other than ``1``/``true``/``yes`` leaves the probe off."""
    return os.environ.get(_ENABLED_ENV, "").strip().lower() in {"1", "true", "yes"}


def _check_loop() -> None:
    """Periodically verify DB pool connectivity."""
    # Import here to avoid circular imports at module load time.
    from db import _get_pool

    while True:
        try:
            pool = _get_pool()
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                logger.debug("Health check passed")
            finally:
                pool.putconn(conn)
        except (psycopg2.OperationalError, psycopg2.Error):
            logger.exception("Health check FAILED — DB pool connectivity issue")
        except Exception:
            logger.exception("Health check FAILED — unexpected error")

        # Use threading.Event for interruptible sleep (daemon thread exits on
        # process shutdown regardless, but this is cleaner than time.sleep).
        _stop_event.wait(_INTERVAL_SECONDS)
        if _stop_event.is_set():
            break


_stop_event = threading.Event()


def start() -> None:
    """Start the background health-check daemon thread, if opted in.

    No-op by default: the probe holds the Lakebase endpoint awake (see module docstring).
    Logs the skip at INFO so a silent no-op is never mistaken for a running probe.
    """
    if not is_enabled():
        logger.info(
            "Background DB health check DISABLED (set %s=1 to enable). Reason: a periodic "
            "probe prevents Lakebase scale-to-zero; execute_query's OperationalError retry "
            "covers wake-up latency on real traffic.",
            _ENABLED_ENV,
        )
        return
    thread = threading.Thread(target=_check_loop, name="health-check", daemon=True)
    thread.start()
    logger.warning(
        "Background DB health check ENABLED (interval=%ds) — this keeps the Lakebase "
        "endpoint from suspending and therefore incurs continuous compute cost.",
        _INTERVAL_SECONDS,
    )
