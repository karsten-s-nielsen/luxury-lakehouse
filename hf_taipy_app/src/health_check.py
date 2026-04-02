"""Background DB pool health check — logs connectivity failures before users hit them.

Runs a lightweight ``SELECT 1`` every 60 seconds in a daemon thread.
Failures are logged as structured JSON (picked up by container log aggregators).
"""

from __future__ import annotations

import logging
import threading

import psycopg2

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS = 60


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
    """Start the background health-check daemon thread."""
    thread = threading.Thread(target=_check_loop, name="health-check", daemon=True)
    thread.start()
    logger.info("Background DB health check started (interval=%ds)", _INTERVAL_SECONDS)
