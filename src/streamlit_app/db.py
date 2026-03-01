"""Database layer: OAuth token management, connection pooling, and parameterized query execution."""

from __future__ import annotations

import base64
import json as jsonlib
import logging
import re
import threading
import time
import uuid
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests as httplib
from databricks.sdk import WorkspaceClient

from streamlit_app.config import get_settings

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Thread lock for token cache and connection pool state (L-4: thread safety)
_pool_lock = threading.Lock()

# Module-level token cache (guarded by _pool_lock)
_token_cache: dict[str, Any] = {"token": None, "user": None, "expires_at": 0.0}

# Connection pool state (guarded by _pool_lock)
_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_created_at: float = 0.0

# Retry settings for scale-to-zero wake-up latency
_RETRY_MAX_ATTEMPTS = 3
_RETRY_DELAY_SECONDS = 3


def _extract_jwt_subject(token: str) -> str:
    """Extract and validate the 'sub' claim from a JWT token.

    The subject must be a valid UUID (service principal ID). This prevents
    malformed or spoofed tokens from being used as PG usernames.
    """
    payload_b64 = token.split(".")[1]
    # Add padding for base64
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    payload = jsonlib.loads(base64.b64decode(payload_b64))
    sub: str = payload["sub"]

    # Validate UUID format (M-4: prevent non-UUID strings as PG usernames)
    try:
        uuid.UUID(sub)
    except (ValueError, AttributeError) as exc:
        msg = f"JWT 'sub' claim is not a valid UUID: {sub!r}"
        raise ValueError(msg) from exc

    return sub


def _generate_credential_via_rest(ws: WorkspaceClient, endpoint_name: str) -> str:
    """Call the Lakebase Autoscaling credentials REST API directly.

    Fallback for older SDK versions that lack ws.postgres.
    """
    host = (ws.config.host or "").rstrip("/")
    auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]

    resp = httplib.post(
        f"{host}/api/2.0/postgres/credentials",
        headers={**auth_headers, "Content-Type": "application/json"},
        json={"endpoint": endpoint_name, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    # L-14: Log HTTP auth failures before raising
    if not resp.ok:
        logger.error(
            "SECURITY: REST credential API returned HTTP %d %s",
            resp.status_code,
            resp.reason,
        )
    resp.raise_for_status()
    return resp.json()["token"]  # type: ignore[no-any-return]


def _refresh_token() -> str:
    """Generate a fresh OAuth token for Lakebase PostgreSQL.

    Tries the high-level SDK API first, falls back to REST for older runtimes.
    Tokens are cached for 55 minutes (the configured max age).

    THREADING: Must be called while holding _pool_lock (L-4). This is
    guaranteed because the only caller is _get_pool(), which acquires
    the lock before calling this function.
    """
    settings = get_settings()
    now = time.time()

    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]  # type: ignore[return-value]

    ws = WorkspaceClient()

    # Always use REST API — the SDK's generate_database_credential() does not
    # yet support the `endpoint` parameter (returns a generic credential that
    # Lakebase Autoscaling endpoints reject).  The REST API correctly scopes
    # the JWT to the specific endpoint.
    try:
        token = _generate_credential_via_rest(ws, settings.lakebase_endpoint_name)
    except Exception:
        logger.exception("SECURITY: Failed to obtain Lakebase OAuth token")
        raise

    pg_user = _extract_jwt_subject(token)

    _token_cache["token"] = token
    _token_cache["user"] = pg_user
    _token_cache["expires_at"] = now + settings.pool_connection_max_age_seconds

    ttl = settings.pool_connection_max_age_seconds
    logger.info("Refreshed Lakebase OAuth token for user=%s (expires in %ds)", pg_user, ttl)
    return token


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return a thread-safe connection pool, recycling when the OAuth token expires.

    The pool is created with the current token and recycled every 55 minutes
    (pool_connection_max_age_seconds) to stay in sync with token rotation.
    """
    global _pool, _pool_created_at

    settings = get_settings()
    now = time.time()

    with _pool_lock:
        if _pool is not None and (now - _pool_created_at) < settings.pool_connection_max_age_seconds:
            return _pool

        # Close old pool connections before creating a new one
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception:
                logger.warning("Failed to close old connection pool", exc_info=True)

        token = _refresh_token()
        pg_user: str = _token_cache["user"] or _extract_jwt_subject(token)

        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=settings.lakebase_host,
            port=5432,
            database=settings.lakebase_database,
            user=pg_user,
            password=token,
            sslmode="require",
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
        _pool_created_at = now
        logger.info("Created connection pool (min=1, max=5) for user=%s", pg_user)
        return _pool


def validate_table_name(table: str) -> str:
    """Validate a table name against the identifier regex.

    Raises ValueError if the name contains invalid characters.
    """
    if not _IDENTIFIER_RE.match(table):
        msg = f"Invalid table name: {table!r}"
        raise ValueError(msg)
    return table


def t(table_name: str) -> str:
    """Return a fully-qualified Lakebase table reference.

    Produces: dev_gold.table_name (PG schema prefix + validated table name).
    """
    validate_table_name(table_name)
    return f"{get_settings().pg_schema_prefix}.{table_name}"


def execute_query(query: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Execute a parameterized query and return results as a DataFrame.

    Uses a ThreadedConnectionPool with 55-min recycle aligned to OAuth token
    expiry. Combined with @st.cache_data(ttl=600), actual DB hits are infrequent.

    Retries up to 3 times on psycopg2.OperationalError to handle Lakebase
    Autoscaling scale-to-zero wake-up latency (typically 2-5 seconds).

    All queries MUST use %s parameterized placeholders — never f-string interpolation.
    """
    last_error: Exception | None = None

    for attempt in range(_RETRY_MAX_ATTEMPTS):
        pool = _get_pool()
        conn = None
        try:
            conn = pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return pd.DataFrame(rows) if rows else pd.DataFrame()
        except psycopg2.OperationalError as exc:
            last_error = exc
            if conn is not None:
                pool.putconn(conn, close=True)
                conn = None
            if attempt < _RETRY_MAX_ATTEMPTS - 1:
                logger.warning(
                    "Lakebase connection failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    _RETRY_MAX_ATTEMPTS,
                    _RETRY_DELAY_SECONDS,
                    exc,
                )
                time.sleep(_RETRY_DELAY_SECONDS)
                continue
            logger.exception("Database query failed after %d attempts", _RETRY_MAX_ATTEMPTS)
            msg = "Database query failed — please try again or contact support."
            raise RuntimeError(msg) from None
        except psycopg2.Error:
            logger.exception("Database query failed")
            if conn is not None:
                pool.putconn(conn, close=True)
                conn = None
            msg = "Database query failed — please try again or contact support."
            raise RuntimeError(msg) from None
        finally:
            if conn is not None:
                pool.putconn(conn)

    # Should not reach here, but satisfy type checker
    msg = "Database query failed — please try again or contact support."
    raise RuntimeError(msg) from last_error
