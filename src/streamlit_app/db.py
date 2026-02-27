"""Database layer: OAuth token management and parameterized query execution."""

from __future__ import annotations

import base64
import json as jsonlib
import logging
import re
import time
import uuid
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import requests as httplib
from databricks.sdk import WorkspaceClient

from streamlit_app.config import get_settings

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Module-level token cache
_token_cache: dict[str, Any] = {"token": None, "user": None, "expires_at": 0.0}


def _extract_jwt_subject(token: str) -> str:
    """Extract the 'sub' claim from a JWT token (no signature verification needed)."""
    payload_b64 = token.split(".")[1]
    # Add padding for base64
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    payload = jsonlib.loads(base64.b64decode(payload_b64))
    return payload["sub"]  # type: ignore[no-any-return]


def _generate_credential_via_rest(ws: WorkspaceClient, instance_name: str) -> str:
    """Call the database credentials REST API directly.

    Fallback for older SDK versions that lack ws.database.
    """
    host = (ws.config.host or "").rstrip("/")
    auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]

    resp = httplib.post(
        f"{host}/api/2.0/database/credentials",
        headers={**auth_headers, "Content-Type": "application/json"},
        json={"instance_names": [instance_name], "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    return resp.json()["token"]  # type: ignore[no-any-return]


def _refresh_token() -> str:
    """Generate a fresh OAuth token for Lakebase PostgreSQL.

    Tries the high-level SDK API first, falls back to REST for older runtimes.
    Tokens are cached for 55 minutes (the configured max age).
    """
    settings = get_settings()
    now = time.time()

    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]  # type: ignore[return-value]

    ws = WorkspaceClient()

    try:
        credential = ws.database.generate_database_credential(
            instance_names=[settings.lakebase_instance_name],
        )
        token: str = credential.token  # type: ignore[assignment]
    except AttributeError:
        logger.info("SDK lacks ws.database — falling back to REST API")
        token = _generate_credential_via_rest(ws, settings.lakebase_instance_name)
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


def _create_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection to Lakebase with SSL required."""
    settings = get_settings()
    token = _refresh_token()

    pg_user: str = _token_cache["user"] or _extract_jwt_subject(token)

    return psycopg2.connect(
        host=settings.lakebase_host,
        port=5432,
        database=settings.lakebase_database,
        user=pg_user,
        password=token,
        sslmode="verify-full",
        connect_timeout=10,
        options="-c statement_timeout=30000",
    )


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

    Uses connection-per-query pattern. Combined with @st.cache_data(ttl=600),
    actual DB hits are infrequent.

    All queries MUST use %s parameterized placeholders — never f-string interpolation.
    """
    try:
        conn = _create_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return pd.DataFrame(rows) if rows else pd.DataFrame()
        finally:
            conn.close()
    except psycopg2.Error:
        logger.exception("Database query failed")
        msg = "Database query failed — please try again or contact support."
        raise RuntimeError(msg) from None
