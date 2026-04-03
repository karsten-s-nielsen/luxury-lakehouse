"""Application configuration via environment variables."""

from __future__ import annotations

import logging
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings

from shared.constants import IDENTIFIER_RE

_logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    """Taipy app settings bound to environment variables."""

    model_config = {"env_prefix": "", "case_sensitive": False, "env_file": ".env", "env_file_encoding": "utf-8"}

    # Required — Lakebase connection
    lakebase_host: str
    lakebase_endpoint_name: str

    # Optional — Databricks credentials (validated at startup for early warning)
    # WorkspaceClient() does ambient lookup, but misconfiguration only surfaces
    # at first user query. These fields surface the problem at boot.
    databricks_host: str | None = None
    databricks_token: str | None = None

    # Defaults
    lakebase_database: str = "databricks_postgres"
    unity_catalog: str = "soccer_analytics"
    gold_schema: str = "dev_gold"
    observability_schema: str = "observability"
    cache_ttl_seconds: int = 600
    pool_connection_max_age_seconds: int = 3300  # 55 min (token expires at 60)

    @field_validator("unity_catalog", "gold_schema", "observability_schema")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not IDENTIFIER_RE.match(v):
            msg = f"Invalid identifier: {v!r}. Must match {IDENTIFIER_RE.pattern}"
            raise ValueError(msg)
        return v

    @property
    def pg_schema_prefix(self) -> str:
        """PG schema prefix for Lakebase queries."""
        return self.gold_schema


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return cached singleton settings instance."""
    return AppSettings()  # type: ignore[call-arg]


def validate_databricks_credentials() -> None:
    """Check Databricks credentials at boot and log warnings if missing.

    Called once at startup. Does not crash — the app can still serve
    static content without Databricks connectivity.
    """
    settings = get_settings()
    if not settings.databricks_host:
        _logger.warning(
            "DATABRICKS_HOST is not set — Lakebase token refresh will rely on "
            "WorkspaceClient ambient auth. Misconfiguration will surface at first query."
        )
    if not settings.databricks_token:
        _logger.warning(
            "DATABRICKS_TOKEN is not set — WorkspaceClient will attempt ambient "
            "OAuth. If running outside Databricks, set DATABRICKS_TOKEN explicitly."
        )
