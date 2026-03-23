"""Application configuration via environment variables."""

from __future__ import annotations

import re
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class AppSettings(BaseSettings):
    """Taipy app settings bound to environment variables."""

    model_config = {"env_prefix": "", "case_sensitive": False}

    # Required — Lakebase connection
    lakebase_host: str
    lakebase_endpoint_name: str

    # Defaults
    lakebase_database: str = "databricks_postgres"
    unity_catalog: str = "soccer_analytics"
    gold_schema: str = "dev_gold"
    cache_ttl_seconds: int = 600
    pool_connection_max_age_seconds: int = 3300  # 55 min (token expires at 60)

    @field_validator("unity_catalog", "gold_schema")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            msg = f"Invalid identifier: {v!r}. Must match {_IDENTIFIER_RE.pattern}"
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
