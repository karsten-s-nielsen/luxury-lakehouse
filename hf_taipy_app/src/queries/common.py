"""Common query infrastructure — re-exports of DB/cache primitives.

Column name constant tuples provide a single source of truth for cross-referencing
with dbt contracts. Import from here instead of db/cache directly in query modules.
"""

from __future__ import annotations

from cache import ttl_cache
from db import execute_query, t, validate_param_id

__all__ = [
    "execute_query",
    "t",
    "ttl_cache",
    "validate_param_id",
]
