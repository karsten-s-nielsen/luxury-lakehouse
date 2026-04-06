"""Common query infrastructure — re-exports of DB/cache primitives.

Column name constant tuples provide a single source of truth for cross-referencing
with dbt contracts. Import from here instead of db/cache directly in query modules.
"""

from __future__ import annotations

import re

import pandas as pd
from cache import ttl_cache
from db import execute_query, t, validate_param_id

_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def decode_unicode_columns(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Decode literal ``\\uXXXX`` escape sequences in string columns.

    Wyscout Figshare data contains double-escaped Unicode in player names
    (e.g. ``Alc\\u00e1cer`` instead of ``Alcácer``). This decodes them
    in-place for display.

    Args:
        df: DataFrame to process.
        columns: Specific columns to decode. If None, decodes all columns
            whose name contains ``name`` (case-insensitive).
    """
    if df.empty:
        return df

    if columns is None:
        columns = [c for c in df.columns if "name" in c.lower()]

    for col in columns:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: _UNICODE_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), v) if isinstance(v, str) else v
            )
    return df


__all__ = [
    "decode_unicode_columns",
    "execute_query",
    "t",
    "ttl_cache",
    "validate_param_id",
]
