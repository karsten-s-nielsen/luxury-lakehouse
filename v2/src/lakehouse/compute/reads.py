"""A declared input read of a compute step: its SQL text and its parameters (perf spec §7.4).

A step and its exact input key take their queries from the SAME declaration, so the key hashes exactly the rows the
output is built from. This module holds only the declaration type. The SQL texts live in the digested modules that own
them (`compute/frames.py` for the frame builder, `compute/spadl_actions.py` for the SPADL steps), so a change to a SQL
text changes the code digest of that step.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

__all__ = ["InputRead"]


@dataclass(frozen=True)
class InputRead:
    """One declared input read: its name (a key label), its SQL text and its parameters. `with_provider` adds the
    provider as the next parameter. `with_record_keys` adds the match's provider record keys as a BIGINT list, the
    filter that reaches the per-provider tracking views (perf spec §7.4a)."""

    name: str
    sql: str
    with_provider: bool
    with_record_keys: bool = False

    def params(self, match_key: int, provider: str, record_keys: Sequence[int] = ()) -> list[object]:
        """The parameters in SQL order: the canonical match key, then the provider, then the record keys."""
        params: list[object] = [match_key, provider] if self.with_provider else [match_key]
        if self.with_record_keys:
            if not record_keys:
                # an empty list reads no rows, which would look like an EMPTY match: refuse it instead
                raise ValueError(f"read {self.name!r} needs the record keys of match {match_key}; none were given")
            params.append(list(record_keys))
        return params
