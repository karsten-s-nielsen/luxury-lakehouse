"""The pining list-fields check (spec §23.6 "Pining list fields", §29.1; plan Task 20).

A pining list entry names its competition and season; the match's own metadata artifact names them too, and the
artifact is the source of truth. The run compares the two for every ingested pining match: a difference does not fail
the match, which commits with the metadata values, and becomes a visible warning row (`kind =
"list_metadata_mismatch"`, Task 21). A field that the entry does not give is not compared (the "competition not
given by source" group).
"""

from __future__ import annotations

from typing import Final

from lakehouse.entries import SourceEntry
from lakehouse.parsers.base import NamesBundle

__all__ = ["MISMATCH_KIND", "list_fields_mismatch"]

MISMATCH_KIND: Final = "list_metadata_mismatch"
_FIELDS: Final = ("competition_id", "competition_name", "season_id", "season_name")


def list_fields_mismatch(entry: SourceEntry, names: NamesBundle) -> list[str]:
    """One line per field that the entry gives and that differs from the metadata; ids compare as their text."""
    rows = names.competitions.to_pylist()
    if len(rows) != 1:
        return [f"the metadata names {len(rows)} competitions"]
    metadata = rows[0]
    differences: list[str] = []
    for field in _FIELDS:
        listed = getattr(entry, field)
        if listed is None:
            continue
        found = metadata.get(field)
        if found is None or str(found) != listed:
            shown = None if found is None else str(found)
            differences.append(f"{field}: the list says {listed!r}, the metadata {shown!r}")
    return differences
