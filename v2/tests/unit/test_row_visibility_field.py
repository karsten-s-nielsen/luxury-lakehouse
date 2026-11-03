"""S1 Task 11: the read seam filters on the authoritative `_visibility` column (plan T11).

`scoped_reads(...).visible(rows)` drops private/null rows for a caller without `VIEW_PRIVATE`. On an encrypted-local
lake a bronze/listing row carries `_visibility`, which overrides the source `visibility` flag; `ops_matches` and the
`dim_*` tables carry only `visibility`, which stays the fallback.
"""

from __future__ import annotations

from lakehouse.services.authz import Principal, Role, scoped_reads


def _anon() -> Principal:
    return Principal(None, frozenset({Role.ANONYMOUS}))


def test_visibility_column_overrides_a_misleading_public_source_flag() -> None:
    reads = scoped_reads(services=None, principal=_anon())
    rows = [{"match_key": 1, "visibility": "public", "_visibility": "private"}]
    assert reads.visible(rows) == [], "a public-flagged listing record of a private match must read private"


def test_visibility_column_present_without_a_source_flag_is_returned() -> None:
    reads = scoped_reads(services=None, principal=_anon())
    rows = [{"match_key": 1, "_visibility": "public"}]
    assert reads.visible(rows) == rows, "a bronze data row carries no source visibility; _visibility is authoritative"


def test_source_visibility_is_the_fallback_without_the_column() -> None:
    reads = scoped_reads(services=None, principal=_anon())
    assert reads.visible([{"visibility": "public"}]) == [{"visibility": "public"}]
    assert reads.visible([{"visibility": "private"}]) == []
