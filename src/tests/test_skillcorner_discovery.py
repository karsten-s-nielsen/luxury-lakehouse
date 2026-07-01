"""Unit tests for the SkillCorner ingestion discovery core + the ``--max-matches`` cap.

Covers the two pieces of the missing-discovery + phased-rollout change:
- ``_select_matches_to_ingest`` — the pure "which matches to ingest" core: MISSING
  (never ingested, any ``updated_at``) OR MODIFIED (re-issued since our watermark),
  deterministically capped by ``max_matches``. This is the "ingest anything missing"
  contract the prior modified-since-only guard did not honour.
- ``_parse_max_matches`` — empty job-parameter → None (daily default), positive int,
  loud SystemExit otherwise (mirrors action-context ``_parse_preflight_filters``).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.skillcorner import _parse_max_matches, _select_matches_to_ingest
from ingestion.skillcorner_common import MatchInfo


def _match(
    mid: str, *, date: str = "2026-01-01", updated: str = "2026-01-01T00:00:00Z", vis: str = "private"
) -> MatchInfo:
    return MatchInfo(
        id=mid,
        artifacts={},
        home="H",
        away="A",
        date=date,
        updated_at=datetime.fromisoformat(updated.replace("Z", "+00:00")),
        visibility=vis,
    )


# ── _select_matches_to_ingest — MISSING ─────────────────────────────────────


def test_missing_match_is_ingested_regardless_of_old_updated_at() -> None:
    """The core fix: a never-ingested match with an OLD updated_at (older than the
    watermark) MUST still be discovered — the modified-since-only guard skipped it."""
    m = _match("100", updated="2020-01-01T00:00:00Z")  # ancient
    watermark = datetime(2026, 6, 1, tzinfo=timezone.utc)  # far newer

    out = _select_matches_to_ingest([m], ingested_ids=set(), watermark=watermark, max_matches=None)

    assert [x.id for x in out] == ["100"]


def test_ingested_and_unmodified_match_is_skipped() -> None:
    m = _match("100", updated="2020-01-01T00:00:00Z")
    watermark = datetime(2026, 6, 1, tzinfo=timezone.utc)

    out = _select_matches_to_ingest([m], ingested_ids={"100"}, watermark=watermark, max_matches=None)

    assert out == []


def test_ingested_but_modified_match_is_reingested() -> None:
    m = _match("100", updated="2026-07-01T00:00:00Z")  # newer than watermark
    watermark = datetime(2026, 6, 1, tzinfo=timezone.utc)

    out = _select_matches_to_ingest([m], ingested_ids={"100"}, watermark=watermark, max_matches=None)

    assert [x.id for x in out] == ["100"]


def test_first_run_no_watermark_ingests_all_missing() -> None:
    matches = [_match("1"), _match("2"), _match("3")]
    out = _select_matches_to_ingest(matches, ingested_ids=set(), watermark=None, max_matches=None)
    assert {x.id for x in out} == {"1", "2", "3"}


# ── _select_matches_to_ingest — the max_matches cap ─────────────────────────


def test_cap_is_deterministic_sorted_by_date_then_id() -> None:
    # Discovery order is scrambled; cap must pick the (date, id)-sorted first N.
    matches = [
        _match("30", date="2026-03-01"),
        _match("10", date="2026-01-01"),
        _match("20", date="2026-02-01"),
    ]
    out = _select_matches_to_ingest(matches, ingested_ids=set(), watermark=None, max_matches=2)
    assert [x.id for x in out] == ["10", "20"]  # earliest two by date


def test_cap_none_is_noop_order_preserved() -> None:
    matches = [_match("30", date="2026-03-01"), _match("10", date="2026-01-01")]
    out = _select_matches_to_ingest(matches, ingested_ids=set(), watermark=None, max_matches=None)
    assert [x.id for x in out] == ["30", "10"]  # original order, no sort


def test_cap_walks_forward_as_ingested_grows() -> None:
    """Phased rollout: capping to N=2 twice, with the first batch now ingested, yields
    the NEXT 2 (the anti-join excludes the already-ingested)."""
    all_m = [_match(str(i), date=f"2026-01-{i:02d}") for i in range(1, 6)]  # ids 1..5 by date

    first = _select_matches_to_ingest(all_m, ingested_ids=set(), watermark=None, max_matches=2)
    assert [x.id for x in first] == ["1", "2"]

    second = _select_matches_to_ingest(all_m, ingested_ids={"1", "2"}, watermark=None, max_matches=2)
    assert [x.id for x in second] == ["3", "4"]


# ── _parse_max_matches ──────────────────────────────────────────────────────


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_parse_max_matches_empty_is_none(raw: str | None) -> None:
    assert _parse_max_matches(raw) is None


def test_parse_max_matches_valid() -> None:
    assert _parse_max_matches("5") == 5
    assert _parse_max_matches("  20 ") == 20


@pytest.mark.parametrize("raw", ["0", "-3"])
def test_parse_max_matches_non_positive_raises(raw: str) -> None:
    with pytest.raises(SystemExit):
        _parse_max_matches(raw)


def test_parse_max_matches_non_integer_raises() -> None:
    with pytest.raises(SystemExit):
        _parse_max_matches("five")
