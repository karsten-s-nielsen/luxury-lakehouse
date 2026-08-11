"""Every bronze table is classified: documented contract, or excluded with a reason.

Enforces ADR-075: an exception records the condition under which it stops being one.

Sibling of ``test_access_tier_backfill.py::test_every_access_tier_table_is_classified_exactly_once``
and built for the same reason — *a NEW table must force a conscious decision and can never be
silently dropped*.

The split follows that precedent too: the cheap structural assertions run per-PR here, and the
comparison against the live ``information_schema`` set is operator-run (Task 6's close-out),
because CI has no warehouse.

WHAT THIS PREVENTS
------------------
Enumerating a provider's bronze tables by prefix picked up four LL2-migration backups
(2026-08-10). Documenting them would have demanded ~988 columns of ``sources.yml`` for tables
nothing reads; filtering them by a name heuristic (``_backup``, ``_pre_*``) would have silently
dropped the next one named differently. Partitioning does neither.
"""

from __future__ import annotations

import datetime
import json
import pathlib

import pytest

from scripts._bronze_table_inventory import (
    NON_CONTRACT_TABLES,
    PERMANENT,
    PROVIDERS,
    TEMPORARY,
    TEMPORARY_EXCLUSION_MAX_AGE_DAYS,
    classify,
    contract_tables,
    sources_yml_path,
)

_FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"

# Providers whose bronze schema is captured in a DESCRIBE snapshot. The remaining providers have
# no snapshot yet; adding one must add it here too, which is what makes this list a gate rather
# than a note.
_SNAPSHOTTED: tuple[str, ...] = ("statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports")

# Live bronze table count, 2026-08-10. Operator verifies against the catalog; a drift here means
# bronze gained or lost a table and the inventory needs revisiting.
_LIVE_BRONZE_TABLE_COUNT = 52


def _snapshot_tables(provider: str) -> frozenset[str]:
    path = _FIXTURES / f"{provider}_bronze_schema_snapshot.json"
    return frozenset(json.loads(path.read_text(encoding="utf-8"))["tables"])


def test_every_non_contract_table_is_fully_justified() -> None:
    """Reason AND review_trigger AND a valid classification — the ignore-list schema.

    An exclusion carrying a justification but no condition to revisit it becomes permanent by
    silence; that is what `review_trigger` exists to prevent in `.pip-audit-ignores.yml`, and
    this file needs it for the same reason.
    """
    for table, entry in NON_CONTRACT_TABLES.items():
        assert entry.reason.strip(), f"{table}: no reason"
        assert entry.review_trigger.strip(), f"{table}: no review_trigger — it would never be revisited"
        assert entry.classification in {TEMPORARY, PERMANENT}, f"{table}: bad classification"
        datetime.date.fromisoformat(entry.recorded)  # raises on a malformed date


def test_no_temporary_exclusion_is_overdue() -> None:
    """A TEMPORARY exclusion has a deadline, and this is it.

    The four LL2 backups were recorded 2026-04-29 and were still present 2026-08-10 — 103 days
    — precisely because nothing was watching. When this fires: drop the table, or reclassify it
    PERMANENT with a reason that says why a migration backup became permanent.
    """
    today = datetime.date.today()
    overdue = {
        table: (today - datetime.date.fromisoformat(e.recorded)).days
        for table, e in NON_CONTRACT_TABLES.items()
        if e.classification == TEMPORARY
        and (today - datetime.date.fromisoformat(e.recorded)).days > TEMPORARY_EXCLUSION_MAX_AGE_DAYS
    }
    assert not overdue, (
        f"TEMPORARY bronze exclusions older than {TEMPORARY_EXCLUSION_MAX_AGE_DAYS} days: {overdue}. "
        "Drop the table, or reclassify PERMANENT with a justification."
    )


def test_non_contract_tables_are_not_also_documented() -> None:
    """The partition must be disjoint — a table cannot be both contract and excluded.

    Documenting a table previously excluded (or vice versa) without removing the other entry is
    exactly the half-edit this assertion exists to catch.
    """
    documented: set[str] = set()
    for provider in PROVIDERS:
        documented |= contract_tables(provider)
    overlap = sorted(documented & set(NON_CONTRACT_TABLES))
    assert not overlap, f"tables classified BOTH contract and non-contract: {overlap}"


@pytest.mark.parametrize("provider", _SNAPSHOTTED)
def test_snapshot_covers_exactly_the_contract(provider: str) -> None:
    """The DESCRIBE snapshot and sources.yml must describe the same table set.

    The snapshot feeds the coverage gate; sources.yml is what the gate compares against. If they
    disagree the gate is measuring a set nobody declared — green because it is comparing a thing
    to itself.
    """
    snap, contract = _snapshot_tables(provider), contract_tables(provider)
    assert snap == contract, (
        f"{provider}: snapshot tables != sources.yml tables; "
        f"only-snapshot={sorted(snap - contract)} only-sources={sorted(contract - snap)}"
    )


@pytest.mark.parametrize("provider", PROVIDERS)
def test_classification_is_total_over_the_snapshot(provider: str) -> None:
    """Nothing unclassified, using the snapshot as the stand-in for live.

    The authoritative check is against live ``information_schema`` and is operator-run (Task 6),
    because CI has no warehouse. This is the offline half: it cannot see a table that appeared
    in bronze since the snapshot, but it does catch an entry deleted from one side only.
    """
    known = _snapshot_tables(provider) if provider in _SNAPSHOTTED else contract_tables(provider)
    live_stand_in = known | (frozenset(NON_CONTRACT_TABLES) & _namespace_guess(provider))
    _contract, _non_contract, unclassified = classify(provider, live_stand_in)
    assert not unclassified, (
        f"{provider}: unclassified bronze table(s) {sorted(unclassified)} — document them in "
        f"{sources_yml_path(provider).name} or add them to NON_CONTRACT_TABLES with a reason."
    )


def _namespace_guess(provider: str) -> frozenset[str]:
    """Non-contract tables whose name places them in this provider's namespace."""
    return frozenset(t for t in NON_CONTRACT_TABLES if t.startswith(provider))


def test_non_contract_set_is_pinned() -> None:
    """Pin the SET, not a count — a count permits one-in-one-out swaps, silently.

    Same reasoning as the `_EXEMPT` shape used by the AST gates: an absolute, asserted both
    directions, so adding OR removing an exclusion is a visible, reviewed change.
    """
    assert set(NON_CONTRACT_TABLES) == {
        "idsse_events_pre_close_out_backup",
        "idsse_events_pre_ll2_backfill",
        "metrica_events_pre_close_out_backup",
        "metrica_events_pre_ll2_backfill",
    }


def test_classified_provider_tables_are_a_known_share_of_bronze() -> None:
    """Pin the classified total against the recorded live count.

    An earlier draft asserted `_LIVE_BRONZE_TABLE_COUNT == 52` — a constant compared to itself,
    which is vacuous and would pass forever. Compare the DERIVED classification against it
    instead, the way `test_access_tier_backfill.py` compares `len(ALL_ACCESS_TIER_TABLES)`.
    """
    classified = set(NON_CONTRACT_TABLES)
    for provider in PROVIDERS:
        classified |= contract_tables(provider)
    assert len(classified) == 25, (
        f"classified {len(classified)} provider bronze tables, expected 25 of the "
        f"{_LIVE_BRONZE_TABLE_COUNT} live (the rest are cross-provider facts like spadl_actions). "
        f"Recount and update both numbers deliberately."
    )
