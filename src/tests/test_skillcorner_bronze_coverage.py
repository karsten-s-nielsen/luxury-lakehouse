"""Bronze SCHEMA-PARITY gate for Skillcorner: sources.yml must document the live schema.

Sibling of ``test_gradientsports_bronze_coverage`` / ``test_statsbomb_bronze_coverage``. The
source of truth is a checked-in ``DESCRIBE TABLE`` snapshot
(``fixtures/skillcorner_bronze_schema_snapshot.json``); refresh it when ingestion adds columns.

**This asserts SCHEMA PARITY, not documentation quality.** Most of the columns it covers carry
auto-generated wording ("<type> — auto-documented from DESCRIBE TABLE…"). Green here means
*every live column is accounted for*, NOT *a human described these*. Read it that way.

WHY THIS EXISTS
---------------
Skillcorner was one of the providers with no bronze-coverage gate at all — which is exactly how
ADR-064's ``visibility``/``access_tier`` sat undocumented on Gradient Sports while the identical
class was rigorously enforced two directories over. A gap that no test watches is a gap that
grows: measured 2026-08-10, skillcorner carried 288 undocumented columns.

The table set comes from ``scripts/_bronze_table_inventory``, so this gate and the
non-contract classification cannot disagree about what skillcorner owns. Migration backups and
other non-contract tables are excluded there, with a reason and an expiry — never by a name
heuristic, and never silently.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    from coverage_utils import load_attr_enumeration, load_bronze_cols_from_sources_yml
except ImportError:  # pragma: no cover - import shim mirrors the sibling coverage tests
    from tests.coverage_utils import (  # type: ignore[no-redef]
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
    )

from scripts._bronze_table_inventory import contract_tables, sources_yml_path

_SNAPSHOT_PATH = Path(__file__).resolve().parent / "fixtures" / "skillcorner_bronze_schema_snapshot.json"
_SOURCES_YML_PATH = sources_yml_path("skillcorner")

# Derived, never hardcoded: the contract set is whatever sources.yml documents, so this gate
# cannot drift from the inventory module's view of the same thing.
_EXPECTED_TABLES = contract_tables("skillcorner")

# Live bronze cols deliberately NOT documented. **EMPTY, AND IT MUST STAY EMPTY** — defended by
# `test_excluded_bronze_cols_stays_empty`. If a future column genuinely cannot be documented,
# deleting that test is the honest move, not quietly adding a key here: an allowlist that grows
# silently is how the gap this gate closes accumulated in the first place.
EXCLUDED_BRONZE_COLS: dict[str, str] = {}


@pytest.fixture(scope="module")
def _snapshot() -> dict:
    return load_attr_enumeration(_SNAPSHOT_PATH)


class TestSkillcornerBronzeCoverage:
    """Every live skillcorner bronze column must be documented in _skillcorner__sources.yml."""

    def test_snapshot_has_expected_tables(self, _snapshot: dict) -> None:
        """Guardrail: the snapshot covers exactly the contract tables."""
        assert set(_snapshot["tables"]) == set(_EXPECTED_TABLES), (
            f"snapshot tables {sorted(_snapshot['tables'])} != contract {sorted(_EXPECTED_TABLES)}. "
            "Refresh the snapshot, or reclassify in scripts/_bronze_table_inventory.py."
        )

    @pytest.mark.parametrize("table_name", sorted(_EXPECTED_TABLES))
    def test_every_bronze_col_documented(self, _snapshot: dict, table_name: str) -> None:
        """Every col from the live schema snapshot must be in sources.yml."""
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        missing = snapshot_cols - sources_cols - set(EXCLUDED_BRONZE_COLS)
        assert not missing, (
            f"[{table_name}] {len(missing)} bronze col(s) in DESCRIBE TABLE snapshot but not "
            f"documented in sources.yml: {sorted(missing)}. "
            "Run scripts/sync_bronze_sources_yml.py."
        )

    @pytest.mark.parametrize("table_name", sorted(_EXPECTED_TABLES))
    def test_no_phantom_sources_yml_cols(self, _snapshot: dict, table_name: str) -> None:
        """sources.yml mustn't document cols that don't exist in live bronze."""
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        phantom = sources_cols - snapshot_cols
        assert not phantom, (
            f"[{table_name}] sources.yml documents {len(phantom)} col(s) that DON'T exist in "
            f"the live bronze schema snapshot: {sorted(phantom)}"
        )

    def test_excluded_bronze_cols_stays_empty(self) -> None:
        """The allowlist is empty and must stay that way.

        An empty allowlist is only meaningful if something defends it; without this, the next
        undocumented column gets a one-line entry and the gate reverts to the state it was
        written to end.
        """
        assert not EXCLUDED_BRONZE_COLS, (
            f"EXCLUDED_BRONZE_COLS must stay empty; found {sorted(EXCLUDED_BRONZE_COLS)}. "
            "Document the column in sources.yml instead."
        )

    def test_exclusions_still_exist_in_live_bronze(self, _snapshot: dict) -> None:
        """Reverse direction: an exclusion for a column that no longer exists is stale.

        Vacuous while the list is empty; retained because it is the invariant that matters if
        the list is ever deliberately reopened — a dropped provider field would otherwise leave
        a permanent entry quietly widening the allowlist for any future column reusing the name.
        """
        live = {c["name"] for t in _snapshot["tables"].values() for c in t}
        stale = sorted(set(EXCLUDED_BRONZE_COLS) - live)
        assert not stale, f"EXCLUDED_BRONZE_COLS names col(s) absent from live bronze: {stale}"

    def test_snapshot_is_valid_json_with_provenance(self) -> None:
        """A snapshot without `snapshot_source` cannot be refreshed by whoever finds it next."""
        raw = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
        assert raw.get("snapshot_source", "").strip(), "snapshot lacks provenance"
        assert raw.get("schema_version", "").strip(), "snapshot lacks a schema_version"
