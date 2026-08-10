"""Bronze-coverage test for Gradient Sports: sources.yml must document the live schema.

Sibling of ``test_statsbomb_bronze_coverage`` / ``test_wyscout_bronze_coverage``. The source
of truth is a checked-in ``DESCRIBE TABLE`` snapshot (``fixtures/gradientsports_bronze_schema
_snapshot.json``); when the pining feed surfaces new fields or ``ingestion/gradientsports_
metadata.py`` adds derived cols, re-run DESCRIBE on the GS bronze tables and refresh it.

WHY THIS EXISTS (PR-2a, 2026-08-09)
-----------------------------------
GS was the only ingested provider with **no bronze-coverage gate at all**. That is why
``visibility`` and ``access_tier`` — stamped on bronze at ingest since the ADR-064 work — sat
undocumented in ``_gradientsports__sources.yml`` while the identical class was rigorously
enforced for statsbomb/wyscout/metrica two directories over. A gap that no test watches is a
gap that grows.

SCOPE — ALL FOUR GS BRONZE TABLES, ZERO EXCLUSIONS
--------------------------------------------------
This gate covers **every** GS bronze table: ``gradientsports_events`` (262 cols),
``gradientsports_metadata`` (40), ``gradientsports_roster`` (9), ``gradientsports_tracking``
(27) — 338 columns, all documented in ``_gradientsports__sources.yml``.

``EXCLUDED_BRONZE_COLS`` is **empty and must stay empty**. An earlier revision of this file
shipped the gate with 20 pre-existing gaps recorded as exclusions-with-reasons. That was
strictly better than no gate, but it was still recording debt rather than paying it, so the
debt was paid instead: the 20 metadata columns are now documented AND carried through
``stg_gradientsports__metadata.sql`` + ``_gradientsports__models.yml``.

THE COUPLING, AND WHY IT DOES NOT APPLY UNIFORMLY
-------------------------------------------------
Documenting a column is not uniformly free, and the asymmetry is the whole reason this was
tractable in one cycle:

* ``gradientsports_metadata`` and ``gradientsports_roster`` **have staging models**, so they
  are in ``test_staging_coverage``'s ``PROVIDER_COVERAGE``. Every column documented for them
  in ``sources.yml`` MUST also reach the staging model and be documented in
  ``_gradientsports__models.yml`` — its ``INITIAL_BRONZE_STAGING_GAPS`` escape hatch is locked
  empty by ``TestCoverageInvariants::test_gaps_snapshot_is_empty``. Dot-named bronze columns
  additionally need a ``RENAMES`` entry mapping bronze name -> staging name.
* ``gradientsports_events`` and ``gradientsports_tracking`` have **no staging model**, so they
  are absent from ``PROVIDER_COVERAGE`` and documenting them costs ``sources.yml`` only. That
  is why 262 + 27 columns could be documented without 289 staging passthroughs.

If either table ever gains a staging model, adding it to ``PROVIDER_COVERAGE`` will demand the
full passthrough — that is the intended, loud consequence, not a surprise.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from coverage_utils import (
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
    )
except ImportError:  # pragma: no cover - import shim mirrors the sibling coverage tests
    from tests.coverage_utils import (  # type: ignore[no-redef]
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
    )

_TEST_DIR = Path(__file__).resolve().parent
_SNAPSHOT_PATH = _TEST_DIR / "fixtures" / "gradientsports_bronze_schema_snapshot.json"
_SOURCES_YML_PATH = (
    _TEST_DIR.parent.parent / "dbt_project" / "models" / "staging" / "gradientsports" / "_gradientsports__sources.yml"
)

_EXPECTED_TABLES = {
    "gradientsports_events",
    "gradientsports_metadata",
    "gradientsports_roster",
    "gradientsports_tracking",
}

# Live bronze cols deliberately NOT documented in sources.yml. **EMPTY, AND IT MUST STAY
# EMPTY** — see the module docstring. Every one of the 338 GS bronze columns is documented.
#
# This is not decoration: `test_excluded_bronze_cols_stays_empty` fails on any entry. If a
# future column genuinely cannot be documented, deleting that test is the honest move, not
# quietly adding a key here — an allowlist that grows silently is how the 20-column gap that
# motivated this gate accumulated in the first place.
EXCLUDED_BRONZE_COLS: dict[str, str] = {}


@pytest.fixture(scope="module")
def _snapshot() -> dict:
    return load_attr_enumeration(_SNAPSHOT_PATH)


class TestGradientSportsBronzeCoverage:
    """Every live GS bronze column must be documented in _gradientsports__sources.yml."""

    def test_snapshot_has_expected_tables(self, _snapshot: dict) -> None:
        """Guardrail: the in-scope GS bronze tables are in the snapshot."""
        assert set(_snapshot["tables"]) == _EXPECTED_TABLES, (
            f"snapshot tables {sorted(_snapshot['tables'])} != expected {sorted(_EXPECTED_TABLES)}. "
            "If a GS bronze table gained a staging model, add it here AND refresh the snapshot."
        )

    @pytest.mark.parametrize("table_name", sorted(_EXPECTED_TABLES))
    def test_every_bronze_col_documented(self, _snapshot: dict, table_name: str) -> None:
        """Every col from the live schema snapshot must be in sources.yml."""
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        missing = snapshot_cols - sources_cols - set(EXCLUDED_BRONZE_COLS.keys())
        assert not missing, (
            f"[{table_name}] {len(missing)} bronze col(s) in DESCRIBE TABLE snapshot but not "
            f"documented in sources.yml:\n  {sorted(missing)}\n"
            "Document them, or add to EXCLUDED_BRONZE_COLS with a reason."
        )

    @pytest.mark.parametrize("table_name", sorted(_EXPECTED_TABLES))
    def test_no_phantom_sources_yml_cols(self, _snapshot: dict, table_name: str) -> None:
        """sources.yml mustn't document cols that don't exist in live bronze."""
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        phantom = sources_cols - snapshot_cols
        assert not phantom, (
            f"[{table_name}] sources.yml documents {len(phantom)} col(s) that DON'T exist in "
            f"the live bronze schema snapshot:\n  {sorted(phantom)}"
        )

    def test_redistribution_cols_are_documented_not_excluded(self) -> None:
        """PR-2a: `visibility`/`access_tier` are the reason this gate exists.

        They must be genuinely documented — never parked in EXCLUDED_BRONZE_COLS. Excluding
        them would leave the gate installed while the exact drift it was written for stayed
        open, which is worse than no gate because it reads as covered.
        """
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, "gradientsports_metadata")
        for col in ("visibility", "access_tier"):
            assert col not in EXCLUDED_BRONZE_COLS, (
                f"{col!r} must not be excluded — it is the ADR-064 redistribution signal this "
                "gate was installed to protect."
            )
            assert col in sources_cols, f"{col!r} is stamped on GS bronze at ingest but not documented in sources.yml."

    def test_excluded_bronze_cols_stays_empty(self) -> None:
        """The allowlist is empty and must stay that way.

        The 20 entries this file originally shipped with were pre-existing debt recorded
        rather than paid. They have since been documented and carried through staging, so the
        list is empty — and an empty allowlist is only meaningful if something defends it.
        Without this test, the next undocumented column gets a one-line entry and the gate
        quietly reverts to the state it was written to end.
        """
        assert not EXCLUDED_BRONZE_COLS, (
            f"EXCLUDED_BRONZE_COLS must stay empty; found {sorted(EXCLUDED_BRONZE_COLS)}. "
            "Document the column in _gradientsports__sources.yml instead — and if it belongs "
            "to a table with a staging model, carry it through staging + _models.yml too."
        )

    def test_every_exclusion_has_a_reason(self) -> None:
        """An exclusion without a reason is a silent gap wearing a gate's clothing.

        Vacuous while the list is empty; retained because it is the invariant that matters if
        the list is ever deliberately reopened.
        """
        empty = sorted(k for k, v in EXCLUDED_BRONZE_COLS.items() if not v or not v.strip())
        assert not empty, f"EXCLUDED_BRONZE_COLS entries with no reason: {empty}"

    def test_exclusions_still_exist_in_live_bronze(self, _snapshot: dict) -> None:
        """Reverse direction: an exclusion for a column that no longer exists is stale.

        Without this, a dropped provider field leaves a permanent entry that quietly widens
        the allowlist for any future column that happens to reuse the name.
        """
        live = {c["name"] for t in _snapshot["tables"].values() for c in t}
        stale = sorted(set(EXCLUDED_BRONZE_COLS) - live)
        assert not stale, (
            f"EXCLUDED_BRONZE_COLS names {len(stale)} col(s) absent from live bronze — remove them:\n  {stale}"
        )
