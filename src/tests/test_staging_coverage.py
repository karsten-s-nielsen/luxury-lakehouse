"""Cross-provider staging-coverage test.

Enforces the bronze-completeness-through-to-staging contract: every bronze
column documented in `_<provider>__sources.yml` must either:

  (a) appear in the staging model as-is,
  (b) appear in the staging model under a renamed identifier (``RENAMES``), or
  (c) be in ``INITIAL_BRONZE_STAGING_GAPS`` — the current-state snapshot of
      bronze cols the staging model deliberately or historically omits.

This is DOCUMENTATION-DRIFT detection, not SQL-level verification: it reads
the ``columns:`` entries in ``_<provider>__models.yml`` as the staging
contract. Staging SQL may surface additional cols; if they're not in
models.yml, they're invisible to this test. Future work: expand models.yml
entries to match actual staging output.

Drift detection:

  - New bronze col appears (DESCRIBE TABLE snapshot updated + sources.yml
    updated) but staging doesn't expose it → test FAILS. Remedy: either
    add the col to the staging model (and document in models.yml), or
    add it to ``INITIAL_BRONZE_STAGING_GAPS`` with a reason.
  - Bronze col removed from sources.yml → test still passes (not currently
    checked; add a "no-phantom-bronze-cols" check later).
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from coverage_utils import (
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
    )

_TEST_DIR = Path(__file__).parent
_DBT_STAGING = _TEST_DIR.parent.parent / "dbt_project" / "models" / "staging"


# Provider → list of (bronze_table, staging_model) pairs.
# Bronze tables without a staging model (e.g. ``statsbomb_competitions``,
# ``metrica_matches``) are omitted from this test; downstream marts read
# them directly.
PROVIDER_COVERAGE: dict[str, list[tuple[str, str]]] = {
    "idsse": [
        ("idsse_events", "stg_idsse__events"),
        ("idsse_tracking", "stg_idsse__tracking"),
    ],
    "skillcorner": [
        ("skillcorner_tracking", "stg_skillcorner__tracking"),
    ],
    "metrica": [
        ("metrica_events", "stg_metrica__events"),
        ("metrica_tracking", "stg_metrica__tracking"),
    ],
    "statsbomb": [
        ("statsbomb_events", "stg_statsbomb__events"),
        ("statsbomb_matches", "stg_statsbomb__matches"),
        ("statsbomb_lineups", "stg_statsbomb__lineups"),
        ("statsbomb_360", "stg_statsbomb__360"),
    ],
    "wyscout": [
        ("wyscout_events", "stg_wyscout__events"),
        # wyscout_matches: stg_wyscout__matches.sql exists but is not yet
        # documented in _wyscout__models.yml. Added to coverage when docs land.
        ("wyscout_players", "stg_wyscout__players"),
        # PR 5a (ADR-011): teams.json ingestion closed the pre-existing gap.
        # bronze.wyscout_teams (142 teams across 7 competitions) is staged
        # verbatim into stg_wyscout__teams.
        ("wyscout_teams", "stg_wyscout__teams"),
    ],
    # PR 6 (ADR-011): stg_pitch_control__values promoted to first-class
    # treatment because notebooks/publish_datasets.py:248 INNER JOINs it
    # to publish luxury-lakehouse/pitch-control-tracking. Staging adds
    # data_source + match_key derived from match_id prefix.
    "pitch_control": [
        ("pitch_control_values", "stg_pitch_control__values"),
    ],
    "tracking_context": [
        ("spadl_tracking_context", "stg_spadl__tracking_context"),
    ],
}


# Explicit bronze→staging column renames per table. Format:
#   (provider, bronze_table): {bronze_col: staging_col}
# Empty dicts indicate no renames — every bronze col keeps its name in staging.
RENAMES: dict[tuple[str, str], dict[str, str]] = {
    ("idsse", "idsse_events"): {},
    ("idsse", "idsse_tracking"): {"timestamp": "timestamp"},
    # stg_skillcorner__tracking renames bronze `timestamp` to
    # `timestamp_seconds` (line 31) for canonical naming across providers.
    ("skillcorner", "skillcorner_tracking"): {"timestamp": "timestamp_seconds"},
    ("metrica", "metrica_events"): {},
    ("metrica", "metrica_tracking"): {},
    ("statsbomb", "statsbomb_events"): {"id": "event_id", "type": "event_type"},
    ("statsbomb", "statsbomb_matches"): {},
    ("statsbomb", "statsbomb_lineups"): {},
    ("statsbomb", "statsbomb_360"): {"id": "event_uuid"},
    # stg_wyscout__events renames the Wyscout camelCase bronze columns to
    # snake_case + some get distinguishing suffixes (raw-JSON variants use
    # _raw; id gets renamed to event_sk because `eventId` is the event-type
    # code, not the unique id). All carry through as-is under the new
    # staging name; none are dropped.
    ("wyscout", "wyscout_events"): {
        "id": "event_sk",
        "eventId": "event_id",
        "eventName": "event_type",
        "eventSec": "event_sec",
        "matchId": "match_id",
        "matchPeriod": "period",
        "playerId": "player_id",
        "positions": "positions_raw",
        "subEventId": "sub_event_id",
        "subEventName": "sub_event_type",
        "tags": "tags_raw",
        "teamId": "team_id",
    },
    ("wyscout", "wyscout_players"): {},
    # stg_wyscout__teams keeps bronze cols verbatim as passthroughs EXCEPT
    # ``name`` which is renamed to ``team_name`` (the primary display col;
    # the bronze camelCase ``officialName`` is kept separately as the
    # ``official_name`` alias + the verbatim passthrough). ``city`` stays
    # under both ``city`` and the ``city_raw`` passthrough alias; ``area``
    # stays as-is and is ALSO exploded into area_name/alpha2/alpha3.
    ("wyscout", "wyscout_teams"): {"name": "team_name"},
    # PR 6: pitch_control bronze cols pass through verbatim; staging
    # additionally derives data_source + match_key (additive, not renames).
    ("pitch_control", "pitch_control_values"): {},
    ("tracking_context", "spadl_tracking_context"): {
        "match_id": "native_match_id",
        "team_id": "team_id_native",
        "player_id": "player_id_native",
    },
}


# Snapshot of the current bronze→staging gap. PR 2 (ADR-011) closed the
# data-surfacing side of this; G5 of the PR #173 drop-safety sweep drained
# the documentation-drift side by (a) correcting RENAMES for cols the
# staging SQL actually surfaces under a new name (Wyscout + SkillCorner
# camelCase→snake_case), and (b) confirming every remaining bronze col was
# already in the staging models.yml. Every pair should have an empty gap
# now; TestCoverageInvariants.test_gaps_snapshot_is_empty enforces that.
#
# Maintenance: when you intentionally leave a bronze col out of staging,
# add it here with a reason comment. When you add a bronze col to staging,
# remove it from this set AND add it to models.yml.
INITIAL_BRONZE_STAGING_GAPS: dict[tuple[str, str], set[str]] = {}


def _all_params() -> list[tuple[str, str, str]]:
    """Flatten PROVIDER_COVERAGE into (provider, bronze_table, staging_model) tuples."""
    return [
        (provider, bronze_table, staging_model)
        for provider, pairs in PROVIDER_COVERAGE.items()
        for bronze_table, staging_model in pairs
    ]


class TestStagingCoverage:
    """Every bronze col is either preserved, renamed, or in INITIAL_BRONZE_STAGING_GAPS."""

    @pytest.mark.parametrize(
        ("provider", "bronze_table", "staging_model"),
        _all_params(),
    )
    def test_bronze_col_coverage(self, provider: str, bronze_table: str, staging_model: str) -> None:
        sources_yml = _DBT_STAGING / provider / f"_{provider}__sources.yml"
        models_yml = _DBT_STAGING / provider / f"_{provider}__models.yml"

        bronze_cols = load_bronze_cols_from_sources_yml(sources_yml, bronze_table)
        staging_cols = load_staging_cols_from_models_yml(models_yml, staging_model)

        renames = RENAMES.get((provider, bronze_table), {})
        gaps = INITIAL_BRONZE_STAGING_GAPS.get((provider, bronze_table), set())

        # Cols we expect to see downstream: bronze minus gaps, with renames applied.
        to_verify = bronze_cols - gaps
        expected_staging_names = {renames.get(c, c) for c in to_verify}
        missing = expected_staging_names - staging_cols

        assert not missing, (
            f"[{provider}.{bronze_table}] {len(missing)} bronze col(s) not "
            f"preserved, renamed, or in INITIAL_BRONZE_STAGING_GAPS:\n"
            f"  {sorted(missing)}\n"
            "Fix: either (a) carry through in staging SQL + document in "
            f"_{provider}__models.yml, (b) add to RENAMES if renamed, or\n"
            f"(c) add to INITIAL_BRONZE_STAGING_GAPS[('{provider}', "
            f"'{bronze_table}')] with a reason."
        )


class TestCoverageInvariants:
    """Invariants on the RENAMES + GAPS config itself."""

    def test_every_provider_covered(self) -> None:
        """Every provider in PROVIDER_COVERAGE has matching sources + models yml files."""
        for provider, pairs in PROVIDER_COVERAGE.items():
            sources_yml = _DBT_STAGING / provider / f"_{provider}__sources.yml"
            models_yml = _DBT_STAGING / provider / f"_{provider}__models.yml"
            assert sources_yml.exists(), f"missing {sources_yml}"
            assert models_yml.exists(), f"missing {models_yml}"
            assert pairs, f"PROVIDER_COVERAGE['{provider}'] is empty"

    def test_renames_keys_match_coverage_keys(self) -> None:
        """Every (provider, bronze_table) in PROVIDER_COVERAGE has a RENAMES entry."""
        coverage_keys = {(p, t) for p, pairs in PROVIDER_COVERAGE.items() for t, _ in pairs}
        extra = set(RENAMES.keys()) - coverage_keys
        assert not extra, f"RENAMES has keys not in PROVIDER_COVERAGE: {extra}"

    def test_gaps_snapshot_is_empty(self) -> None:
        """Post-G5 (PR #173 drop-safety sweep): every pair in PROVIDER_COVERAGE has
        its bronze cols either carried through, renamed, or intentionally dropped
        (via sources.yml). Leaving a gap entry here is documentation drift.

        Reopening a gap is allowed but must ship with a reason: add a per-col
        comment + either document the col in models.yml or add it to RENAMES.
        """
        assert INITIAL_BRONZE_STAGING_GAPS == {}, (
            f"INITIAL_BRONZE_STAGING_GAPS is non-empty — documentation drift:\n"
            f"  {INITIAL_BRONZE_STAGING_GAPS}\n"
            "Close by either (a) adding the col to the staging models.yml, "
            "(b) adding a RENAMES entry, or (c) documenting the drop reason "
            "with a comment."
        )
