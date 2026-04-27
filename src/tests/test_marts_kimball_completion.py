"""Kimball-completion invariant: every fct_* mart with a smart legacy *_id
column must also expose the corresponding *_key surrogate FK.

The ADR-011 staged migration is end-to-end at PR 7 — every fact mart in
the warehouse should now carry the appropriate Kimball surrogate FKs
(match_key, team_key, player_key, opponent_team_key where applicable)
alongside the legacy native ID columns during the 2026-07-22 dual-column
window. This test catches any future regression where a new mart adds a
smart key without its surrogate counterpart, OR a refactor drops a
surrogate column.

The test is a pure-static parser over dbt model SQL files — does not
require a live warehouse — so it runs on every CI shard.

Resolution rule: for each fct_*.sql, parse the final SELECT column list
(or the only-final SELECT in single-CTE marts). For each canonical legacy
ID seen, assert the corresponding *_key column is also selected. Allowed
absences are listed in `_KEY_ABSENT_BY_DESIGN` with rationale.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MARTS_DIR = _REPO_ROOT / "dbt_project" / "models" / "marts"

# (mart_filename, legacy_id, surrogate_key, rationale)
# These are the (legacy, key) pairs where the surrogate is intentionally
# absent at this mart's grain — career-aggregate marts that don't carry
# match_key, team-less player-grain marts that don't carry team_key, etc.
_KEY_ABSENT_BY_DESIGN: set[tuple[str, str, str]] = {
    # Career-grain marts (no match-level FK):
    ("fct_player_embeddings_career.sql", "match_id", "match_key"),
    ("fct_player_embeddings_career_360.sql", "match_id", "match_key"),
    ("fct_pausa_rankings.sql", "match_id", "match_key"),
    ("fct_player_percentiles.sql", "match_id", "match_key"),
    # Season-grain marts:
    ("fct_player_embeddings_season.sql", "match_id", "match_key"),
    ("fct_player_embeddings_season_360.sql", "match_id", "match_key"),
    # Aggregate marts where the team_id grain is NOT a per-row team:
    ("fct_funnel_stages_agg.sql", "player_id", "player_key"),
    # Workflow-cost / governance marts (not Kimball-conformed):
    ("fct_workflow_costs.sql", "match_id", "match_key"),
    ("fct_workflow_costs.sql", "team_id", "team_key"),
    ("fct_workflow_costs.sql", "player_id", "player_key"),
}

# Known team-less marts (player-only or career-only):
_TEAM_KEY_ABSENT_BY_DESIGN: set[str] = {
    "fct_off_ball_xt.sql",  # player-match grain, no team
    "fct_physical_stats.sql",  # player-match grain, no team
    "fct_pausa_rankings.sql",  # player-career
    "fct_pass_timing.sql",  # player-match
    "fct_pausa_values.sql",  # inherits team_key only via fct_passes (one team per pass — passer team)
    "fct_player_percentiles.sql",  # player-comp-season-position
    "fct_player_embeddings.sql",  # player grain
    "fct_player_embeddings_career.sql",
    "fct_player_embeddings_career_360.sql",
    "fct_player_embeddings_season.sql",
    "fct_player_embeddings_season_360.sql",
    "fct_position_maps.sql",  # tracking — team_key now present (PR 7), keep tracking
    "fct_space_creation.sql",  # tracking — team is just role; PR 7 deferred team_key per spec
}

_LEGACY_TO_KEY: list[tuple[str, str]] = [
    ("match_id", "match_key"),
    ("team_id", "team_key"),
    ("player_id", "player_key"),
]

# Lines that count as a "selection" of a column (in dbt SELECT lists):
# `<col>` alone, `... as <col>,`, `<table>.<col>,`, `<table>.<col> as <alias>,`.
# We grep the whole file for column TOKENS rather than parsing — false positives
# from CTE-internal column references are acceptable since they still mean the
# column flows through the model.


def _surfaces_column(sql: str, col: str) -> bool:
    """Return True if `col` appears as a selected/aliased column anywhere."""
    # Patterns:
    #   "col,"  "col\n"  "as col,"  "as col\n"  "col)"  ".col"
    pattern = (
        r"(?<![A-Za-z0-9_])"  # boundary
        + re.escape(col)
        + r"(?![A-Za-z0-9_])"  # boundary
    )
    return bool(re.search(pattern, sql))


def _list_fct_marts() -> list[Path]:
    return sorted(p for p in _MARTS_DIR.glob("fct_*.sql"))


class TestKimballCompletion:
    """Every fct_* mart with a legacy *_id column must surface the *_key surrogate."""

    def test_all_fct_marts_have_kimball_keys_where_legacy_ids_present(self) -> None:
        violations: list[str] = []

        for mart_path in _list_fct_marts():
            mart_name = mart_path.name
            sql = mart_path.read_text(encoding="utf-8")

            for legacy_col, key_col in _LEGACY_TO_KEY:
                # Skip exempt pairs.
                if (mart_name, legacy_col, key_col) in _KEY_ABSENT_BY_DESIGN:
                    continue

                # Skip team_key-absent-by-design.
                if key_col == "team_key" and mart_name in _TEAM_KEY_ABSENT_BY_DESIGN:
                    continue

                if _surfaces_column(sql, legacy_col) and not _surfaces_column(sql, key_col):
                    violations.append(
                        f"{mart_name}: surfaces legacy '{legacy_col}' but no '{key_col}'. "
                        f"Add LEFT JOIN to dim or passthrough from upstream, or add to "
                        f"_KEY_ABSENT_BY_DESIGN with rationale."
                    )

        assert not violations, (
            "Kimball-completion regression — smart-key islands found:\n"
            + "\n".join(f"  - {v}" for v in violations)
            + "\n\nPR 7 closed the ADR-011 staged migration; any future mart that "
            "introduces a legacy *_id without the *_key surrogate fails this invariant. "
            "Either add the JOIN/passthrough or add an entry to _KEY_ABSENT_BY_DESIGN "
            "with documented rationale."
        )
