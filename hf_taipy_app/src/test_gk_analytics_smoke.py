"""Property/smoke test through the live GK Analytics read path (NOT a frozen golden).

Operator/live-run gate — NOT a CI gate. conftest sets LAKEBASE_HOST="test-host"; the real-host
predicate below skips unless a real host is present. The CI regression guard for the IDSSE fix is
the dbt singular test (assert_psxg_pooled_keeps_idsse) + the pure text guard
(src/tests/test_gk_pooled_join_null_safe.py). Asserts invariants, not expected values.
"""

import os

import pytest

pytest.importorskip("plotly")

_HOST = os.environ.get("LAKEBASE_HOST", "")
if not _HOST or _HOST == "test-host" or "example" in _HOST:
    pytest.skip("needs a real Lakebase host (operator/live run only)", allow_module_level=True)

from pages.gk_analytics import page_config  # noqa: E402
from queries.gk_analytics import (  # noqa: E402
    fetch_distribution_profile,
    fetch_gk_competitions,
    fetch_gk_keepers,
    fetch_goals_prevented,
)


def _comp_key(name_contains: str) -> int:
    comps = fetch_gk_competitions()
    row = comps[comps["competition_name"].str.contains(name_contains, case=False, na=False)].iloc[0]
    return int(row["competition_key"])


def test_competition_lov_is_tracking_only():
    comps = fetch_gk_competitions()
    assert not comps.empty
    names = " ".join(comps["competition_name"].tolist())
    assert "World Cup" in names or "Bundesliga" in names or "A-League" in names


def test_distribution_profile_two_axes_present_and_floored():
    df = fetch_distribution_profile(_comp_key("World Cup"))
    assert not df.empty and len(df) >= 8, "WC cohort should have many qualifying keepers"
    for col in ("share_adds_threat", "mean_progress_m", "mean_completion", "mean_xtgk", "n_distributions"):
        assert col in df.columns
    assert (df["n_distributions"] >= 20).all()  # floor honoured
    # share-adds-threat genuinely varies (the whole point — the preset ladder did not)
    assert df["share_adds_threat"].max() - df["share_adds_threat"].min() > 0.05
    assert df["player_display_name"].map(lambda s: isinstance(s, str) and not s.isdigit()).all()


def test_idsse_goals_prevented_present():
    """The Task-1.0 NULL-safe join keeps IDSSE; the unfixed pooled mart returned 0 IDSSE rows."""
    gp = fetch_goals_prevented(_comp_key("Bundesliga"))
    assert not gp.empty, "IDSSE goals-prevented rows missing — pooled-mart NULL-safe join regressed"
    r = gp.iloc[0]
    assert r["goals_prevented_ci_low"] <= r["goals_prevented"] <= r["goals_prevented_ci_high"]


def test_no_ranking_pctile_surfaced():
    gp = fetch_goals_prevented(_comp_key("World Cup"))
    assert "goals_prevented_pctile" not in gp.columns


def test_page_carries_tracking_cohort_scope_note():
    text = page_config.description + " " + page_config.empty_message
    assert "GradientSports" in text and "SkillCorner" in text and "IDSSE" in text
    assert "StatsBomb" in text  # explicitly states SB/WS keepers don't appear


def test_keeper_lov_uses_display_names_not_ids():
    keepers = fetch_gk_keepers(_comp_key("World Cup"))
    assert "player_display_name" in keepers.columns
    assert all(isinstance(n, str) and not n.isdigit() for n in keepers["player_display_name"].head(5))
