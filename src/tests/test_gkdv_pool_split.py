"""Golden test for the gkdv scoring/pooling SPLIT (Task 2, ADR-037 tracking-marts drain fan-out).

The old ``gkdv_writer.run_pipeline`` scored every unit on the driver, stamped four identity columns
inline, concatenated, and pooled once. The drain refactor factors the per-unit body into
:func:`score_gkdv_unit` (called by the per-unit drain worker) and REUSES the existing whole-corpus
:func:`pool_keepers` reduce unchanged (no new ``pool_gkdv_observations`` — ``pool_keepers`` already IS the
reduce). This test pins that the split reproduces the old end-stage EXACTLY: per-unit
``score_gkdv_unit`` + ``pd.concat`` + ``pool_keepers`` equals the reference (the original inline stamp +
the same ``pool_keepers``).

``score_unit`` (the expensive per-frame accessible-space / pitch-control scoring) is monkeypatched to a
deterministic per-unit observation frame — the split's correctness is in the STAMPING + reduce wiring,
which is pure Python; the scoring math is Part-B/live-validated and unchanged (imported verbatim).
"""

from __future__ import annotations

import pandas as pd

from ingestion import gkdv_writer
from ingestion.gkdv_writer import pool_keepers, score_gkdv_unit


class _DummyReport:
    """Stand-in for silly-kicks' GkdvReport (score_gkdv_unit ignores it)."""

    n_frames_in = 0
    n_frames_scored = 0


def _obs(player_ids, das, threat, periods, frames) -> pd.DataFrame:
    """A per-unit observation frame in build_keeper_observations' grain (pre-stamp)."""
    return pd.DataFrame(
        {
            "player_id": player_ids,
            "period_id": periods,
            "frame_id": frames,
            "delta_das": das,
            "delta_threat_suppression": threat,
        }
    )


# (provider, match_id, competition_id, season_id, per-unit observations) — two competitions, two seasons,
# a repeated keeper across two games (so n_games > 1 and the (comp, season) grouping is exercised).
_CORPUS: list[tuple[str, str, str, str | None, pd.DataFrame]] = [
    ("skillcorner", "M1", "C1", "2023", _obs(["gk1", "gk1"], [-0.5, -0.3], [-0.2, -0.1], [1, 2], [10, 20])),
    ("skillcorner", "M2", "C1", "2023", _obs(["gk1", "gk2"], [-0.4, 0.0], [-0.15, 0.0], [1, 1], [11, 12])),
    ("idsse", "G1", "bl", None, _obs(["gkX", "gkX"], [-0.2, -0.1], [-0.05, -0.02], [1, 1], [5, 6])),
]


def test_per_unit_score_then_pool_matches_run_pipeline_end_stage(monkeypatch) -> None:
    """score_gkdv_unit + concat + pool_keepers == the old run_pipeline end-stage (value equality)."""
    obs_iter = iter([row[4] for row in _CORPUS])
    monkeypatch.setattr(gkdv_writer, "score_unit", lambda *a, **k: (next(obs_iter), _DummyReport()))

    # NEW split path: score each unit via score_gkdv_unit, concat, then the reused pool_keepers reduce.
    new_parts = [
        score_gkdv_unit(
            pd.DataFrame(),  # frames unused (score_unit is stubbed)
            "home",
            None,
            data_source=prov,
            match_id=mid,
            competition_id=comp,
            season_id=season,
        )
        for prov, mid, comp, season, _ in _CORPUS
    ]
    new_pooled = pool_keepers(pd.concat(new_parts, ignore_index=True), min_nonzero=1, min_games=1)

    # REFERENCE: the ORIGINAL run_pipeline loop-body stamp (gkdv_writer.py pre-refactor) + same reduce.
    ref_parts = []
    for prov, mid, comp, season, obs in _CORPUS:
        o = obs.copy()
        o["data_source"] = prov
        o["game_id"] = mid  # native match id -> aggregate_by_keeper n_games
        o["competition_id"] = comp
        o["season_id"] = season
        ref_parts.append(o)
    ref_pooled = pool_keepers(pd.concat(ref_parts, ignore_index=True), min_nonzero=1, min_games=1)

    pd.testing.assert_frame_equal(new_pooled, ref_pooled)
    # Non-vacuous: the pooled output actually carries keeper rows (a bug returning empty must fail).
    assert not new_pooled.empty
    assert set(new_pooled["player_id"]) == {"gk1", "gk2", "gkX"}


def test_score_gkdv_unit_stamps_the_four_identity_columns(monkeypatch) -> None:
    """score_gkdv_unit stamps data_source / game_id(=match_id) / competition_id / season_id, keeps the rest."""
    raw = _obs(["gk1"], [-0.5], [-0.2], [1], [10])
    monkeypatch.setattr(gkdv_writer, "score_unit", lambda *a, **k: (raw, _DummyReport()))

    stamped = score_gkdv_unit(
        pd.DataFrame(), "home", None, data_source="idsse", match_id="G9", competition_id="bl", season_id=None
    )

    assert (stamped["data_source"] == "idsse").all()
    assert (stamped["game_id"] == "G9").all()  # native match id
    assert (stamped["competition_id"] == "bl").all()
    assert stamped["season_id"].isna().all()
    # The observation grain columns survive unchanged.
    assert stamped["player_id"].tolist() == ["gk1"]
    assert stamped["delta_das"].tolist() == [-0.5]
    # score_gkdv_unit returns a COPY — the caller's raw frame is not mutated with identity columns.
    assert "data_source" not in raw.columns


def test_score_gkdv_unit_empty_observations_keeps_full_columns(monkeypatch) -> None:
    """An empty score_unit result stamps cleanly (no scored keepers is a legitimate zero-row unit)."""
    empty = _obs([], [], [], [], [])
    monkeypatch.setattr(gkdv_writer, "score_unit", lambda *a, **k: (empty, _DummyReport()))

    stamped = score_gkdv_unit(
        pd.DataFrame(), "home", None, data_source="idsse", match_id="G9", competition_id="bl", season_id="2023"
    )

    assert stamped.empty
    for col in ("data_source", "game_id", "competition_id", "season_id", "player_id", "delta_das"):
        assert col in stamped.columns
