"""Unit tests for ``ingestion.gkdv_writer`` (Task 17h) on synthetic provenance / observations.

The expensive per-frame accessible-space + pitch-control scoring is validated live in Part B; these
tests pin the two correctness contracts that are pure-Python and must not silently regress:

* **review-4 B1 (HIGH, silent null-bias):** a dropped / off-domain frame is byte-identical across the
  actual/ghost legs, so differencing it yields ``delta == 0`` and biases every keeper aggregate toward
  the null. The writer must NEVER score such a frame. Proved two ways: (1) the selection helper excludes
  dropped + attacking-keeper rows, and (2) a call-spy shows the arms are invoked ONLY for the scored,
  defending-keeper frame — so a dropped frame never contributes an observation at all.
* **native-id passthrough + aggregate shape (review-4 B2):** ``pool_keepers`` carries the NATIVE
  ``(player_id, competition_id, season_id)`` unchanged (so ``stg_gkdv`` can resolve them to surrogates)
  and reproduces silly-kicks' NaN/zero-safe ``n_nonzero`` counting.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import silly_kicks.gkdv as gkdv

from ingestion.gkdv_writer import (
    POOLED_COLUMNS,
    _index_frames_by_key,
    _scored_defending_keepers,
    build_keeper_observations,
    pool_keepers,
)


def _provenance() -> pd.DataFrame:
    """3 rows: scored+defending (keep), scored+ATTACKING keeper (wrong keeper), DROPPED frame."""
    return pd.DataFrame(
        [
            {
                "game_id": 100,
                "period_id": 1,
                "frame_id": 1,
                "gk_team_id": 1,
                "defending_team_id": 1,
                "player_id": "gk_home",
                "drop_reason": pd.NA,
            },
            {
                "game_id": 100,
                "period_id": 1,
                "frame_id": 1,
                "gk_team_id": 2,
                "defending_team_id": 1,
                "player_id": "gk_away",
                "drop_reason": pd.NA,
            },
            {
                "game_id": 100,
                "period_id": 1,
                "frame_id": 2,
                "gk_team_id": 1,
                "defending_team_id": 1,
                "player_id": "gk_home",
                "drop_reason": "ball_far_from_attacked_goal",
            },
        ]
    )


def test_scored_defending_keepers_excludes_dropped_and_attacking() -> None:
    """review-4 B1 core guard: keep ONLY the scored, DEFENDING-keeper rows."""
    selected = _scored_defending_keepers(_provenance())

    assert len(selected) == 1
    assert selected.iloc[0]["player_id"] == "gk_home"
    assert selected.iloc[0]["frame_id"] == 1
    # The dropped frame (frame 2) — the byte-identical, zero-delta frame — is excluded.
    assert 2 not in set(selected["frame_id"])


def test_dropped_frames_are_never_scored(monkeypatch) -> None:
    """review-4 B1 end-to-end: the arms are called ONLY for the scored, defending frame.

    A call-spy is non-vacuous: it records exactly which frames were differenced, so a regression that
    differenced the dropped frame (contributing a 0) would show frame 2 in ``seen``.
    """
    frames = pd.DataFrame(
        [
            {"game_id": 100, "period_id": 1, "frame_id": 1, "is_ball": False, "team_id": 1},
            {"game_id": 100, "period_id": 1, "frame_id": 1, "is_ball": False, "team_id": 2},
            {"game_id": 100, "period_id": 1, "frame_id": 1, "is_ball": True, "team_id": None},
            {"game_id": 100, "period_id": 1, "frame_id": 2, "is_ball": False, "team_id": 1},
            {"game_id": 100, "period_id": 1, "frame_id": 2, "is_ball": False, "team_id": 2},
        ]
    )
    cf = frames.copy()

    seen: list[int] = []

    def _spy_das(actual, ghost, *, attacking_team_id, **_kw):
        seen.append(int(actual["frame_id"].iloc[0]))
        assert attacking_team_id == 2  # the non-defending team on the pitch
        return -0.5

    monkeypatch.setattr(gkdv, "delta_das", _spy_das)

    obs = build_keeper_observations(frames, cf, _provenance(), xt=None, want_threat=False)

    assert seen == [1], f"arms scored frames {seen} — a dropped frame was differenced (contributes a 0)"
    assert list(obs["frame_id"]) == [1]
    assert list(obs["player_id"]) == ["gk_home"]
    assert (obs["delta_das"] == -0.5).all()


def _observations(deltas_das, deltas_threat, games=None) -> pd.DataFrame:
    n = len(deltas_das)
    return pd.DataFrame(
        {
            "player_id": ["gk1"] * n,
            "game_id": games if games is not None else [f"M{i}" for i in range(n)],
            "data_source": ["skillcorner"] * n,
            "competition_id": ["C1"] * n,
            "season_id": ["2023"] * n,
            "delta_das": deltas_das,
            "delta_threat_suppression": deltas_threat,
        }
    )


def test_pool_keepers_columns_dtypes_and_native_passthrough() -> None:
    obs = _observations([-0.5, 0.0, -0.3], [-0.2, -0.1, 0.0], games=["M1", "M1", "M2"])
    pooled = pool_keepers(obs, min_nonzero=1, min_games=1)

    assert list(pooled.columns) == list(POOLED_COLUMNS)
    assert len(pooled) == 1
    row = pooled.iloc[0]

    # Native ids pass through unchanged for stg_gkdv to resolve to surrogates (review-4 B2).
    assert row["data_source"] == "skillcorner"
    assert row["player_id"] == "gk1"
    assert row["competition_id"] == "C1"
    assert row["season_id"] == "2023"

    # aggregate_by_keeper semantics: n counts ALL sampled rows; n_nonzero excludes NaN AND zero.
    assert int(row["gkdv_delta_das_n"]) == 3
    assert int(row["gkdv_delta_das_n_nonzero"]) == 2  # the 0.0 is not informative
    assert int(row["gkdv_delta_das_n_games"]) == 2
    assert bool(row["gkdv_delta_das_gate_eligible"]) is True  # 2 >= 1 nonzero AND 2 >= 1 games

    # dtypes as the bronze schema expects.
    assert pooled["gkdv_delta_das_mean"].dtype == np.float64
    assert pd.api.types.is_integer_dtype(pooled["gkdv_delta_das_n"])
    assert pooled["gkdv_delta_das_gate_eligible"].dtype == bool


def test_dropped_frame_zero_biases_the_aggregate_toward_null() -> None:
    """The MECHANISM review-4 B1 exists to prevent: a spurious 0 drags the keeper mean toward the null.

    Same keeper, same real deltas; the ``buggy`` set adds the byte-identical dropped frame's 0.0. It
    must inflate ``n`` but NOT ``n_nonzero``, and pull the magnitude of the mean toward zero — which is
    exactly why the scoring path (proved above) excludes it.
    """
    correct = _observations([-0.5, -0.4], [-0.2, -0.2], games=["M1", "M2"])
    buggy = _observations([-0.5, -0.4, 0.0], [-0.2, -0.2, 0.0], games=["M1", "M2", "M2"])

    p_correct = pool_keepers(correct, min_nonzero=1, min_games=1).iloc[0]
    p_buggy = pool_keepers(buggy, min_nonzero=1, min_games=1).iloc[0]

    assert int(p_buggy["gkdv_delta_das_n"]) == int(p_correct["gkdv_delta_das_n"]) + 1
    assert int(p_buggy["gkdv_delta_das_n_nonzero"]) == int(p_correct["gkdv_delta_das_n_nonzero"])
    assert abs(p_buggy["gkdv_delta_das_mean"]) < abs(p_correct["gkdv_delta_das_mean"])


def test_frame_index_lookup_matches_the_boolean_mask() -> None:
    """The pre-built (game_id, period_id, frame_id) index returns EXACTLY what the old mask did.

    Guards the I1 refactor (mask-in-a-loop -> pre-indexed lookup): same rows AND same row order per key
    (the DAS arm's positional actual/ghost alignment depends on order), and a missing key -> empty slice.
    """
    frames = pd.DataFrame(
        [
            {"game_id": 100, "period_id": 1, "frame_id": 1, "is_ball": False, "team_id": 1, "x": 5.0},
            {"game_id": 100, "period_id": 1, "frame_id": 1, "is_ball": False, "team_id": 2, "x": 9.0},
            {"game_id": 100, "period_id": 1, "frame_id": 2, "is_ball": True, "team_id": None, "x": 7.0},
            {"game_id": 100, "period_id": 2, "frame_id": 1, "is_ball": False, "team_id": 1, "x": 3.0},
            {"game_id": 100, "period_id": 2, "frame_id": 1, "is_ball": False, "team_id": 2, "x": 8.0},
        ]
    )
    index = _index_frames_by_key(frames)

    for gid, per, fid in [(100, 1, 1), (100, 1, 2), (100, 2, 1)]:
        mask = frames[(frames["game_id"] == gid) & (frames["period_id"] == per) & (frames["frame_id"] == fid)]
        looked = index.get((gid, per, fid), frames.iloc[0:0])
        pd.testing.assert_frame_equal(looked.reset_index(drop=True), mask.reset_index(drop=True))

    # A key the index does not carry -> empty same-schema slice (the old mask's empty result).
    assert index.get((999, 9, 9), frames.iloc[0:0]).empty


def test_pool_keepers_empty_input_returns_full_schema() -> None:
    empty = _observations([], []).iloc[0:0]
    pooled = pool_keepers(empty, min_nonzero=1, min_games=1)
    assert list(pooled.columns) == list(POOLED_COLUMNS)
    assert pooled.empty
