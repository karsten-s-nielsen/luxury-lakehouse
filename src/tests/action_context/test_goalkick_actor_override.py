"""Unit tests for ``_override_goalkick_actor_from_frames`` (silly-kicks 4.39.0).

The lakehouse-domain modeling helper that credits goal-kicks to the acting keeper,
overriding the carrier-derived NULL-taker fill with the frames-resolved acting GK
(``silly_kicks.tracking.acting_gk_from_frames``). Actor-analog of the possession
synthesis (``_fill_possession_from_set_piece_actions``): silly-kicks is a pure
resolver; the lakehouse decides WHEN to apply it. See the
2026-07-01-goalkick-actor-override handoff.

Contract under test (the resolver itself is silly-kicks' — mocked here):
- goal-kicks with a non-NaN resolved GK → player_id AND player_id_native overridden;
- non-goal-kick action types → never touched (even if the resolver returns a value);
- goal-kicks where the resolver is NaN → untouched (never blank a real/carrier taker);
- no goal-kicks in the batch → returned unchanged (early out);
- absent player_id_native column → only player_id written, no crash.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import silly_kicks.tracking as sk_tracking

from analytics.action_context.enrich import _override_goalkick_actor_from_frames


def _type_id(name: str) -> int:
    from silly_kicks.spadl.config import actiontypes

    return actiontypes.index(name)


_GOALKICK = _type_id("goalkick")
_PASS = _type_id("pass")


def _actions(rows: list[tuple[int, object, object]]) -> pd.DataFrame:
    """Each row: (type_id, player_id, player_id_native)."""
    return pd.DataFrame([{"type_id": t, "player_id": p, "player_id_native": pn} for t, p, pn in rows])


@pytest.fixture
def _patch_resolver(monkeypatch: pytest.MonkeyPatch):
    """Patch acting_gk_from_frames to return a caller-supplied per-index mapping."""

    def _install(mapping: dict[int, object]) -> None:
        def _fake(actions: pd.DataFrame, frames: pd.DataFrame, **_kw: object) -> pd.Series:
            return pd.Series([mapping.get(i, np.nan) for i in actions.index], index=actions.index)

        monkeypatch.setattr(sk_tracking, "acting_gk_from_frames", _fake, raising=True)

    return _install


def test_goalkick_actor_overridden_to_resolved_gk(_patch_resolver) -> None:
    # index 0 = goalkick credited (wrongly) to outfielder "OUT" by the carrier fill.
    out = _actions([(_GOALKICK, "OUT", None)])
    _patch_resolver({0: "GK1"})

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    assert res.loc[0, "player_id"] == "GK1"
    assert res.loc[0, "player_id_native"] == "GK1"  # both set so build_output's restore keeps it


def test_non_goalkick_never_touched(_patch_resolver) -> None:
    # A pass whose resolver would return a keeper must NOT be relabelled (open-play actor is real).
    out = _actions([(_PASS, "REAL_PASSER", "REAL_PASSER")])
    _patch_resolver({0: "GK1"})

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    assert res.loc[0, "player_id"] == "REAL_PASSER"
    assert res.loc[0, "player_id_native"] == "REAL_PASSER"


def test_goalkick_with_nan_resolver_untouched(_patch_resolver) -> None:
    # Event-only-style: resolver returns NaN → keep the existing taker (never blank it).
    out = _actions([(_GOALKICK, "EXISTING", "EXISTING")])
    _patch_resolver({})  # resolver returns NaN for index 0

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    assert res.loc[0, "player_id"] == "EXISTING"
    assert res.loc[0, "player_id_native"] == "EXISTING"


def test_mixed_batch_only_resolved_goalkicks_change(_patch_resolver) -> None:
    out = _actions(
        [
            (_GOALKICK, "OUT_A", None),  # 0: resolved → override
            (_PASS, "PASSER", "PASSER"),  # 1: pass → untouched
            (_GOALKICK, "OUT_B", None),  # 2: resolver NaN → untouched
        ]
    )
    _patch_resolver({0: "GK_A"})  # only index 0 resolves

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    assert list(res["player_id"]) == ["GK_A", "PASSER", "OUT_B"]
    assert list(res["player_id_native"]) == ["GK_A", "PASSER", None]


def test_no_goalkicks_returns_unchanged(_patch_resolver) -> None:
    out = _actions([(_PASS, "P1", "P1"), (_PASS, "P2", "P2")])
    _patch_resolver({0: "GK1", 1: "GK2"})

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    pd.testing.assert_frame_equal(res, out)


def test_absent_player_id_native_writes_player_id_only(_patch_resolver) -> None:
    out = pd.DataFrame([{"type_id": _GOALKICK, "player_id": "OUT"}])  # no player_id_native col
    _patch_resolver({0: "GK1"})

    res = _override_goalkick_actor_from_frames(out, pd.DataFrame())

    assert res.loc[0, "player_id"] == "GK1"
    assert "player_id_native" not in res.columns
