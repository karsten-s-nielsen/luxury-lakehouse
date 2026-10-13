"""Stage-2 window-bounded scoring seam (ADR-090).

TDD for the seam that bounds Stage-2 scoring by WINDOW size (not unit size): per-mart scoring halo
(`score_halo_frames`), the built-frame scoring-window assignment (`assign_score_windows`), the
whole-unit invariant precompute (`precompute_unit_invariants`), and the windowed scoring dispatch
(`windowed_score_sdf`). Pure cores are tested here on fixtures; Spark plumbing on the Docker/live path.
"""

from __future__ import annotations

import math

import pytest

from analytics.action_context.frame_windows import halo_frames, provider_hz, score_halo_frames

# ── Task A.1 — per-mart scoring halo ────────────────────────────────────────


def test_score_halo_off_ball_is_pre_seconds():
    # off_ball reaches pre_seconds (1.5 s) before the action -> ceil(1.5*hz).
    assert score_halo_frames("off_ball_runs", "skillcorner") == math.ceil(1.5 * provider_hz("skillcorner")) == 15
    assert score_halo_frames("off_ball_runs", "idsse") == 38  # ceil(1.5*25)
    assert score_halo_frames("off_ball_runs", "gradientsports") == 15  # ceil(1.5*10)


def test_score_halo_off_ball_vs_build_halo_per_provider():
    # TM-SPEC-01: off_ball halo exceeds the BUILD halo for idsse/GS, but is <= it for skillcorner
    # (the provider of all 8 OOM units) — the design does NOT rest on "exceeds the build halo".
    assert score_halo_frames("off_ball_runs", "idsse") > halo_frames("idsse", provider_hz("idsse"))  # 38 > 22
    assert score_halo_frames("off_ball_runs", "gradientsports") > halo_frames(
        "gradientsports", provider_hz("gradientsports")
    )  # 15 > 9
    assert score_halo_frames("off_ball_runs", "skillcorner") <= halo_frames(
        "skillcorner", provider_hz("skillcorner")
    )  # 15 <= 16


def test_score_halo_gkdv_is_zero_single_frame_arms():
    assert score_halo_frames("gkdv_observations", "skillcorner") == 0
    assert score_halo_frames("gkdv_observations", "idsse") == 0


def test_score_halo_link_tolerance_marts():
    # action-linked marts need only the asof link-tolerance (0.2 s) neighbourhood.
    for mart in ("action_defensive", "defensive_credit_attributions", "rest_defense", "gk_decision"):
        assert score_halo_frames(mart, "skillcorner") == math.ceil(0.2 * provider_hz("skillcorner")) == 2
        assert score_halo_frames(mart, "idsse") == math.ceil(0.2 * provider_hz("idsse")) == 5


def test_score_halo_unknown_mart_raises():
    with pytest.raises(ValueError, match="scoring halo"):
        score_halo_frames("not_a_mart", "skillcorner")


# ── Task A.2 — built-frame scoring-window assignment (pure) ──────────────────


def _built_fixture(n_frames: int):
    import pandas as pd

    rows = [
        dict(game_id="g", period_id=1, frame_id=fid, player_id=pid, x=float(fid), y=0.0)
        for fid in range(n_frames)
        for pid in (1, 2)
    ]
    return pd.DataFrame(rows)


def test_assign_score_windows_off_ball_halo_replication():
    from analytics.action_context.frame_windows import assign_score_window_ids

    out = assign_score_window_ids(_built_fixture(40), "off_ball_runs", "skillcorner", target_window_frames=20)
    core = out[~out["_is_halo"]]
    assert core.groupby("frame_id").ngroups == 40  # every frame core exactly once
    assert set(core["_window_id"].unique()) == {0, 1}
    halo = out[out["_is_halo"]]
    # left-halo: first 15 ordinals of w1 (frames 20..34) feed w0; right-halo: last 15 of w0 (5..19) feed w1
    assert set(halo.loc[halo["_window_id"] == 0, "frame_id"]) == set(range(20, 35))
    assert set(halo.loc[halo["_window_id"] == 1, "frame_id"]) == set(range(5, 20))


def test_assign_score_windows_gkdv_cores_only():
    from analytics.action_context.frame_windows import assign_score_window_ids

    out = assign_score_window_ids(_built_fixture(40), "gkdv_observations", "skillcorner", target_window_frames=20)
    assert not out["_is_halo"].any()  # halo 0 -> no replication
    assert len(out) == len(_built_fixture(40))


# ── Task A.3 — whole-unit invariant precompute (pure) ───────────────────────


def test_compute_unit_invariants_equals_whole_unit_sk():
    import pandas as pd
    from silly_kicks.tracking import infer_ball_carrier, resolve_defended_goals

    from ingestion.tracking_marts_dispatch import compute_unit_invariants
    from tests.tracking_marts import _oracle

    inp = _oracle.build_inputs("idsse")
    inv = compute_unit_invariants(inp.frames, need_carrier=True)
    assert inv.goal_map == resolve_defended_goals(inp.frames)
    assert inv.carrier is not None
    pd.testing.assert_frame_equal(inv.carrier, infer_ball_carrier(inp.frames))


def test_compute_unit_invariants_no_carrier():
    from ingestion.tracking_marts_dispatch import compute_unit_invariants
    from tests.tracking_marts import _oracle

    inp = _oracle.build_inputs("idsse")
    inv = compute_unit_invariants(inp.frames, need_carrier=False)
    assert inv.carrier is None
    assert inv.goal_map is not None


# ── Task B — per-mart neutrality (windowed union == whole-unit) ─────────────


def _canary_unit():
    from analytics.action_context.local.parquet_sources import ParquetMatchMetadataSource
    from ingestion import tracking_marts_canary as c

    root = c.canary_fixture_root()
    wu = c.canary_work_unit()
    inp = c.build_inputs(root, wu)
    meta = ParquetMatchMetadataSource(root).metadata(wu)
    return "idsse", inp, wu.match_id, meta


def _target_for_at_least_2_windows(inp, mart, provider):
    n = inp.frames["frame_id"].nunique()
    return max(2, n // 3)


def _restricted_whole(mart, provider, inp, match_id, meta):
    """The whole-unit baseline on the SAME coverage-restricted actions the windowed path uses (ADR-090
    owner decision): beyond-frame-coverage actions are excluded identically in both paths."""
    import dataclasses

    from ingestion import tracking_marts_canary as c
    from ingestion.tracking_marts_dispatch import restrict_actions_to_frame_coverage

    inp_r = dataclasses.replace(inp, actions=restrict_actions_to_frame_coverage(inp.actions, inp.frames))
    return c.score_one_mart(mart, provider, inp_r, inp.frames, match_id=match_id, meta=meta)


def test_coverage_guard_drops_beyond_coverage_actions_nonvacuous():
    # guard-of-the-guard: the canary fixture DOES contain a beyond-frame-coverage action (the shot 1.4 s
    # past the last frame), so the exclusion is actually exercised — else the neutrality tests below are
    # not testing the guard. (ADR-090: beyond-coverage actions are out-of-contract, excluded both paths.)
    from ingestion.tracking_marts_dispatch import restrict_actions_to_frame_coverage

    _provider, inp, _match_id, _meta = _canary_unit()
    kept = restrict_actions_to_frame_coverage(inp.actions, inp.frames)
    assert len(kept) < len(inp.actions), "fixture has no beyond-coverage action — guard is vacuous here"


#: Lakehouse-only marts (no whole-unit invariant) — windowable with the §4.2 scoring halo alone.
_LAKEHOUSE_ONLY_MARTS = ("off_ball_runs", "action_defensive", "defensive_credit_attributions")


@pytest.mark.parametrize("mart", _LAKEHOUSE_ONLY_MARTS)
def test_lakehouse_only_mart_windowed_equals_whole_unit(mart):
    from analytics.action_context.frame_windows import assign_score_window_ids
    from ingestion.tracking_marts_dispatch import score_unit_windowed
    from tests.tracking_marts._oracle import assert_mart_equal

    provider, inp, match_id, meta = _canary_unit()
    target = _target_for_at_least_2_windows(inp, mart, provider)
    # non-vacuity: the fixture must span >= 2 scoring windows and produce >= 1 scored row
    tagged = assign_score_window_ids(inp.frames, mart, provider, target_window_frames=target)
    assert tagged["_window_id"].nunique() >= 2, f"{mart}: fixture does not span >= 2 windows (vacuous)"
    whole = _restricted_whole(mart, provider, inp, match_id, meta)
    assert len(whole) >= 1, f"{mart}: whole-unit produced 0 rows (vacuous)"
    windowed = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert_mart_equal(windowed, whole, mart)


# ── Task B.3 / E.1 / E.2 — invariant-injection marts (rest_defense, gk_decision, gkdv) ──────────────

_INVARIANT_MARTS = (
    "rest_defense",
    "gk_decision",
    # gkdv: the possession_stride survivor set (a whole-unit rank) is injected via the sk 4.133
    # `select_gkdv_scored_frames` + `build_ghost_frames(scored_frame_keys=)` seam (ADR-119), alongside the
    # 4.132 GoalMap + carrier injection — so windowed == whole-unit byte-identical, same as the others.
    "gkdv_observations",
)


@pytest.mark.parametrize("mart", _INVARIANT_MARTS)
def test_invariant_mart_windowed_equals_whole_unit(mart):
    """Windowed (whole-unit GoalMap/carrier injected) == whole-unit (internal recompute), byte-identical."""
    from analytics.action_context.frame_windows import assign_score_window_ids
    from ingestion.tracking_marts_dispatch import score_unit_windowed
    from tests.tracking_marts._oracle import assert_mart_equal

    provider, inp, match_id, meta = _canary_unit()
    target = _target_for_at_least_2_windows(inp, mart, provider)
    tagged = assign_score_window_ids(inp.frames, mart, provider, target_window_frames=target)
    assert tagged["_window_id"].nunique() >= 2, f"{mart}: fixture does not span >= 2 windows (vacuous)"
    whole = _restricted_whole(mart, provider, inp, match_id, meta)
    assert len(whole) >= 1, f"{mart}: whole-unit produced 0 rows (vacuous)"
    windowed = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert_mart_equal(windowed, whole, mart)


# ── S2W-SPEC-01 (Reviewer B) — the injections are LOAD-BEARING (perturb → output changes) ──────────
# No committed fixture diverges per-window (the sk 7.1% GoalMap figure is per-FRAME; a ~150-frame window's
# mean GK-x does not flip the defended end), so windowed==whole-unit alone cannot prove the injection is
# wired (goal_map=None recompute == injected whole-unit on a non-divergent fixture — B's red-green). Instead
# INJECT the divergence: perturb each whole-unit invariant (like the sk seam's own inverted-GoalMap test)
# and assert the windowed output CHANGES. Red-green: drop the injection (None→recompute) and the perturbation
# is ignored → output unchanged → these FAIL. Together with the sk-side load-bearing tests (the seam consumes
# the value) this proves the full chain.


def _invert_goal_map(gm):
    from types import MappingProxyType

    from silly_kicks.spadl import config as spadlcfg
    from silly_kicks.tracking import GoalMap

    fl = float(spadlcfg.field_length)

    def _swap(m):
        return MappingProxyType({k: fl - v for k, v in m.items()})

    return GoalMap(_swap(gm.resolved), _swap(gm.guessed), gm.unresolved)


def test_goal_map_injection_is_load_bearing(monkeypatch):
    import ingestion.tracking_marts_dispatch as d
    from tests.tracking_marts._oracle import sorted_for_compare

    provider, inp, match_id, meta = _canary_unit()
    mart = "rest_defense"  # goal_map-dependent + produces rows on the canary unit
    target = _target_for_at_least_2_windows(inp, mart, provider)
    correct = d.score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert len(correct) >= 1, "vacuous: correct produced 0 rows"

    orig = d.compute_unit_invariants
    monkeypatch.setattr(
        d,
        "compute_unit_invariants",
        lambda frames, *, need_carrier: d.UnitInvariants(
            goal_map=_invert_goal_map(orig(frames, need_carrier=need_carrier).goal_map),
            carrier=orig(frames, need_carrier=need_carrier).carrier,
        ),
    )
    wrong = d.score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert not sorted_for_compare(correct).equals(sorted_for_compare(wrong)), (
        "goal_map injection is NOT load-bearing — inverting the injected whole-unit GoalMap did not change the "
        "windowed output, so score_unit_windowed is not threading goal_map to the scorer (S2W-SPEC-01)"
    )


def test_scored_frame_keys_injection_is_load_bearing(monkeypatch):
    import silly_kicks.gkdv as skg

    from ingestion.tracking_marts_dispatch import score_unit_windowed
    from tests.tracking_marts._oracle import sorted_for_compare

    provider, inp, match_id, meta = _canary_unit()
    mart = "gkdv_observations"
    target = _target_for_at_least_2_windows(inp, mart, provider)
    correct = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert len(correct) >= 2, "vacuous: need >=2 gkdv rows to drop a subset"

    orig = skg.select_gkdv_scored_frames
    # perturb: drop the first survivor frame -> gkdv must score strictly fewer keeper-frames
    monkeypatch.setattr(
        skg, "select_gkdv_scored_frames", lambda frames, **kw: orig(frames, **kw).iloc[1:].reset_index(drop=True)
    )
    wrong = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert not sorted_for_compare(correct).equals(sorted_for_compare(wrong)), (
        "scored_frame_keys injection is NOT load-bearing — a reduced survivor set did not change gkdv output "
        "(score_unit_windowed is not threading scored_frame_keys) (S2W-SPEC-01 / ADR-119)"
    )


def test_carrier_injection_is_load_bearing(monkeypatch):
    import ingestion.tracking_marts_dispatch as d
    from tests.tracking_marts._oracle import sorted_for_compare

    provider, inp, match_id, meta = _canary_unit()
    mart = "gkdv_observations"  # carrier drives the DAS-arm possession
    target = _target_for_at_least_2_windows(inp, mart, provider)
    correct = d.score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert len(correct) >= 1, "vacuous: correct produced 0 rows"

    orig = d.compute_unit_invariants

    def _perturb(frames, *, need_carrier):
        inv = orig(frames, need_carrier=need_carrier)
        assert inv.carrier is not None
        car = inv.carrier.copy()
        # flip the ball-carrier team on every frame -> possession inverted -> domain/arms change
        if "ball_carrier_team_id" in car.columns:
            vals = car["ball_carrier_team_id"].dropna().unique().tolist()
            if len(vals) >= 2:
                car["ball_carrier_team_id"] = car["ball_carrier_team_id"].map({vals[0]: vals[1], vals[1]: vals[0]})
        return d.UnitInvariants(goal_map=inv.goal_map, carrier=car)

    monkeypatch.setattr(d, "compute_unit_invariants", _perturb)
    wrong = d.score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert not sorted_for_compare(correct).equals(sorted_for_compare(wrong)), (
        "carrier injection is NOT load-bearing — inverting the injected whole-unit carrier did not change gkdv "
        "output (score_unit_windowed is not threading carrier) (S2W-SPEC-01)"
    )


# ── Halo sufficiency + tightness (plan B.1) — the off_ball scoring halo carries EXACTLY pre_seconds ──
# The neutrality tests are the SCORER-level half (windowed == whole-unit at the correct halo). This is the
# STRUCTURAL half: the off_ball halo must carry the full pre_seconds (1.5 s) look-back into the next window
# — no fewer (correctness) and tight (halo-1 drops the earliest look-back frame, so it is not vacuously
# oversized). Deterministic on a synthetic built fixture — NON-VACUOUS by construction, unlike a scoring-
# divergence on the canary unit (whose off-ball runs happen not to straddle a boundary at a 3-window target,
# so a shrink-to-0 scoring test passes vacuously — the HFW-SPEC-03 "detector must be able to fail" trap).


def test_off_ball_halo_covers_pre_seconds_lookback_and_is_tight():
    from analytics.action_context.frame_windows import (
        _HALO_COL,
        _WINDOW_COL,
        _assign_windows_with_halo,
        score_halo_frames,
    )

    halo = score_halo_frames("off_ball_runs", "idsse")  # ceil(1.5 * 25) = 38 frames of pre-action look-back
    assert halo == 38
    target = 100  # 2 windows, core boundary at frame_id == target (dense 0-based ordinal)
    frames = _built_fixture(target * 2)

    # Correct halo: window 1's left-halo must carry the full look-back [target-halo .. target-1] from window 0.
    tagged = _assign_windows_with_halo(frames, frame_col="frame_id", target_window_frames=target, halo=halo)
    w1_halo = set(tagged.loc[(tagged[_WINDOW_COL] == 1) & tagged[_HALO_COL], "frame_id"])
    assert w1_halo == set(range(target - halo, target)), (
        "off_ball halo must carry the FULL pre_seconds look-back into the next window (sufficiency)"
    )
    # Tightness: halo-1 drops the EARLIEST look-back frame → the halo is sized exactly, not vacuously large.
    short = _assign_windows_with_halo(frames, frame_col="frame_id", target_window_frames=target, halo=halo - 1)
    w1_halo_short = set(short.loc[(short[_WINDOW_COL] == 1) & short[_HALO_COL], "frame_id"])
    assert (target - halo) not in w1_halo_short, "halo-1 must drop the earliest look-back frame (halo is tight)"


def test_off_ball_halo_is_load_bearing_end_to_end(monkeypatch):
    """S2W-IMPL-02 / S2W-FULL-01 (spec §6 / plan B.1, SCORER-level): the off_ball SCORER's output depends on
    the halo frames across a window boundary. At a small target an off-ball run's 1.5 s (38-frame) look-back
    straddles a window boundary (swept 2026-10-12: diverges at every target <= 200): windowed@correct-halo ==
    whole-unit (neutrality), but windowed@halo=0 (look-back starved) DIFFERS. This exercises off_ball-across-
    boundary behaviourally (the structural test above proves the halo is sized/tight; this proves the scorer
    consumes it — it is NOT injection-independent)."""
    import analytics.action_context.frame_windows as fw
    from ingestion.tracking_marts_dispatch import score_unit_windowed
    from tests.tracking_marts._oracle import assert_mart_equal, sorted_for_compare

    provider, inp, match_id, meta = _canary_unit()
    mart = "off_ball_runs"
    target = 200  # small enough that an off-ball run's look-back straddles a window boundary on the canary
    whole = _restricted_whole(mart, provider, inp, match_id, meta)
    assert len(whole) >= 1, "vacuous: off_ball produced 0 rows"

    correct = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert_mart_equal(correct, whole, f"{mart} windowed@correct-halo==whole")  # neutrality at this target

    orig = fw.score_halo_frames
    monkeypatch.setattr(fw, "score_halo_frames", lambda m, p: 0 if m == mart else orig(m, p))
    starved = score_unit_windowed(mart, provider, inp, match_id=match_id, meta=meta, target_window_frames=target)
    assert not sorted_for_compare(starved).equals(sorted_for_compare(whole)), (
        "off_ball halo is NOT load-bearing end-to-end — zeroing it did not change the scored output, so no "
        "off-ball run's look-back crosses a window boundary at this target (vacuous)"
    )


# ── Stage-2a driver precompute (ADR-090) — driver values == whole-unit, through the SPILL round-trip ──
# precompute_stage2a is the PRODUCTION driver-side counterpart of score_unit_windowed's in-memory setup:
# it reads the spill (not inp.frames) and produces the SAME whole-unit invariants + coverage-restricted
# actions + action→window routing that the windowed Spark dispatch injects. The gkdv=True test below
# exercises the FULL spill read (carrier needs vx/vy/ball_state) + stride-survivor round-trip fidelity; the
# column-PRUNED goal_map-only read (the non-gkdv branch) is covered by the gkdv=False test further down. A
# lossy read for the GoalMap / carrier / stride survivors would surface HERE against a full-frame reference.


def test_precompute_stage2a_equals_whole_unit(tmp_path):
    import os

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from silly_kicks.gkdv import select_gkdv_scored_frames
    from silly_kicks.tracking import infer_ball_carrier, resolve_defended_goals

    import ingestion.tracking_marts_dispatch as d
    from analytics.action_context.frame_windows import assign_score_window_ids

    provider, inp, _match_id, _meta = _canary_unit()
    target = _target_for_at_least_2_windows(inp, "gkdv_observations", provider)

    # Spill the built frames to a parquet directory exactly as Stage-1 does; Stage-2a reads IT (not inp).
    spill_dir = str(tmp_path / "spill")
    os.makedirs(spill_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(inp.frames, preserve_index=False), os.path.join(spill_dir, "part-0.parquet"))

    got = d.precompute_stage2a(None, spill_dir, inp.actions, provider, gkdv_enabled=True, target_window_frames=target)

    # Reference = the SAME sk functions over the SPILL content (isolates orchestration + the invariant
    # column-prune from round-trip fidelity, which the Docker spill test owns). A prune that drops a column
    # resolve_defended_goals / infer_ball_carrier needs makes got.invariants diverge from these.
    ref = d._read_spill_table(spill_dir)
    gm = resolve_defended_goals(ref)
    carrier = infer_ball_carrier(ref)
    assert got.invariants.goal_map == gm
    assert got.invariants.carrier is not None
    assert got.scored_frame_keys is not None
    pd.testing.assert_frame_equal(got.invariants.carrier, carrier)
    pd.testing.assert_frame_equal(got.scored_frame_keys, select_gkdv_scored_frames(ref, carrier=carrier, goal_map=gm))

    restricted = d.restrict_actions_to_frame_coverage(inp.actions, ref)
    pd.testing.assert_frame_equal(got.restricted_actions.reset_index(drop=True), restricted.reset_index(drop=True))
    tagged = assign_score_window_ids(ref, "gkdv_observations", provider, target_window_frames=target)
    assert got.action_window == d.route_actions_to_windows(restricted, tagged)

    # Non-vacuity: the fixture must span >= 2 windows, routing must distribute actions across >= 2 windows,
    # and the coverage restriction must actually drop the fixture's beyond-coverage action.
    assert tagged["_window_id"].nunique() >= 2, "vacuous: fixture does not span >= 2 scoring windows"
    assert len(set(got.action_window.values())) >= 2, "vacuous: all actions routed to one window"
    assert len(restricted) < len(inp.actions), "vacuous: no beyond-coverage action dropped"


def test_precompute_stage2a_non_gkdv_uses_pruned_goal_map_only_read(tmp_path):
    """S2W-IMPL-03: the non-gkdv branch takes the PRUNED `_INVARIANT_COLUMNS` goal_map-only read (no carrier /
    stride). Covers the branch the gkdv=True test above does not (it full-reads). The GoalMap from the pruned
    read must still equal `resolve_defended_goals` over the full spill (the prune is sufficient for it)."""
    import os

    import pyarrow as pa
    import pyarrow.parquet as pq
    from silly_kicks.tracking import resolve_defended_goals

    import ingestion.tracking_marts_dispatch as d

    provider, inp, _match_id, _meta = _canary_unit()
    target = _target_for_at_least_2_windows(inp, "off_ball_runs", provider)
    spill_dir = str(tmp_path / "spill")
    os.makedirs(spill_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(inp.frames, preserve_index=False), os.path.join(spill_dir, "part-0.parquet"))

    got = d.precompute_stage2a(None, spill_dir, inp.actions, provider, gkdv_enabled=False, target_window_frames=target)

    ref = d._read_spill_table(spill_dir)
    assert got.invariants.goal_map == resolve_defended_goals(ref), "pruned goal_map-only read must match the full read"
    assert got.invariants.carrier is None, "non-gkdv must not resolve a carrier"
    assert got.scored_frame_keys is None, "non-gkdv must not resolve stride survivors"
    # routing + coverage-restriction still run for the action-grain marts in the non-gkdv path.
    assert len(set(got.action_window.values())) >= 1
    assert len(got.restricted_actions) <= len(inp.actions)
