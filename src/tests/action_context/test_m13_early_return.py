"""Unit tests for the M13 early-return short-circuit in ``enrich_batch``.

When a frame batch has zero owned actions (all buffer-windowed actions belong
to adjacent batches per the M13 single-owner partition), ``enrich_batch`` must
short-circuit BEFORE invoking the expensive 20-step enrich chain. The dead-ball
fixtures (J03WN1, J03WOH per `test_dead_ball_batches.py`) exercise this in
integration; this test asserts the surgical invariant: ``_enrich_tracking_match``
is NEVER called when ``_owned_action_ids`` returns an empty set.

Pre-refactor, a zero-owned batch wasted ~10s of compute on enrichment that the
M13 filter immediately dropped. The early-return makes that path O(setup) instead
of O(enrich).
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context import pipeline

# Frame/timestamp geometry derived from the resolved provider batch size so a
# default change (250<->2500, ADR-047 + amendment 2) cannot silently break the
# boundary setup. The fixture is the LAST 250 frames of batch 0 — the outside
# action must satisfy BOTH constraints: inside the ±_ACTION_TIME_BUFFER_SECONDS
# window of the frames (else the empty-actions path short-circuits first and M13
# is never reached) AND owned by batch 1 per the frame<->time line. Only near the
# batch boundary can both hold. The tests pass provider="idsse" to enrich_batch,
# so resolve for idsse here (H3: same size on both sides).
from analytics.action_context.batching import resolve_frame_batch_size
from analytics.action_context.work_unit import MatchMeta

_BS = resolve_frame_batch_size("idsse")
_FPS_DT = 0.04  # 25 fps

# est_frame = _BS + 7.5 -> owning_batch = 1 (7.5-frame margin past the boundary beats
# 0.04s float imprecision); 0.3s past the last frame -> inside the 0.5s buffer window.
_OUTSIDE_BATCH_TIME_S = (_BS + 7.5) * _FPS_DT
# est_frame = _BS - 50 -> owning_batch = 0 = this batch; inside the frame window.
_OWNED_BATCH_TIME_S = (_BS - 50) * _FPS_DT


def _make_frames_pdf() -> pd.DataFrame:
    """Last-250-frames-of-batch-0 IDSSE-bronze-shaped slice — minimal M13 columns.

    250 rows suffice regardless of ``_FRAME_BATCH_SIZE``: ``_owned_action_ids`` only
    needs >=2 (frame, timestamp) points to fit the linear frame<->time map.
    """
    frames = list(range(_BS - 250, _BS))
    return pd.DataFrame(
        {
            "match_id": ["M"] * 250,
            "frame": frames,
            "period": [1] * 250,
            "timestamp": [f * _FPS_DT for f in frames],
            "frame_batch_id": [0] * 250,
        }
    )


def _make_actions_records_outside_batch() -> list[dict[str, object]]:
    """Actions whose est_frame lands in a DIFFERENT batch (batch_id=1, not 0).

    Pulled into batch 0's enrich call via the ±buffer window but owned by batch 1,
    so the M13 filter would drop them post-enrich. Refactor short-circuits BEFORE
    enrich.
    """
    return [
        {
            "action_id": 99,
            "period_id": 1,
            "time_seconds": _OUTSIDE_BATCH_TIME_S,  # est_frame ~ _BS+7.5 -> owning_batch=1, NOT this batch (0)
            "game_id": 1,
            "type_id": 0,
        }
    ]


def test_zero_owned_batch_short_circuits_before_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero-owned batch must NOT invoke _enrich_tracking_match.

    Asserts the perf-critical invariant: the 20-step enrich chain is skipped
    entirely when M13 says this batch owns nothing. Without this, the dead-ball
    fixtures (J03WN1, J03WOH) would each waste ~10s of pointless silly-kicks
    work per batch in prod.
    """
    enrich_calls: list[object] = []

    def _stub_enrich(**kwargs: object) -> pd.DataFrame:
        enrich_calls.append(kwargs)
        return pd.DataFrame()

    # Patch _enrich_tracking_match — if the refactor calls it for a zero-owned batch,
    # this list grows and the assertion below fails loud.
    monkeypatch.setattr(pipeline, "_enrich_tracking_match", _stub_enrich)
    # Stub the upstream identity/conversion steps so a future regression that
    # skips the early-return gives a clear "_enrich was called" signal instead
    # of a confusing KeyError on missing bronze columns.
    monkeypatch.setattr(pipeline, "_resolve_enrichment_identity", lambda actions, **kw: actions)
    monkeypatch.setattr(pipeline, "_convert_tracking_batch", lambda *a, **k: pd.DataFrame())

    result = pipeline.enrich_batch(
        provider="idsse",
        tier="tracking",
        frames_pdf=_make_frames_pdf(),
        actions_records=_make_actions_records_outside_batch(),
        period=1,
        xt_grid_data=[[0.0]],
        xt_l=1,
        xt_w=1,
        meta=MatchMeta(home_team_id="H", home_start_left=True),
        native_match_id="M",
    )

    assert not enrich_calls, (
        f"_enrich_tracking_match was called {len(enrich_calls)} time(s) for a zero-owned batch; "
        f"expected M13 early-return to short-circuit. Refactor broken."
    )
    # Result should be empty (no rows from this batch).
    assert len(result) == 0


def test_owned_batch_still_invokes_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bit-identity guard: when this batch DOES own actions, enrichment runs as before.

    Catches over-eager refactors that accidentally short-circuit batches that
    should produce output. The action's est_frame = _BS - 50 →
    owning_batch = floor((_BS - 50)/_BS) = 0 = this batch.
    """
    enrich_calls: list[object] = []

    def _stub_enrich(**kwargs: object) -> pd.DataFrame:
        enrich_calls.append(kwargs)
        # Return shape compatible with build_output downstream.
        return pd.DataFrame({"action_id": [1], "period_id": [1]})

    monkeypatch.setattr(pipeline, "_enrich_tracking_match", _stub_enrich)
    # _convert_tracking_batch is called before _enrich_tracking_match; stub it too.
    monkeypatch.setattr(pipeline, "_convert_tracking_batch", lambda *a, **k: pd.DataFrame())
    # _resolve_enrichment_identity needs team_id which we don't synthesize — stub passthrough.
    monkeypatch.setattr(pipeline, "_resolve_enrichment_identity", lambda actions, **kw: actions)

    pipeline.enrich_batch(
        provider="idsse",
        tier="tracking",
        frames_pdf=_make_frames_pdf(),
        actions_records=[
            {
                "action_id": 1,
                "period_id": 1,
                "time_seconds": _OWNED_BATCH_TIME_S,  # est_frame = _BS-50, owning_batch = 0 = this batch
                "game_id": 1,
                "type_id": 0,
                "team_id_native": "T",
                "player_id_native": "P",
            }
        ],
        period=1,
        xt_grid_data=[[0.0]],
        xt_l=1,
        xt_w=1,
        meta=MatchMeta(home_team_id="H", home_start_left=True),
        native_match_id="M",
    )

    assert len(enrich_calls) == 1, (
        f"_enrich_tracking_match expected to be called exactly once for an owned-action batch; "
        f"got {len(enrich_calls)} calls."
    )
