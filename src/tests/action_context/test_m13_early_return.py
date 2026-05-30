"""Unit tests for the M13 early-return short-circuit in ``enrich_batch``.

When a 250-frame batch has zero owned actions (all buffer-windowed actions belong
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
from analytics.action_context.work_unit import MatchMeta


def _make_frames_pdf() -> pd.DataFrame:
    """250-frame IDSSE-bronze-shaped batch — minimal columns to drive the M13 path."""
    return pd.DataFrame(
        {
            "match_id": ["M"] * 250,
            "frame": list(range(250)),
            "period": [1] * 250,
            "timestamp": [i * 0.04 for i in range(250)],
            "frame_batch_id": [0] * 250,
        }
    )


def _make_actions_records_outside_batch() -> list[dict[str, object]]:
    """Actions whose est_frame lands in a DIFFERENT batch (batch_id=1, not 0).

    Pulled into batch 0's enrich call via the ±buffer window but owned by batch 1,
    so the M13 filter would drop them post-enrich. Refactor short-circuits BEFORE
    enrich. ``time_seconds=10.3`` → est_frame ≈ 257.5 → batch 1 (well past the
    floating-point-edge at 10.0s where 0.04 imprecision can flip the floor).
    """
    return [
        {
            "action_id": 99,
            "period_id": 1,
            "time_seconds": 10.3,  # est_frame ~258, owning_batch=1, NOT this batch (0)
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
    should produce output. The action's time_seconds=2.0 → est_frame ~50,
    owning_batch = floor(50/250) = 0 = this batch.
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
                "time_seconds": 2.0,  # frame ~50, owning_batch = 0 = this batch
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
