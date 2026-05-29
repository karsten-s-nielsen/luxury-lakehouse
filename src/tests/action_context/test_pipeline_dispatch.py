"""run_work_unit dispatch + H3 batch-loop wiring (enrich_batch stubbed).

Verifies the orchestration contract without invoking real silly-kicks: that
``run_work_unit`` (a) routes by FrameBundle.tier (M6/M11), (b) for the tracking
tier loops ``floor(frame/250)`` calling ``enrich_batch`` ONCE per
(period, frame_batch) group — the H3 invariant — and (c) writes via the sink.
Real enrichment correctness is covered by the Phase C differential / e2e.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context import pipeline
from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


class _Actions:
    def actions(self, wu: WorkUnit) -> pd.DataFrame:
        return pd.DataFrame({"action_id": [0], "game_id": [1], "period_id": [1]})


class _Xt:
    def grid(self) -> tuple[list[list[float]], int, int]:
        return ([[0.0]], 1, 1)


class _Meta:
    def metadata(self, wu: WorkUnit) -> MatchMeta:
        return MatchMeta(home_team_id="A", home_start_left=True)


class _Sink:
    def __init__(self) -> None:
        self.rows: int | None = None

    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
        self.rows = len(result_df)
        return len(result_df)


def test_event_only_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def _stub(**kw: object) -> pd.DataFrame:
        calls.append(str(kw["tier"]))
        return pd.DataFrame({"match_id": ["M"], "action_id": [0]})

    monkeypatch.setattr(pipeline, "enrich_batch", _stub)

    class _Frames:
        def frames(self, wu: WorkUnit) -> FrameBundle:
            return FrameBundle(tier="event_only", frames=pd.DataFrame())

    sink = _Sink()
    n = pipeline.run_work_unit(
        WorkUnit("wyscout", "M"), frames=_Frames(), actions=_Actions(), xt=_Xt(), meta=_Meta(), sink=sink
    )
    assert calls == ["event_only"]  # single non-tracking call
    assert n == 1 and sink.rows == 1


def test_tracking_tier_loops_one_call_per_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    # 500 frames over one period -> floor(frame/250) = batches {0, 1} -> 2 enrich_batch calls.
    calls: list[tuple[str, int]] = []

    def _stub(**kw: object) -> pd.DataFrame:
        calls.append((str(kw["tier"]), int(kw["period"])))  # type: ignore[arg-type]
        return pd.DataFrame({"match_id": ["M"], "action_id": [len(calls)]})

    monkeypatch.setattr(pipeline, "enrich_batch", _stub)

    frames_df = pd.DataFrame(
        {
            "frame": list(range(500)),
            "period": [1] * 500,
            "timestamp": [i * 0.04 for i in range(500)],
        }
    )

    class _Frames:
        def frames(self, wu: WorkUnit) -> FrameBundle:
            return FrameBundle(tier="tracking", frames=frames_df)

    sink = _Sink()
    n = pipeline.run_work_unit(
        WorkUnit("idsse", "M", period=1), frames=_Frames(), actions=_Actions(), xt=_Xt(), meta=_Meta(), sink=sink
    )
    assert calls == [("tracking", 1), ("tracking", 1)]  # exactly one call per 250-frame batch (H3)
    assert n == 2 and sink.rows == 2
