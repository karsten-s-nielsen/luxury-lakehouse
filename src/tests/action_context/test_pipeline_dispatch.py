"""run_work_unit dispatch + H3 batch-loop wiring (enrich_batch stubbed).

Verifies the orchestration contract without invoking real silly-kicks: that
``run_work_unit`` (a) routes by FrameBundle.tier (M6/M11), (b) for the tracking
tier loops ``floor(frame/_FRAME_BATCH_SIZE)`` calling ``enrich_batch`` ONCE per
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


def test_sb360_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    # Frames-required (ADR-057): the only non-tracking tier is sb360; it runs a single
    # enrich_batch call (no frame batching). (Replaces the retired event_only dispatch.)
    calls: list[str] = []

    def _stub(**kw: object) -> pd.DataFrame:
        calls.append(str(kw["tier"]))
        return pd.DataFrame({"match_id": ["M"], "action_id": [0]})

    monkeypatch.setattr(pipeline, "enrich_batch", _stub)

    class _Frames:
        def frames(self, wu: WorkUnit) -> FrameBundle:
            return FrameBundle(tier="sb360", frames=pd.DataFrame())

    sink = _Sink()
    n = pipeline.run_work_unit(
        WorkUnit("statsbomb", "M"), frames=_Frames(), actions=_Actions(), xt=_Xt(), meta=_Meta(), sink=sink
    )
    assert calls == ["sb360"]  # single non-tracking call
    assert n == 1 and sink.rows == 1


def test_enrich_batch_rejects_unknown_tier() -> None:
    # Total dispatch (review M3): an unknown/retired tier raises rather than falling through
    # to the tracking path. The guard fires before any frame work, so dummy args are fine.
    with pytest.raises(ValueError, match="unknown action-context tier"):
        pipeline.enrich_batch(
            provider="wyscout",
            tier="event_only",  # type: ignore[arg-type]  # deliberately invalid FrameTier
            frames_pdf=pd.DataFrame(),
            actions_records=[{"action_id": 0, "game_id": 1, "period_id": 1}],
            period=1,
            xt_grid_data=[[0.0]],
            xt_l=1,
            xt_w=1,
            meta=MatchMeta(home_team_id="A", home_start_left=True),
            native_match_id="M",
        )


def test_tracking_tier_loops_one_call_per_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    # Frames spanning exactly 2 batches -> floor(frame/size) = batches
    # {0, 1} -> 2 enrich_batch calls. Derived from the resolved provider size so a
    # default change (250<->2500, ADR-047 + amendment 2) does not silently turn
    # this into a 1-batch test.
    from analytics.action_context.batching import resolve_frame_batch_size

    calls: list[tuple[str, int]] = []

    def _stub(**kw: object) -> pd.DataFrame:
        calls.append((str(kw["tier"]), int(kw["period"])))  # type: ignore[arg-type]
        return pd.DataFrame({"match_id": ["M"], "action_id": [len(calls)]})

    monkeypatch.setattr(pipeline, "enrich_batch", _stub)

    n_frames = 2 * resolve_frame_batch_size("idsse")
    frames_df = pd.DataFrame(
        {
            "frame": list(range(n_frames)),
            "period": [1] * n_frames,
            "timestamp": [i * 0.04 for i in range(n_frames)],
        }
    )

    class _Frames:
        def frames(self, wu: WorkUnit) -> FrameBundle:
            return FrameBundle(tier="tracking", frames=frames_df)

    sink = _Sink()
    n = pipeline.run_work_unit(
        WorkUnit("idsse", "M", period=1), frames=_Frames(), actions=_Actions(), xt=_Xt(), meta=_Meta(), sink=sink
    )
    assert calls == [("tracking", 1), ("tracking", 1)]  # exactly one call per frame batch (H3)
    assert n == 2 and sink.rows == 2


def test_frame_batch_size_lockstep() -> None:
    """H3 lockstep sentinel (ADR-047 + amendment 2): the batch size is part of the
    domain contract — per-provider, resolved through ONE module.

    Prod (Spark dispatch), local (run_work_unit), and the fixture extractor MUST batch
    identically — a drifted copy silently changes window-dependent feature values for
    whichever path runs it. Since amendment 2 the lockstep is BY SHARED IMPORT: both
    sides call analytics.action_context.batching.resolve_frame_batch_size (asserted by
    identity below), so there is no per-module constant left to drift. The fixture
    extractor must import the same resolver (source-level assert — it is a PEP 723
    script, not importable here). ingestion.tracking_context is deliberately NOT
    included: a separate, deprecating pipeline pinned at 250 (test_tracking_context_udf).
    """
    import re
    from pathlib import Path

    from analytics.action_context import batching
    from ingestion import action_context as ingestion_ac

    # One resolver, imported by both sides (identity, not equality — a copy would drift).
    assert ingestion_ac.resolve_frame_batch_size is batching.resolve_frame_batch_size
    assert pipeline.resolve_frame_batch_size is batching.resolve_frame_batch_size

    # Neither side declares a local size constant any more.
    for mod in (pipeline, ingestion_ac):
        assert not hasattr(mod, "_FRAME_BATCH_SIZE"), f"{mod.__name__} regrew a local _FRAME_BATCH_SIZE"

    extractor_src = Path("scripts/extract_action_context_fixture.py").read_text(encoding="utf-8")
    assert re.search(r"from analytics\.action_context\.batching import .*resolve_frame_batch_size", extractor_src), (
        "extract_action_context_fixture.py must resolve its batch size via analytics.action_context.batching"
    )
    assert not re.search(r"^_FRAME_BATCH_SIZE = \d+$", extractor_src, re.MULTILINE), (
        "extract_action_context_fixture.py regrew a local _FRAME_BATCH_SIZE constant"
    )
