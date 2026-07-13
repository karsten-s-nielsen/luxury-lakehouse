"""D6 (ADR-067): a work unit is written ALL-OR-NOTHING. One raising batch => ZERO rows written.

This is a DELIBERATE contract, not an accident of ``applyInPandas``. A partially-written unit is the
silent-corruption case ADR-040 exists to prevent: downstream cannot distinguish "this half has 12
actions" from "this half HAD 550 and we lost 538". Failing the unit loudly (D2) and writing nothing
is the safer half of that trade.

It is also the AMPLIFIER in the 2026-07-11 incident: a single 1-frame player track in ONE frame
batch (``skillcorner:1552423:2``, batch 184) raised inside the UDF, and the whole unit emitted 0 of
550 actions -- not just batch 184's share.

SCOPE: this pins the LOCAL HEXAGON (``run_work_unit``). Production writes via
``_process_tracking_match`` -> ``mapInPandas`` -> a single ``write_delta_table(replace_where=...)``,
where all-or-nothing comes from Spark plus the atomic Delta transaction -- a DIFFERENT mechanism.

If this test ever fails, the blast radius of one bad batch has CHANGED. That is a decision to take
consciously, not a regression to paper over.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context import pipeline
from analytics.action_context.batching import resolve_frame_batch_size
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


class _RecordingSink:
    """Records every write. The contract is that it records NOTHING when a batch raises."""

    def __init__(self) -> None:
        self.writes: list[pd.DataFrame] = []

    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
        self.writes.append(result_df)
        return len(result_df)


def test_run_work_unit_writes_all_or_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def _boom_on_second(**kw: object) -> pd.DataFrame:
        """Batch 1 SUCCEEDS; batch 2 raises.

        Raising on the FIRST batch would make this test vacuous: ``sink.write`` would be unreachable
        whether the write lives after the batch loop (correct) or inside it (the regression this
        test claims to pin). Only a first-batch success can tell those two worlds apart.
        """
        calls.append(len(calls))
        if len(calls) == 1:
            return pd.DataFrame({"match_id": ["M"], "action_id": [0]})
        raise RuntimeError("synthetic batch failure (frame_batch_id=184)")

    monkeypatch.setattr(pipeline, "enrich_batch", _boom_on_second)

    n_frames = 2 * resolve_frame_batch_size("idsse")  # exactly two frame batches
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

    sink = _RecordingSink()

    with pytest.raises(RuntimeError, match="184"):
        pipeline.run_work_unit(
            WorkUnit("idsse", "M", period=1),
            frames=_Frames(),
            actions=_Actions(),
            xt=_Xt(),
            meta=_Meta(),
            sink=sink,
        )

    assert len(calls) == 2, "the first batch must have SUCCEEDED, or this test proves nothing"
    assert sink.writes == [], (
        "a failing batch must write NOTHING for the unit (all-or-nothing contract) -- a partial "
        "write is silent corruption: downstream cannot tell a short half from a lost one"
    )
