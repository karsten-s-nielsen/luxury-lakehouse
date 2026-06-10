"""ADR-045 unit tests — AQE-proof streaming group dispatch + per-batch overhead quick wins.

Covers:
* ``_make_streaming_group_mapper`` — the mapInPandas adapter MUST pass every
  ``(match_id, period, frame_batch_id)`` group through ``udf_fn`` exactly once with
  exactly the rows ``groupBy().applyInPandas`` would have passed, including when Arrow
  chunking splits a group across consecutive chunks.
* ghost-GK process-local model cache (``enrich._ghost_gk_model_cached``).
* ``executor_env_fingerprint`` once-per-process latch.
* gc-gate sentinel on ``pipeline._convert_tracking_batch``.
"""

from __future__ import annotations

import inspect

import pandas as pd
import pytest

from ingestion.action_context import _make_streaming_group_mapper

KEYS = ["match_id", "period", "frame_batch_id"]


def _frames(match_id: str, period: float, batch: int, n: int, start: int = 0) -> pd.DataFrame:
    """Rows for one (match, period, batch) group with a per-row payload."""
    return pd.DataFrame(
        {
            "match_id": [match_id] * n,
            "period": [period] * n,
            "frame_batch_id": [batch] * n,
            "frame": list(range(start, start + n)),
        }
    )


def _collecting_udf(seen: list[pd.DataFrame]):
    """udf_fn stub: records each group it receives, emits one summary row per group."""

    def _udf(g: pd.DataFrame) -> pd.DataFrame:
        seen.append(g.copy())
        return pd.DataFrame(
            {
                "match_id": [g["match_id"].iloc[0]],
                "period": [g["period"].iloc[0]],
                "frame_batch_id": [g["frame_batch_id"].iloc[0]],
                "n_rows": [len(g)],
            }
        )

    return _udf


def _run(mapper, chunks: list[pd.DataFrame]) -> pd.DataFrame:
    outs = list(mapper(iter(chunks)))
    return pd.concat(outs, ignore_index=True) if outs else pd.DataFrame()


class TestStreamingGroupMapper:
    def test_groups_in_one_chunk(self) -> None:
        """Multiple complete groups in a single chunk each reach udf_fn once, in order."""
        seen: list[pd.DataFrame] = []
        chunk = pd.concat(
            [_frames("A", 1.0, 0, 5), _frames("A", 1.0, 1, 3), _frames("A", 2.0, 0, 4)],
            ignore_index=True,
        )
        out = _run(_make_streaming_group_mapper(_collecting_udf(seen), KEYS), [chunk])
        assert len(seen) == 3
        assert list(out["n_rows"]) == [5, 3, 4]

    def test_group_split_across_chunks_reassembles(self) -> None:
        """A group split by Arrow chunking is carried and passed to udf_fn ONCE, whole.
        This is the load-bearing semantic: a split group fed twice would double-enrich
        (duplicate (match_id, action_id) rows) or mis-batch the savgol velocity window."""
        seen: list[pd.DataFrame] = []
        g_b = _frames("A", 1.0, 1, 8)
        chunks = [
            pd.concat([_frames("A", 1.0, 0, 4), g_b.iloc[:3]], ignore_index=True),
            pd.concat([g_b.iloc[3:6], g_b.iloc[6:]], ignore_index=True),
            _frames("A", 2.0, 0, 2),
        ]
        out = _run(_make_streaming_group_mapper(_collecting_udf(seen), KEYS), chunks)
        assert len(seen) == 3
        assert list(out["n_rows"]) == [4, 8, 2]
        # the reassembled group is byte-equal to the original
        reassembled = seen[1].reset_index(drop=True)
        pd.testing.assert_frame_equal(reassembled, g_b.reset_index(drop=True))

    def test_single_group_spanning_all_chunks(self) -> None:
        """One group across every chunk flushes exactly once at iterator exhaustion."""
        seen: list[pd.DataFrame] = []
        g = _frames("A", 1.0, 0, 12)
        chunks = [g.iloc[:5], g.iloc[5:9], g.iloc[9:]]
        out = _run(_make_streaming_group_mapper(_collecting_udf(seen), KEYS), chunks)
        assert len(seen) == 1
        assert list(out["n_rows"]) == [12]

    def test_empty_chunks_and_empty_iterator(self) -> None:
        seen: list[pd.DataFrame] = []
        mapper = _make_streaming_group_mapper(_collecting_udf(seen), KEYS)
        assert _run(mapper, [pd.DataFrame(columns=["match_id", "period", "frame_batch_id", "frame"])]).empty
        assert _run(mapper, []).empty
        assert not seen

    def test_udf_empty_output_yields_nothing(self) -> None:
        """Groups whose enrichment returns no rows (M13 zero-owned batches) emit nothing."""

        def _empty_udf(g: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(columns=["match_id"])

        out = _run(_make_streaming_group_mapper(_empty_udf, KEYS), [_frames("A", 1.0, 0, 5)])
        assert out.empty

    def test_equivalent_to_direct_groupby(self) -> None:
        """Mapper output == plain groupby-apply output (the applyInPandas semantic),
        regardless of how chunk boundaries fall — including GS-style float periods and
        string match_ids."""
        seen: list[pd.DataFrame] = []
        full = pd.concat(
            [_frames("10505", 1.0, b, n) for b, n in [(0, 7), (1, 1), (2, 10), (3, 2)]] + [_frames("10505", 2.0, 0, 6)],
            ignore_index=True,
        )
        udf = _collecting_udf(seen)
        expected = pd.concat(
            [udf(g.reset_index(drop=True)) for _, g in full.groupby(KEYS, sort=False)],
            ignore_index=True,
        )
        seen.clear()
        # adversarial chunking: boundaries inside groups, single-row chunks
        chunks = [full.iloc[:3], full.iloc[3:8], full.iloc[8:9], full.iloc[9:20], full.iloc[20:]]
        out = _run(_make_streaming_group_mapper(udf, KEYS), chunks)
        pd.testing.assert_frame_equal(out, expected)


class TestGhostGkModelCache:
    def test_loads_once_and_reuses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from analytics.action_context import enrich

        calls: list[str] = []
        sentinel = object()

        class _FakeModel:
            @classmethod
            def from_variant(cls, variant: str = "default") -> object:
                calls.append(variant)
                return sentinel

        import silly_kicks.tracking as skt

        monkeypatch.setattr(skt, "GhostGkModel", _FakeModel)
        monkeypatch.setattr(enrich, "_GHOST_GK_MODEL_CACHE", {})

        first = enrich._ghost_gk_model_cached()
        second = enrich._ghost_gk_model_cached()
        assert first is sentinel and second is sentinel
        assert calls == ["default"]  # loaded exactly once

    def test_call_sites_pass_instance_not_string(self) -> None:
        """Both add_ghost_gk call sites must pass the cached instance — the "default"
        STRING makes silly-kicks reload the ~12 MB weights from disk per batch."""
        from analytics.action_context import enrich

        src = inspect.getsource(enrich)
        # the trailing comma distinguishes the kwarg call-site form from prose mentions
        # of model="default" in docstrings/comments
        assert 'model="default",' not in src
        assert src.count("model=_ghost_gk_model_cached()") == 2


class TestEnvfpOnceGuard:
    def test_second_call_is_noop(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        import socket

        from ingestion import exec_visibility as ev

        # no real network probe in tests
        monkeypatch.setattr(socket, "create_connection", lambda *a, **k: (_ for _ in ()).throw(OSError("no")))
        writes: list[str] = []
        monkeypatch.setattr(ev, "executor_marker", lambda d, *, seq, payload: writes.append(seq) or True)

        ev.reset_executor_env_fingerprint()
        assert ev.executor_env_fingerprint(str(tmp_path), seq="b0_envfp") is True
        assert ev.executor_env_fingerprint(str(tmp_path), seq="b1_envfp") is False
        assert writes == ["b0_envfp"]  # one fingerprint per process
        ev.reset_executor_env_fingerprint()


def test_gc_collect_is_gated_on_group_size() -> None:
    """Sentinel: the per-batch gc.collect() in _convert_tracking_batch must stay gated
    behind _GC_COLLECT_MIN_ROWS (unconditional full collection measured 9.5% of local
    per-half wall on 250-frame batches)."""
    from analytics.action_context import pipeline

    src = inspect.getsource(pipeline._convert_tracking_batch)
    assert src.count("_GC_COLLECT_MIN_ROWS") == 2
    assert pipeline._GC_COLLECT_MIN_ROWS >= 50_000
