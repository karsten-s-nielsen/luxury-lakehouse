"""Tests for ingestion.idsse — IDSSE tracking and event data DFL XML parsing."""

from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest

if TYPE_CHECKING:
    from unittest.mock import MagicMock

from ingestion.idsse import IDSSE_MATCH_IDS, _smooth_tracking

_logger = logging.getLogger("test_idsse")

# Minimal DFL match info XML for testing (includes PlayingPosition for GK detection)
_MATCH_INFO_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <General MatchId="DFL-MAT-J03WMX" HomeTeamId="DFL-CLU-000008"
             GuestTeamId="DFL-CLU-00000G" />
    <Teams>
      <Team TeamId="DFL-CLU-000008" Role="home">
        <Players>
          <Player PersonId="H001" ShirtNumber="1" PlayingPosition="TW" FirstName="A" LastName="B" />
          <Player PersonId="H002" ShirtNumber="2" PlayingPosition="IV" FirstName="C" LastName="D" />
        </Players>
      </Team>
      <Team TeamId="DFL-CLU-00000G" Role="guest">
        <Players>
          <Player PersonId="A001" ShirtNumber="1" PlayingPosition="TW" FirstName="E" LastName="F" />
          <Player PersonId="A002" ShirtNumber="10" PlayingPosition="RA" FirstName="G" LastName="H" />
        </Players>
      </Team>
    </Teams>
  </MatchInformation>
</PutDataRequest>
"""

# Minimal DFL position XML for testing
_POSITIONS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="0.5" Y="1.0"/>
<Frame N="1" X="1.5" Y="2.0"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="0" X="-10.5" Y="5.2"/>
<Frame N="1" X="-10.0" Y="5.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H002">
<Frame N="0" X="20.0" Y="-15.0"/>
<Frame N="1" X="20.5" Y="-14.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000G" PersonId="A001">
<Frame N="0" X="30.0" Y="10.0"/>
<Frame N="1" X="30.5" Y="10.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="referee" PersonId="REF001">
<Frame N="0" X="0.0" Y="0.0"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="-5.0" Y="-3.0"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000G" PersonId="A002">
<Frame N="0" X="45.0" Y="25.0"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


# Ball FrameSet appears AFTER player FrameSets — tests the ordering assumption (TD#26)
_POSITIONS_XML_BALL_LAST = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="0" X="-10.5" Y="5.2"/>
<Frame N="1" X="-10.0" Y="5.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="0.5" Y="1.0"/>
<Frame N="1" X="1.5" Y="2.0"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


# PR 1.6 regression fixture: DFL position XML with NON-ZERO period-start frame
# numbers. Real DFL data starts period 1 at frame ~10000 and period 2 at
# ~100000 (frame numbers are absolute across the match, not reset per period).
# Under the old absolute-timestamp logic (timestamp = n / 25), frame 10000
# → 400.0s, which never matches the event side's period-relative 0s timestamp.
# Under the fixed period-relative logic, frame 10000 → 0.0s.
_POSITIONS_XML_ABSOLUTE_FRAMES = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="10000" X="0.5" Y="1.0"/>
<Frame N="10001" X="1.5" Y="2.0"/>
<Frame N="10002" X="2.5" Y="3.0"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="10000" X="-10.5" Y="5.2"/>
<Frame N="10001" X="-10.0" Y="5.5"/>
<Frame N="10002" X="-9.5" Y="5.8"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="100000" X="0.0" Y="0.0"/>
<Frame N="100001" X="0.1" Y="0.1"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="100000" X="-5.0" Y="0.0"/>
<Frame N="100001" X="-5.5" Y="0.5"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


def _write_temp_xml(content: str) -> str:
    """Write XML content to a temporary file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".xml")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class TestSmoothTracking:
    """Tests for _smooth_tracking integration."""

    def test_reduces_noise_in_parsed_data(self) -> None:
        """Smoothing reduces frame-to-frame jitter on realistic tracking data."""
        rng = np.random.default_rng(42)
        n = 50
        df = pd.DataFrame(
            {
                "player_id": "H001",
                "period": 1,
                "frame": range(n),
                "timestamp": [i / 25.0 for i in range(n)],
                "team": "home",
                "x": np.linspace(-10, 10, n) + rng.normal(0, 0.02, n),
                "y": np.linspace(5, 15, n) + rng.normal(0, 0.02, n),
                "ball_x": 0.0,
                "ball_y": 0.0,
                "match_id": "idsse_J03WMX",
                "frame_rate": 25,
            }
        )
        raw_jitter = df["x"].diff().std()

        smoothed = _smooth_tracking(df)

        smooth_jitter = smoothed["x"].diff().std()
        assert smooth_jitter < raw_jitter
        assert len(smoothed) == len(df)

    def test_short_sequence_unchanged(self) -> None:
        """Sequences shorter than window_length pass through unmodified."""
        df = pd.DataFrame(
            {
                "player_id": ["H001"] * 3,
                "period": [1] * 3,
                "frame": [0, 1, 2],
                "timestamp": [0.0, 0.04, 0.08],
                "team": ["home"] * 3,
                "x": [1.0, 2.0, 3.0],
                "y": [4.0, 5.0, 6.0],
                "ball_x": [0.0] * 3,
                "ball_y": [0.0] * 3,
                "match_id": ["idsse_J03WMX"] * 3,
                "frame_rate": [25] * 3,
            }
        )

        smoothed = _smooth_tracking(df)

        pd.testing.assert_series_equal(smoothed["x"], df["x"], check_names=False)


class TestIDSSEMatchIDs:
    """Tests for match ID constants."""

    def test_seven_match_ids(self) -> None:
        assert len(IDSSE_MATCH_IDS) == 7

    def test_known_match_id_present(self) -> None:
        assert "J03WMX" in IDSSE_MATCH_IDS


# Minimal DFL event XML for testing (DFL_03_02 series — actual format)
# Mirrors the real PutDataRequest > Event > {child type} structure.
# Coordinates are DFL pitch-origin meters: x in (0, 105), y in (0, 68).
_EVENTS_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
<PutDataRequest RequestId="TEST" MessageTime="2023-05-27T15:00:00.000+02:00" \
TransmissionComplete="true" DataStatus="postmatch">
<Event MatchId="DFL-MAT-J03WMX" X-Source-Position="52.50" \
EventTime="2023-05-27T15:30:12.230+02:00" Y-Source-Position="34.00" \
EventId="18226500000006" X-Position="52.50" Y-Position="34.00">
 <KickOff TeamLeft="DFL-CLU-00000G" TeamRight="DFL-CLU-000008" GameSection="firstHalf">
  <Play SemiField="false" Player="DFL-OBJ-0027G6" Team="DFL-CLU-00000G" \
FromOpenPlay="false" Recipient="DFL-OBJ-0027KL" Evaluation="successfullyCompleted">
   <Pass FreeKickLayup="false"/>
  </Play>
 </KickOff>
</Event>
<Event MatchId="DFL-MAT-J03WMX" X-Source-Position="38.27" \
EventTime="2023-05-27T15:30:15.059+02:00" Y-Source-Position="33.80" \
EventId="18226500000007" X-Position="38.27" Y-Position="33.80">
 <Play SemiField="false" Player="H001" Team="DFL-CLU-000008" \
FromOpenPlay="true" Evaluation="unsuccessful">
  <Pass FreeKickLayup="false" Direction="diagonalBall"/>
 </Play>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T15:30:34.498+02:00" \
EventId="18226500000009" X-Position="50.55" Y-Position="59.11">
 <TacklingGame WinnerTeam="DFL-CLU-00000G" Winner="A001" \
Loser="H002" LoserTeam="DFL-CLU-000008" Type="air"/>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:00.000+02:00" \
EventId="18226500000050" X-Position="52.50" Y-Position="34.00">
 <KickOff TeamLeft="DFL-CLU-000008" TeamRight="DFL-CLU-00000G" GameSection="secondHalf">
  <Play SemiField="false" Player="H001" Team="DFL-CLU-000008" \
FromOpenPlay="false" Recipient="H002" Evaluation="successfullyCompleted">
   <Pass FreeKickLayup="false"/>
  </Play>
 </KickOff>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:05.500+02:00" \
EventId="18226500000051" X-Position="65.00" Y-Position="40.00">
 <Play SemiField="false" Player="A001" Team="DFL-CLU-00000G" \
FromOpenPlay="true" Evaluation="successfullyCompleted">
  <Pass FreeKickLayup="false"/>
 </Play>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:10.000+02:00" \
EventId="18226500000099">
 <OtherBallAction Player="A002" Team="DFL-CLU-00000G"/>
</Event>
</PutDataRequest>
"""


class TestParseMatchIdsArg:
    """`_parse_match_ids_arg` — CLI subset parsing + validation.

    Used by the Terraform `for_each_task` fan-out: each child iteration
    receives `--match-ids "J03WMX,J03WN1"` (comma-separated subset,
    runtime-discovered by the preflight task). `main()` calls this helper
    to parse + validate.
    """

    def test_returns_none_for_none_input(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg(None) is None

    def test_returns_none_for_empty_string(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg("") is None

    def test_parses_single_id(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg("J03WMX") == ["J03WMX"]

    def test_parses_comma_separated_list(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg("J03WMX,J03WN1") == ["J03WMX", "J03WN1"]

    def test_strips_whitespace(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg(" J03WMX , J03WN1 ") == ["J03WMX", "J03WN1"]

    def test_skips_empty_segments(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        assert _parse_match_ids_arg("J03WMX,,J03WN1") == ["J03WMX", "J03WN1"]

    def test_rejects_unknown_id(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        with pytest.raises(SystemExit) as excinfo:
            _parse_match_ids_arg("BOGUS_ID")
        assert "BOGUS_ID" in str(excinfo.value)

    def test_rejects_mixed_known_and_unknown(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg

        with pytest.raises(SystemExit) as excinfo:
            _parse_match_ids_arg("J03WMX,BOGUS")
        assert "BOGUS" in str(excinfo.value)

    def test_accepts_full_idsse_match_id_set(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, _parse_match_ids_arg

        joined = ",".join(IDSSE_MATCH_IDS)
        result = _parse_match_ids_arg(joined)
        assert result == list(IDSSE_MATCH_IDS)


class TestRunPipelineMatchIds:
    """`run_pipeline` forwards `match_ids` to the inner ingest functions.

    Verifies the for_each_task wiring at the run_pipeline boundary.
    """

    def test_run_pipeline_forwards_match_ids_to_both_inner_functions(self) -> None:
        from unittest.mock import MagicMock, patch

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=2)
        chunk = ["J03WMX", "J03WN1"]

        with (
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            run_pipeline(
                spark,
                "cat",
                "schema",
                logger_mock,
                filter_result=fr,
                match_ids=chunk,
            )

        assert mock_track.call_args.kwargs.get("match_ids") == chunk
        assert mock_events.call_args.kwargs.get("match_ids") == chunk

    def test_run_pipeline_default_match_ids_is_none(self) -> None:
        """Backward-compat: existing callers passing no match_ids see None."""
        from unittest.mock import MagicMock, patch

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=7)

        with (
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            run_pipeline(spark, "cat", "schema", logger_mock, filter_result=fr)

        assert mock_track.call_args.kwargs.get("match_ids") is None
        assert mock_events.call_args.kwargs.get("match_ids") is None

    def test_run_pipeline_skip_when_count_zero_does_not_invoke_ingest(self) -> None:
        """When filter_result.count == 0, the inner ingest functions are
        NOT invoked. The @workflow decorator's runner catches the internal
        WorkflowSkippedError and returns cleanly — we verify skip
        semantics by asserting no work was performed."""
        from unittest.mock import MagicMock, patch

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=0)

        with (
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            run_pipeline(
                spark,
                "cat",
                "schema",
                logger_mock,
                filter_result=fr,
                match_ids=["J03WMX", "J03WN1"],
            )
            mock_track.assert_not_called()
            mock_events.assert_not_called()


class TestMainCliE2E:
    """End-to-end test of the iteration's CLI flow.

    Exercises the full path that each `ingest_idsse_iteration` task hits:
    `python -m ingestion.idsse --catalog cat --schema bronze --match-ids "J03WMX,J03WN1"`
    Mocks only the Spark session + bootstrap + the inner ingest functions.
    """

    def test_main_with_chunk_subset_threads_through_to_ingest(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            [
                "ingest_idsse",
                "--catalog",
                "soccer_analytics",
                "--schema",
                "bronze",
                "--match-ids",
                "J03WMX,J03WN1",
            ],
        )

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.bootstrap.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            from ingestion.guards import FilterResult

            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=2)

            from ingestion.idsse import main

            main()

        assert mock_track.call_count == 1
        assert mock_events.call_count == 1
        assert mock_track.call_args.kwargs.get("match_ids") == ["J03WMX", "J03WN1"]
        assert mock_events.call_args.kwargs.get("match_ids") == ["J03WMX", "J03WN1"]

    def test_main_without_match_ids_processes_all(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["ingest_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.bootstrap.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            from ingestion.guards import FilterResult

            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=7)

            from ingestion.idsse import main

            main()

        assert mock_track.call_args.kwargs.get("match_ids") is None
        assert mock_events.call_args.kwargs.get("match_ids") is None

    def test_main_with_unknown_match_id_exits(self, monkeypatch) -> None:
        """Fail-fast: SystemExit before any Spark session is created."""
        from unittest.mock import patch

        monkeypatch.setattr(
            "sys.argv",
            [
                "ingest_idsse",
                "--catalog",
                "soccer_analytics",
                "--schema",
                "bronze",
                "--match-ids",
                "J03WMX,BOGUS_ID",
            ],
        )

        with (
            patch("ingestion.idsse.get_spark_session"),
            patch("ingestion.bootstrap.bootstrap_hooks"),
            patch("ingestion.idsse.ingest_idsse") as mock_track,
        ):
            from ingestion.idsse import main

            with pytest.raises(SystemExit) as excinfo:
                main()
            assert "BOGUS_ID" in str(excinfo.value)
            mock_track.assert_not_called()


class TestIdsseGuardChunks:
    """`_IdsseGuard.check()` runtime chunk discovery.

    The guard anti-joins IDSSE_MATCH_IDS against (tracking ∩ events) to
    determine missing matches, then partitions them into chunks of size
    `chunk_size` (default 2). The preflight task forwards these chunks
    to the for_each_task fan-out via `dbutils.jobs.taskValues`.
    """

    def _mock_spark_with_match_ids(
        self,
        tracking_ids: set[str],
        events_ids: set[str],
    ) -> MagicMock:
        """Build a MagicMock Spark whose `.table(...).select(...).distinct().collect()`
        returns rows with the configured match_ids per table name."""
        from unittest.mock import MagicMock

        spark = MagicMock()

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            ids = events_ids if "events" in name else tracking_ids
            mock_rows = []
            for mid in ids:
                row = MagicMock()
                row.__getitem__ = lambda self, key, _mid=mid: _mid
                mock_rows.append(row)
            mock_df.select.return_value.distinct.return_value.collect.return_value = mock_rows
            return mock_df

        spark.table.side_effect = table_side_effect
        return spark

    def test_all_seven_missing_returns_four_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        spark = self._mock_spark_with_match_ids(set(), set())
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.workflow_id == "wf-idsse"
        assert result.count == 7
        assert result.chunks is not None
        assert len(result.chunks) == 4  # ceil(7 / 2)
        # Sizing: 2,2,2,1
        assert [len(c) for c in result.chunks] == [2, 2, 2, 1]
        # All match IDs accounted for, in deterministic order
        flattened = [mid for chunk in result.chunks for mid in chunk]
        assert flattened == list(IDSSE_MATCH_IDS)

    def test_all_seven_done_returns_count_zero_no_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        all_ids = set(IDSSE_MATCH_IDS)
        spark = self._mock_spark_with_match_ids(all_ids, all_ids)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.count == 0
        assert result.chunks is None or result.chunks == []

    def test_partial_three_missing_returns_two_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        # First 4 done (in both tables), last 3 missing.
        done = set(IDSSE_MATCH_IDS[:4])
        spark = self._mock_spark_with_match_ids(done, done)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.count == 3
        assert result.chunks is not None
        assert len(result.chunks) == 2  # ceil(3 / 2)
        assert [len(c) for c in result.chunks] == [2, 1]
        flattened = [mid for chunk in result.chunks for mid in chunk]
        assert sorted(flattened) == sorted(IDSSE_MATCH_IDS[4:])

    def test_match_in_tracking_but_not_events_counts_as_missing(self) -> None:
        """A match is 'complete' only when present in BOTH tracking AND events.

        If tracking has it but events doesn't (e.g. mid-flight from a
        previous job run), the match must still be re-attempted so the
        events ingestion gets a chance to run."""
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        all_ids = set(IDSSE_MATCH_IDS)
        partial_events = all_ids - {IDSSE_MATCH_IDS[0]}  # missing one in events
        spark = self._mock_spark_with_match_ids(all_ids, partial_events)

        result = skip_guard.check(spark, "cat", "bronze")
        assert result.count == 1
        assert result.chunks == [[IDSSE_MATCH_IDS[0]]]

    def test_chunk_size_is_two(self) -> None:
        from ingestion.idsse import _IdsseGuard

        assert _IdsseGuard.chunk_size == 2

    def test_no_chunk_exceeds_chunk_size(self) -> None:
        """Sizing invariant — preserved for any subset of missing matches."""
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        # 5 missing
        done = set(IDSSE_MATCH_IDS[:2])
        spark = self._mock_spark_with_match_ids(done, done)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.chunks is not None
        for chunk in result.chunks:
            assert len(chunk) <= skip_guard.chunk_size


class TestPreflightIdsse:
    """`main_preflight` runs the guard and writes chunks to task values.

    Output contract: `dbutils.jobs.taskValues.set(key="idsse_match_chunks",
    value=<list>)` where `<list>` is a list of comma-separated match-ID
    strings, exactly the shape that the for_each_task `inputs` field
    expects. Empty list when no work — for_each_task spawns 0 iterations.
    """

    def test_preflight_writes_chunks_in_for_each_input_format(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["preflight_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        from ingestion.guards import FilterResult

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.bootstrap.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse._write_match_chunks_task_value") as mock_write,
        ):
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(
                workflow_id="wf-idsse",
                count=3,
                chunks=[["J03WMX", "J03WN1"], ["J03WPY"]],
            )

            from ingestion.idsse import main_preflight

            main_preflight()

        # Helper called once with the for_each-shaped chunks.
        assert mock_write.call_count == 1
        chunks_for_inputs = mock_write.call_args.args[0]
        assert chunks_for_inputs == ["J03WMX,J03WN1", "J03WPY"]

    def test_preflight_writes_empty_list_when_no_work(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["preflight_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        from ingestion.guards import FilterResult

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.bootstrap.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse._write_match_chunks_task_value") as mock_write,
        ):
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=0)

            from ingestion.idsse import main_preflight

            main_preflight()

        # Empty list → for_each_task spawns 0 iterations.
        chunks_for_inputs = mock_write.call_args.args[0]
        assert chunks_for_inputs == []

    def test_write_helper_degrades_cleanly_outside_databricks(self) -> None:
        """The dbutils import fails in local/test mode; the helper logs and returns."""
        from unittest.mock import MagicMock

        from ingestion.idsse import _write_match_chunks_task_value

        logger_mock = MagicMock()
        # Should NOT raise, even though dbutils.jobs.taskValues is unavailable.
        _write_match_chunks_task_value(["J03WMX,J03WN1"], logger_mock)
