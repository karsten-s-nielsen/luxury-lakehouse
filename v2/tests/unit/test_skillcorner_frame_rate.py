"""The SkillCorner tracking frame rate, derived at parse time from the per-frame clock cadence (v1 parity).

The owner-tier (RM-format) SkillCorner match file has no `match_periods`, so v2 cannot take the frame rate from
the match file. v1 derives it from the tracking clock (`src/ingestion/skillcorner_tracking.py` `derive_frame_rate`):
1/median(dt) within each period, snapped to {10, 25, 30} within +-5 %, fail loud, never a default. v2 does the same
and stores it as the bronze `frame_rate` column of `skillcorner_tracking_frames`.
"""

# pyarrow's stubs leave nested DataType members untyped; relax the unknown-family rules (see compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import json

import pyarrow as pa
import pytest
from skillcornerkit import fixture_bytes, parse_with

from lakehouse.bronze.schemas import TRACKING_FRAMES_EXTRA
from lakehouse.errors import UnexpectedPayloadError
from lakehouse.parsers.skillcorner import MATCHES, TRACKING_FRAMES, derive_frame_rate


def test_frame_rate_snaps_10_from_the_clock() -> None:
    clocks = [i * 100 for i in range(50)]
    assert derive_frame_rate([1] * 50, clocks) == 10


def test_frame_rate_snaps_25_from_a_noisy_cadence() -> None:
    # 40 ms steps with one 50 ms step: the median is 40 ms -> 25 fps.
    clocks = [i * 40 for i in range(50)] + [50 * 40 + 10]
    assert derive_frame_rate([1] * 51, clocks) == 25


def test_frame_rate_uses_within_period_deltas_only() -> None:
    # Period 2 starts at 45:00: the cross-period gap must not count, nor the frames outside every period.
    periods = [None, None] + [1] * 30 + [None] + [2] * 30
    clocks = [None, None] + [i * 40 for i in range(30)] + [None] + [2_700_000 + i * 40 for i in range(30)]
    assert derive_frame_rate(periods, clocks) == 25


def test_frame_rate_does_not_pool_steps_across_periods() -> None:
    # Each period steps 40 ms (25 fps), but the two clocks interleave 20 ms apart. Steps pooled across periods would be
    # 20 ms (50 fps, out of band); only the within-period rule gives 25.
    periods = [1] * 30 + [2] * 30
    clocks = [i * 40 for i in range(30)] + [20 + i * 40 for i in range(30)]
    assert derive_frame_rate(periods, clocks) == 25


def test_frame_rate_out_of_band_fails() -> None:
    with pytest.raises(UnexpectedPayloadError, match=r"50\.00 fps"):
        derive_frame_rate([1] * 20, [i * 20 for i in range(20)])


def test_frame_rate_without_deltas_fails() -> None:
    with pytest.raises(UnexpectedPayloadError, match="no within-period clock steps"):
        derive_frame_rate([1, None], [0, None])
    with pytest.raises(UnexpectedPayloadError, match="no within-period clock steps"):
        derive_frame_rate([], [])


def test_frame_rate_is_a_declared_tracking_column() -> None:
    assert TRACKING_FRAMES_EXTRA["skillcorner"].field("frame_rate").type == pa.int32()


def test_parsed_frames_carry_the_derived_rate() -> None:
    frames = parse_with().tables[TRACKING_FRAMES]
    assert frames.schema.field("frame_rate").type == pa.int32()
    assert set(frames.column("frame_rate").to_pylist()) == {10}


def test_owner_format_match_file_without_match_periods() -> None:
    """The RM-format match file has no `match_periods`; the frame rate still comes from the tracking."""
    match = json.loads(fixture_bytes("match"))
    del match["match_periods"]
    parsed = parse_with(match=json.dumps(match).encode("utf-8"))
    assert parsed.tables[MATCHES].column("match_periods").to_pylist() == [None]
    assert set(parsed.tables[TRACKING_FRAMES].column("frame_rate").to_pylist()) == {10}
