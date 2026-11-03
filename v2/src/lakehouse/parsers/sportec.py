"""A typed seam over silly-kicks' DFL / Sportec parse (spec §12.7). silly-kicks ships no type stubs and returns pandas
frames; this module keeps that untyped surface in one place and hands typed numpy arrays and lists to the parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import numpy.typing as npt
import silly_kicks.providers.sportec as _sportec  # pyright: ignore[reportMissingTypeStubs]
import silly_kicks.providers.sportec.parse as _sportec_parse  # pyright: ignore[reportMissingTypeStubs]

__all__ = ["SportecTracking", "parse_tracking", "silly_kicks_game_sections"]


class SportecTracking:
    """The silly-kicks tracking rows (one per player frame), read column by column."""

    def __init__(self, frame: Any) -> None:
        self._frame: Any = frame

    def __len__(self) -> int:
        return len(self._frame)

    def ints(self, column: str) -> npt.NDArray[np.int64]:
        return cast("npt.NDArray[np.int64]", self._frame[column].to_numpy(dtype=np.int64))

    def floats(self, column: str) -> npt.NDArray[np.float64]:
        """The column as float64, with NaN for a missing value."""
        return cast("npt.NDArray[np.float64]", self._frame[column].to_numpy(dtype=np.float64, na_value=np.nan))

    def texts(self, column: str) -> list[str | None]:
        series: Any = self._frame[column]
        values = cast("list[object]", series.tolist())
        missing = cast("list[bool]", series.isna().tolist())
        return [None if gone else str(value) for value, gone in zip(values, missing, strict=True)]


def parse_tracking(positions: Path, matchinformation: Path, *, match_id: str) -> SportecTracking:
    """`parse_dfl_tracking` with the `MatchInfo` of `parse_dfl_match_info`, as IRIS and v1 call them."""
    info: Any = _sportec.parse_dfl_match_info(str(matchinformation))  # pyright: ignore[reportUnknownMemberType]
    frame: Any = _sportec.parse_dfl_tracking(str(positions), match_info=info, match_id=match_id)  # pyright: ignore[reportUnknownMemberType]
    return SportecTracking(frame)


def silly_kicks_game_sections() -> dict[str, int]:
    """silly-kicks' private game-section table, for the parity test of `idsse.GAME_SECTIONS`."""
    return dict(cast("dict[str, int]", getattr(_sportec_parse, "_SECTION_TO_PERIOD")))  # noqa: B009 - private name
