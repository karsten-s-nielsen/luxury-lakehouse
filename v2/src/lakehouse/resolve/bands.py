"""The three bands of the review queue and the margin of "Accept recommended" (spec §15.4 to §15.6; plan Task 22).

A score at or above `auto_link_min` links automatically, a score at or below `reject_max` is rejected, and the middle
goes to the review queue. "Recommended" means the top candidate, and only when it beats the runner-up by `margin`.
The values come from the truth-set measurement (plan Task 34) and are stored in `ops_resolution_bands`; an entity type
without a measured row has no bands, so every candidate goes to the queue and none is recommended.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import duckdb

from lakehouse.resolve.model import Candidate, Entity
from lakehouse.resolve.tables import insert, read_rows

__all__ = ["Band", "Bands", "band_of", "is_recommended", "load_bands", "write_bands"]

Band = Literal["auto", "queue", "reject"]


@dataclass(frozen=True)
class Bands:
    auto_link_min: float
    reject_max: float
    margin: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.reject_max < self.auto_link_min:
            raise ValueError(
                f"bands need 0 <= reject_max < auto_link_min, not {self.reject_max} and {self.auto_link_min}"
            )
        if self.margin < 0.0:
            raise ValueError(f"the margin cannot be negative: {self.margin}")


def load_bands(conn: duckdb.DuckDBPyConnection, entity: Entity) -> Bands | None:
    """The newest measured bands of `entity`; `None` before any measurement (measured values only)."""
    rows = read_rows(conn, "ops_resolution_bands", entity=entity)
    if not rows:
        return None
    newest = max(rows, key=lambda row: row["measured_at"])
    return Bands(float(newest["auto_link_min"]), float(newest["reject_max"]), float(newest["margin"]))


def write_bands(
    conn: duckdb.DuckDBPyConnection,
    entity: Entity,
    bands: Bands,
    *,
    truthset_rows: int,
    measured_by: str,
    measured_at: datetime,
) -> None:
    """Record a measurement (plan Task 34). Earlier rows stay as the history of the bands."""
    if truthset_rows < 1 or not measured_by:
        raise ValueError("bands are measured on a truth set: name its row count and who measured")
    insert(
        conn,
        "ops_resolution_bands",
        [
            {
                "entity": entity,
                "auto_link_min": bands.auto_link_min,
                "reject_max": bands.reject_max,
                "margin": bands.margin,
                "truthset_rows": truthset_rows,
                "measured_by": measured_by,
                "measured_at": measured_at,
            }
        ],
    )


def band_of(score: float, bands: Bands | None) -> Band:
    if bands is None:
        return "queue"
    if score >= bands.auto_link_min:
        return "auto"
    if score <= bands.reject_max:
        return "reject"
    return "queue"


def is_recommended(candidate: Candidate, bands: Bands | None) -> bool:
    """The candidate beats its runner-up by the measured margin (a tie never does); no bands, no recommendation."""
    if bands is None:
        return False
    rival = candidate.runner_up if candidate.runner_up is not None else 0.0
    return candidate.score - rival >= bands.margin and candidate.score > rival
