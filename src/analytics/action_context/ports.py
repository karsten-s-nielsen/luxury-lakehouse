"""Hexagon ports — domain-defined Protocols (WorkUnit-in, pandas-out).

A non-Databricks runtime implements these against any store; the domain is
untouched. Spark/Delta adapters live in ``ingestion.action_context``; local
Parquet adapters in ``analytics.action_context.local``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pandas as pd

    from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


class FrameSource(Protocol):
    def frames(self, wu: WorkUnit) -> FrameBundle: ...


class ActionsSource(Protocol):
    def actions(self, wu: WorkUnit) -> pd.DataFrame: ...


class XtSource(Protocol):
    def grid(self) -> tuple[list[list[float]], int, int]: ...


class MatchMetadataSource(Protocol):
    def metadata(self, wu: WorkUnit) -> MatchMeta: ...


class ResultSink(Protocol):
    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int: ...
