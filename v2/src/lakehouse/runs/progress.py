"""The run's progress object (spec §17.1; plan Task 21).

A run writes `runs/progress/<run_id>.json` in the lake root at each step, outside the catalog, because the catalog
changes only at catalog commits. The Ops page reads it for live progress. It holds no match content.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict

from lakehouse.storage.base import Storage

__all__ = ["PROGRESS_PREFIX", "Progress", "progress_key", "read_progress", "write_progress"]

PROGRESS_PREFIX: Final = "runs/progress/"


class Progress(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    trigger: str
    status: Literal["running", "ok", "failed", "skipped"]
    step: str
    module_id: str | None
    done: int
    total: int | None
    started_at: AwareDatetime
    updated_at: AwareDatetime
    message: str | None

    def advance(
        self,
        *,
        at: datetime,
        step: str | None = None,
        module_id: str | None = None,
        done: int | None = None,
        total: int | None = None,
        status: Literal["running", "ok", "failed", "skipped"] | None = None,
        message: str | None = None,
    ) -> Progress:
        changes: dict[str, object] = {"updated_at": at}
        for name, value in (
            ("step", step),
            ("module_id", module_id),
            ("done", done),
            ("total", total),
            ("status", status),
            ("message", message),
        ):
            if value is not None:
                changes[name] = value
        return self.model_copy(update=changes)


def progress_key(run_id: str) -> str:
    return f"{PROGRESS_PREFIX}{run_id}.json"


def write_progress(storage: Storage, progress: Progress) -> None:
    storage.put("lake", progress_key(progress.run_id), progress.model_dump_json().encode("utf-8"))


def read_progress(storage: Storage, run_id: str) -> Progress:
    return Progress.model_validate_json(storage.get("lake", progress_key(run_id)))
