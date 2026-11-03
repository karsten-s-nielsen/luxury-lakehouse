"""The match-file helpers shared by the run engine's collaborators (M1 extraction, plan Task 2).

`MatchFiles` owns reading `ops_matches`, reading a match's file registry, and moving a match's files between
the lake and restricted roots on a visibility change (spec §9). `VisibilityMover`, `MatchWriter` and `_Run` all
depend on it; it depends on nothing in `lakehouse.runs` except `_common` — the import-linter layers contract
forbids a back-edge to `run.py`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from lakehouse.catalog.moves import StagedMove, finish_match_move, stage_match_move
from lakehouse.catalog.visibility import reapply_visibility
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.lineage.model import LINEAGE_TABLE
from lakehouse.logging import get_logger
from lakehouse.raw.archive import RawArchive
from lakehouse.runs._common import RUN_LOGGER_NAME
from lakehouse.runs.history import (
    MatchFile,
    MatchRow,
    VisibilityChange,
    read_table,
    record_visibility_change,
    replace_match,
    set_match_visibility,
)
from lakehouse.storage.base import Storage
from lakehouse.telemetry import step_span

if TYPE_CHECKING:
    from lakehouse.runs.checkpoint import CheckpointPolicy

_log = get_logger(RUN_LOGGER_NAME)


class MatchFiles:
    """Read `ops_matches`, read a match's files, and move a match's files on a visibility change."""

    def __init__(
        self,
        cat: WorkingCatalog,
        storage: Storage,
        archive: RawArchive,
        run_id: str,
        now: Callable[[], datetime],
        *,
        checkpoints: CheckpointPolicy,
    ) -> None:
        self.cat = cat
        self.storage = storage
        self.archive = archive
        self.run_id = run_id
        self.now = now
        self.checkpoints = checkpoints

    def ops_matches(self) -> dict[int, dict[str, Any]]:
        return {row["match_key"]: row for row in read_table(self.cat.conn, "ops_matches")}

    def files_of(self, key: int) -> list[MatchFile]:
        return [MatchFile(**row) for row in read_table(self.cat.conn, "ops_match_files") if row["match_key"] == key]

    def move(self, key: int, visibility: Literal["public", "private"], row: Mapping[str, Any]) -> None:
        """Move one match now: a barrier with its own publish (T3 spec D2). A register move stages the copies, then
        one publish carries the move records, the file registry and lineage; a private move deletes the old public
        copies only after that publish (§29.7)."""
        with step_span(
            "lakehouse.run.move",
            {"lakehouse.match_key": key, "lakehouse.layer": "bronze", "lakehouse.visibility": visibility},
            log=_log,
        ):
            if self.cat.encryption_key is not None:
                self._flip_native(key, visibility)
                self.checkpoints.now("move")
                return
            staged = self._stage_register(key, visibility, row)
            self.checkpoints.now("move")
            if staged is not None:
                finish_match_move(self.storage, staged)

    def move_public_batch(self, rows: Sequence[tuple[int, Mapping[str, Any]]]) -> None:
        """Private-to-public moves of up to B matches with one publish (T3 spec D3, decision 10). The privacy direction
        never comes here: a public-to-private move is `move`, with its own publish."""
        if not rows:
            return
        staged: list[StagedMove] = []
        with step_span(
            "lakehouse.run.move_batch", {"lakehouse.layer": "bronze", "lakehouse.rows": len(rows)}, log=_log
        ):
            for key, row in rows:
                if self.cat.encryption_key is not None:
                    self._flip_native(key, "public")
                elif (move := self._stage_register(key, "public", row)) is not None:
                    staged.append(move)
            self.checkpoints.now("move_batch")
            for move in staged:
                finish_match_move(self.storage, move)

    def _stage_register(
        self, key: int, visibility: Literal["public", "private"], row: Mapping[str, Any]
    ) -> StagedMove | None:
        """Copy the match's files to the target root, swap the catalog, and update the registry and lineage; no
        publish."""
        staged = stage_match_move(
            self.cat, self.storage, key, "restricted" if visibility == "private" else "lake", archive=self.archive
        )
        records = staged.records if staged is not None else ()
        moved = {(r.old_key.root, r.old_key.path): r.new_key for r in records if r.new_key is not None}
        files: list[MatchFile] = []
        for file in self.files_of(key):
            new = moved.get((file.root, file.path))  # the file registry follows the copies (new write ids)
            files.append(file if new is None else dataclasses.replace(file, root=new.root, path=new.path))
        updated = dataclasses.replace(
            MatchRow(**dict(row)), visibility=visibility, run_id=self.run_id, updated_at=self.now()
        )
        replace_match(self.cat.conn, updated, [], files)
        self.cat.conn.execute(
            f"UPDATE lake.main.{LINEAGE_TABLE} SET visibility = ? WHERE match_key = ? AND kept",  # noqa: S608 - constant name
            [visibility, key],
        )
        return staged

    def _flip_native(self, key: int, visibility: Literal["public", "private"]) -> None:
        """Flip a match's visibility on an encrypted-local lake without moving a file; the caller publishes. The bronze
        rows live in one encrypted catalog, so the restricted copy is the `_visibility` column, not a second root:
        update it for the match's rows in every native bronze table, set the authoritative `ops_matches.visibility`,
        update lineage and the visibility ledger. The publish that follows holds all of them, so `_visibility` always
        equals `ops_matches.visibility` in a published version (plan T9 integrity invariant)."""
        conn = self.cat.conn
        reapply_visibility(conn, key, visibility)
        set_match_visibility(conn, key, visibility, run_id=self.run_id, updated_at=self.now())
        conn.execute(
            f"UPDATE lake.main.{LINEAGE_TABLE} SET visibility = ? WHERE match_key = ? AND kept",  # noqa: S608 - constant name
            [visibility, key],
        )
        record_visibility_change(
            conn, VisibilityChange(match_key=key, new_visibility=visibility, changed_at=self.now(), run_id=self.run_id)
        )
