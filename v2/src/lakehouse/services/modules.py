"""The Modules page's service (spec §6, §7.2, §8, §19.5; plan Task 25).

One row per module: its display name, whether it is active, its filter in plain words (counts, never raw ids), how
many matches the lake holds from it, its last run, the pining token status (never the value) and a warning that the
page shows next to it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

import duckdb

from lakehouse.config.model import AllClause, CompetitionClause, ModuleConfig, ModuleId
from lakehouse.config.store import ConfigStore
from lakehouse.runs.history import read_table
from lakehouse.services.catalog import read_catalog
from lakehouse.services.runs import RunSummary, run_counts
from lakehouse.sources.pining import PUBLIC_MARKER
from lakehouse.storage.base import Storage

__all__ = ["DISPLAY_NAMES", "EMPTY_FILTER_WARNING", "ModuleService", "ModuleStatus"]

# spec §6, the module table
DISPLAY_NAMES: Final[dict[str, str]] = {
    "statsbomb_public": "StatsBomb (incl. 360)",
    "wyscout_public": "Wyscout",
    "idsse_public": "IDSSE",
    "skillcorner_public": "SkillCorner",
    "gradientsports_public": "Gradient Sports",
    "idsse_pining": "IDSSE (pining)",
    "statsbomb_pining": "StatsBomb (pining)",
    "skillcorner_pining": "SkillCorner (pining)",
    "gradientsports_pining": "Gradient Sports (pining)",
}
EMPTY_FILTER_WARNING: Final = "no tournament selected: the next run downloads nothing for this module"
TokenStatus = Literal["present", "missing", "public-only"]


@dataclass(frozen=True)
class ModuleStatus:
    module_id: ModuleId
    display_name: str
    active: bool
    filter_summary: str
    match_count: int | None
    last_run: RunSummary | None
    token_status: TokenStatus | None
    warning: str | None


def filter_summary(module: ModuleConfig) -> str:
    """The module's clauses in plain words, with counts (the page shows tournament names from the preview)."""
    if not module.filter:
        return "nothing selected"
    if any(isinstance(clause, AllClause) for clause in module.filter):
        text = "all tournaments"
    else:
        competitions = [c for c in module.filter if isinstance(c, CompetitionClause)]
        matches = sum(len(c.match_ids) for c in module.filter if not isinstance(c, CompetitionClause | AllClause))
        parts: list[str] = []
        whole = sum(1 for c in competitions if c.seasons == "all")
        seasons = sum(len(c.seasons) for c in competitions if c.seasons != "all")
        if whole:
            parts.append(f"{whole} whole competition{'s' if whole != 1 else ''}")
        if seasons:
            parts.append(f"{seasons} tournament{'s' if seasons != 1 else ''}")
        if matches:
            parts.append(f"{matches} single match{'es' if matches != 1 else ''}")
        text = ", ".join(parts)
    return text + (", 360 data only" if module.statsbomb_360_only else "")


class ModuleService:
    def __init__(self, storage: Storage, *, env: Mapping[str, str]) -> None:
        self._storage = storage
        self._env = env

    def _token_status(self, module_id: str) -> TokenStatus | None:
        if not module_id.endswith("_pining"):
            return None
        if self._env.get("PINING_FOR_THE_DATA_TOKEN") or self._env.get("PINING_LIST_TOKEN"):
            return "present"  # the status only: the value never leaves the environment
        return "public-only"

    def _catalog(self) -> tuple[dict[str, int], dict[str, dict[str, Any]], dict[str, int]]:
        def read(conn: duckdb.DuckDBPyConnection) -> tuple[dict[str, int], dict[str, dict[str, Any]], dict[str, int]]:
            matches: dict[str, set[int]] = {}
            for row in read_table(conn, "ops_match_sources"):
                matches.setdefault(row["module_id"], set()).add(row["match_key"])
            latest: dict[str, dict[str, Any]] = {}
            for row in read_table(conn, "ops_module_runs"):
                kept = latest.get(row["module_id"])
                if kept is None or row["started_at"] > kept["started_at"]:
                    latest[row["module_id"]] = row
            runs = {row["run_id"]: row for row in read_table(conn, "ops_runs")}
            errors: dict[str, int] = {}
            for row in read_table(conn, "ops_errors"):
                if row["severity"] == "error":
                    errors[row["run_id"]] = errors.get(row["run_id"], 0) + 1
            last = {module: runs[row["run_id"]] for module, row in latest.items() if row["run_id"] in runs}
            return {m: len(keys) for m, keys in matches.items()}, last, errors

        return read_catalog(self._storage, read, empty=({}, {}, {}))

    def list_modules(self) -> list[ModuleStatus]:
        settings, _ = ConfigStore(self._storage).load()
        counts, last, errors = self._catalog()
        statuses: list[ModuleStatus] = []
        for module in settings.modules:
            access = self._token_status(module.module_id)
            run = last.get(module.module_id)
            warning = (
                EMPTY_FILTER_WARNING
                if module.active and not module.filter
                else PUBLIC_MARKER
                if access == "public-only"
                else None
            )
            statuses.append(
                ModuleStatus(
                    module_id=module.module_id,
                    display_name=DISPLAY_NAMES.get(module.module_id, module.module_id),
                    active=module.active,
                    filter_summary=filter_summary(module),
                    match_count=counts.get(module.module_id, 0),
                    last_run=None
                    if run is None
                    else RunSummary(
                        run_id=run["run_id"],
                        trigger=run["trigger"] or "schedule",
                        status=run["status"],
                        started_at=run["started_at"],
                        ended_at=run["ended_at"],
                        counts=run_counts(run),
                        error_count=errors.get(run["run_id"], 0),
                    ),
                    token_status=access,
                    warning=warning,
                )
            )
        return statuses
