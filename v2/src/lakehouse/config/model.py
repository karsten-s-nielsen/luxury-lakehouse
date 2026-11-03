"""The config model (spec §8): modules with an active switch and a filter, the schedule, retention and resource caps.

Every model is frozen. The config is versioned outside the catalog by `lakehouse.config.store`. Clause ids go only into
parameterised SQL and JSON values; they never form a storage key, a file path or SQL text.
"""

from __future__ import annotations

import re
from importlib import resources
from typing import Annotated, Any, Final, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ModuleId = Literal[
    "statsbomb_public",
    "wyscout_public",
    "idsse_public",
    "skillcorner_public",
    "gradientsports_public",
    "idsse_pining",
    "statsbomb_pining",
    "skillcorner_pining",
    "gradientsports_pining",
]
MODULE_IDS: Final[tuple[ModuleId, ...]] = (
    "statsbomb_public",
    "wyscout_public",
    "idsse_public",
    "skillcorner_public",
    "gradientsports_public",
    "idsse_pining",
    "statsbomb_pining",
    "skillcorner_pining",
    "gradientsports_pining",
)
STATSBOMB_MODULES: Final[frozenset[str]] = frozenset({"statsbomb_public", "statsbomb_pining"})
InstallKind = Literal["laptop", "hf", "staging"]

# spec §20: clause ids are validated before use
_CLAUSE_ID: Final = re.compile(r"^[A-Za-z0-9_:\-/.]+$")
_MEMORY_LIMIT: Final = re.compile(r"^[0-9]+(?:\.[0-9]+)?(?:KB|MB|GB|TB)$")
_CRON_FIELD: Final = re.compile(r"^[0-9*/,\-]+$")
_CRON_MACROS: Final = frozenset({"@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@annually"})


def _check_clause_id(value: str) -> str:
    if not _CLAUSE_ID.match(value):
        raise ValueError(f"invalid id {value!r}: allowed characters are letters, digits and _ : - / .")
    if ".." in value.split("/") or value == "..":
        raise ValueError(f"invalid id {value!r}: a '..' part is not allowed")
    return value


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CompetitionClause(_Frozen):
    """One competition with a list of seasons, or "all" seasons (live: a later season is included automatically)."""

    kind: Literal["competition"] = "competition"
    competition_id: str
    seasons: list[str] | Literal["all"]

    @field_validator("competition_id")
    @classmethod
    def _competition(cls, value: str) -> str:
        return _check_clause_id(value)

    @field_validator("seasons")
    @classmethod
    def _seasons(cls, value: list[str] | Literal["all"]) -> list[str] | Literal["all"]:
        if value == "all":
            return value
        if not value:
            raise ValueError("a season list must not be empty; use 'all' for every season")
        return [_check_clause_id(season) for season in value]


class MatchClause(_Frozen):
    """Single matches, by the provider's own match ids."""

    kind: Literal["match"] = "match"
    match_ids: list[str]

    @field_validator("match_ids")
    @classmethod
    def _matches(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("a match clause needs at least one match id")
        return [_check_clause_id(match_id) for match_id in value]


class AllClause(_Frozen):
    """Every match of the module's source (the "active, all" defaults of spec §8.2)."""

    kind: Literal["all"] = "all"


Clause = Annotated[CompetitionClause | MatchClause | AllClause, Field(discriminator="kind")]


class ModuleConfig(_Frozen):
    module_id: ModuleId
    active: bool
    filter: list[Clause]
    statsbomb_360_only: bool = False

    @model_validator(mode="after")
    def _three_sixty(self) -> ModuleConfig:
        if self.statsbomb_360_only and self.module_id not in STATSBOMB_MODULES:
            raise ValueError(f"{self.module_id}: the 360 option exists only for the StatsBomb modules (spec §29.13)")
        return self


class ScheduleConfig(_Frozen):
    enabled: bool = False
    cron: str | None = None

    @model_validator(mode="after")
    def _cron(self) -> ScheduleConfig:
        if self.cron is not None:
            fields = self.cron.split()
            macro = len(fields) == 1 and fields[0] in _CRON_MACROS
            if not macro and (len(fields) != 5 or not all(_CRON_FIELD.match(field) for field in fields)):
                raise ValueError(f"invalid cron expression {self.cron!r}: five fields or a macro such as @daily")
        if self.enabled and self.cron is None:
            raise ValueError("an enabled schedule needs a cron expression")
        return self


class RetentionConfig(_Frozen):
    snapshot_days: int = Field(default=8, ge=1)
    version_days: int = Field(default=7, ge=1)

    @model_validator(mode="after")
    def _longer(self) -> RetentionConfig:
        # spec §10.7: snapshot retention must stay longer than version retention, or cleanup may delete a file that
        # a kept catalog version names
        if self.snapshot_days <= self.version_days:
            raise ValueError("snapshot_days must be greater than version_days (spec §10.7)")
        return self


class ResourceCaps(_Frozen):
    threads: int = Field(default=4, ge=1)
    memory_limit: str = "3GB"
    arrow_cpus: int = Field(default=4, ge=1)

    @field_validator("memory_limit")
    @classmethod
    def _memory(cls, value: str) -> str:
        if not _MEMORY_LIMIT.match(value):
            raise ValueError(f"invalid memory_limit {value!r}: for example '3GB'")
        return value


class CheckpointConfig(_Frozen):
    """The catalog publish cadence and version grace (T3 spec D1, D3, D4, D8; decision 21).

    `minutes` (T) and `matches` (M) bound a run's deferrable publishes; `grace_minutes` (G) keeps a superseded version
    readable; `move_batch` (B) caps a private-to-public move batch; the two `warn_*` thresholds raise the
    `catalog_growth` ops warning."""

    minutes: int = Field(default=5, ge=1)
    matches: int = Field(default=50, ge=1)
    grace_minutes: int = Field(default=2, ge=1)
    move_batch: int = Field(default=50, ge=1)
    warn_versions: int = Field(default=200, ge=1)
    warn_catalog_mb: int = Field(default=1024, ge=1)  # at most half the HF single-part limit (T6 spec D2)


class MaintenanceConfig(_Frozen):
    """Run-end maintenance of the small DuckLake data files (T12 spec D3): at most `merge_budget_seconds` of rewriting
    and merging per run; the rest waits for the next run (`lakehouse sweep --allow-large` has no budget)."""

    merge_budget_seconds: int = Field(default=300, ge=0)


class Settings(_Frozen):
    schema_version: Literal[1]
    modules: list[ModuleConfig]
    schedule: ScheduleConfig
    retention: RetentionConfig
    caps: ResourceCaps
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)  # a pre-T3 saved config has no block
    maintenance: MaintenanceConfig = Field(default_factory=MaintenanceConfig)  # nor has a pre-T12 one
    raw_archive: bool
    auto_accept_recommended: bool
    sweep_max_fraction: float = Field(default=0.10, gt=0, le=1)
    pining_base_url: str
    truthset_owner: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> Settings:
        seen = [module.module_id for module in self.modules]
        if sorted(seen) != sorted(MODULE_IDS):
            raise ValueError(f"every module must appear exactly once; got {seen}")
        if not self.pining_base_url.startswith("https://"):
            raise ValueError("pining_base_url must use https (spec §20)")
        return self

    def module(self, module_id: ModuleId) -> ModuleConfig:
        return next(module for module in self.modules if module.module_id == module_id)


def load_defaults(install_kind: InstallKind) -> Settings:
    """The defaults of one install kind (spec §8.2, §13.2, §15.6, §18), from the packaged `defaults.yaml`."""
    text = resources.files("lakehouse.config").joinpath("defaults.yaml").read_text(encoding="utf-8")
    data: dict[str, Any] = yaml.safe_load(text)
    overrides: dict[str, Any] = data.pop("install_kinds")[install_kind]
    return Settings.model_validate({**data, **overrides})
