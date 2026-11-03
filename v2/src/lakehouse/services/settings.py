"""The Settings page's service (spec §8.3; plan Task 25): every config save goes through here, under the config lock
of `ConfigStore`; the storage roots are shown read-only with the source they were resolved from."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from lakehouse.config.model import Clause, ModuleConfig, ModuleId, Settings
from lakehouse.config.store import ConfigStore, ConfigVersionInfo
from lakehouse.storage.base import Storage
from lakehouse.storage.roots import ResolvedRoots

__all__ = ["TIMELINE_LIMIT", "SettingsService"]

TIMELINE_LIMIT: Final = 2000  # spec §19.5: a timeline shows at most 2000 rows


def _require_known(module_id: ModuleId, settings: Settings) -> None:
    """Reject a config write for a module id the config does not hold (review V2UI-IMPL-24). Without this an unknown
    id leaves every module unchanged and `save` still succeeds, so the action reports "Done" and nothing changed."""
    if module_id not in {module.module_id for module in settings.modules}:
        raise ValueError(f"unknown module {module_id!r}")


class SettingsService:
    def __init__(self, storage: Storage, roots: ResolvedRoots) -> None:
        self._store = ConfigStore(storage)
        self._roots = roots

    def load(self) -> tuple[Settings, int]:
        return self._store.load()

    def save(self, draft: Settings, *, expected_version: int, operator: str) -> int:
        return self._store.save(draft, expected_version=expected_version, operator=operator)

    def set_module_active(self, module_id: ModuleId, *, active: bool, operator: str) -> int:
        """Turn one module on or off under the config lock (spec §8). The Modules page's active switch calls this
        through the guarded dispatch; the page holds no config logic."""
        settings, version = self.load()
        _require_known(module_id, settings)
        modules = [m.model_copy(update={"active": active}) if m.module_id == module_id else m for m in settings.modules]
        return self.save(settings.model_copy(update={"modules": modules}), expected_version=version, operator=operator)

    def set_module_filter(
        self,
        module_id: ModuleId,
        *,
        clauses: Sequence[Clause],
        statsbomb_360_only: bool | None = None,
        operator: str,
    ) -> int:
        """Save one module's filter clauses (and the StatsBomb 360-only option, when given) under the config lock
        (spec §8, §29.13). A narrowed filter never deletes data (the diff shows what would stop updating)."""
        settings, version = self.load()
        _require_known(module_id, settings)

        def updated(module: ModuleConfig) -> ModuleConfig:
            changes: dict[str, object] = {"filter": list(clauses)}
            if statsbomb_360_only is not None:
                changes["statsbomb_360_only"] = statsbomb_360_only
            return module.model_copy(update=changes)

        modules = [updated(m) if m.module_id == module_id else m for m in settings.modules]
        return self.save(settings.model_copy(update={"modules": modules}), expected_version=version, operator=operator)

    def history(self, limit: int = 500) -> list[ConfigVersionInfo]:
        if not 1 <= limit <= TIMELINE_LIMIT:
            raise ValueError(f"limit must be 1 to {TIMELINE_LIMIT}")
        return self._store.history(limit)

    def storage_roots(self) -> ResolvedRoots:
        return self._roots
