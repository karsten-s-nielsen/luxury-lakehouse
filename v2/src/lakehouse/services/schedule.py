"""The schedule's service (spec §18; plan Task 25): saving the schedule setting, then applying it.

Locally, an enabled schedule installs the native OS task (idempotent) and a disabled one removes it. On HF, an enabled
schedule reschedules and resumes the owner-created scheduled job and a disabled one suspends it; nothing creates a job
or sends a secret. The setting is saved first, so a failure to apply it is visible against the saved value.

`status` and `uninstall` need no lake (`settings` may be `None`), so a lost or disconnected lake can still lose
its OS task; `install` and `set` read the setting from the lake.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from lakehouse.config.model import ScheduleConfig
from lakehouse.schedule.base import ScheduleBackend, ScheduleStatus
from lakehouse.services.settings import SettingsService

__all__ = ["HfSchedule", "ScheduleService"]


@runtime_checkable
class HfSchedule(Protocol):
    def reschedule(self, cron: str) -> None: ...

    def suspend(self) -> None: ...

    def resume(self) -> None: ...

    def status(self) -> ScheduleStatus: ...


class ScheduleService:
    def __init__(
        self,
        settings: SettingsService | None,
        backend: ScheduleBackend | HfSchedule,
        *,
        env: Mapping[str, str],
        system: bool = False,
        run_as: str | None = None,
    ) -> None:
        self._settings = settings
        self._backend = backend
        self._env = env
        self._system = system
        self._run_as = run_as

    def get(self) -> ScheduleStatus:
        return self._backend.status()

    def install(self) -> ScheduleStatus:
        """`lakehouse schedule install`: install the saved schedule as the native OS task (idempotent); no save."""
        backend = self._native()
        cfg = self._service().load()[0].schedule
        if not cfg.enabled or cfg.cron is None:
            raise ValueError(
                "the saved schedule is off: set schedule.cron and then schedule.enabled with lakehouse config set"
            )
        backend.install(cfg.cron, system=self._system, env=self._env, run_as=self._run_as)
        return backend.status()

    def uninstall(self) -> ScheduleStatus:
        """`lakehouse schedule uninstall`: remove the native OS task; the saved setting stays as it is."""
        backend = self._native()
        backend.uninstall()
        return backend.status()

    def _service(self) -> SettingsService:
        if self._settings is None:
            raise ValueError("the schedule setting lives in the lake: name the lake (lakehouse init, or --lake)")
        return self._settings

    def _native(self) -> ScheduleBackend:
        if isinstance(self._backend, HfSchedule):
            raise ValueError("on HF the owner-created scheduled job holds the schedule: change it on the Schedule page")
        return self._backend

    def set(self, cfg: ScheduleConfig, *, expected_version: int, operator: str) -> ScheduleStatus:
        service = self._service()
        settings, _ = service.load()
        service.save(
            settings.model_copy(update={"schedule": cfg}), expected_version=expected_version, operator=operator
        )
        backend = self._backend
        if isinstance(backend, HfSchedule):
            if cfg.enabled and cfg.cron is not None:
                backend.reschedule(cfg.cron)
                backend.resume()
            else:
                backend.suspend()
        elif cfg.enabled and cfg.cron is not None:
            backend.install(cfg.cron, system=self._system, env=self._env, run_as=self._run_as)
        else:
            backend.uninstall()
        return backend.status()
