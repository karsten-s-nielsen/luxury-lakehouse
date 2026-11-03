"""The HF scheduled job (spec §18, §29.5; plan Task 24).

The owner creates the scheduled job once from the CLI, with the gateway key and the pining owner token as job secrets.
The Space only changes its schedule, pauses, resumes and triggers it: no method here creates a job, and none sends a
secret. The scheduled job id comes from the Space variable `LAKEHOUSE_HF_SCHEDULED_JOB_ID` (plan Task 33).

Separation of duties (two-token design). A scheduled job under a personal namespace can only be managed by a classic
`read`/`write`-role token — HF's fine-grained Jobs permission is organisation-only, and HF OAuth has no jobs scope. A
role token also reads every repo the owner can read, including the restricted bucket, so it must NEVER touch the lake
data path. This module is the ONLY reader of `LAKEHOUSE_HF_JOBS_TOKEN` (the job-control token); the storage and page
reads use the lake-only `HF_TOKEN`. The confinement is enforced by `test_jobs_token_is_confined_to_the_schedule_seam`.
When `LAKEHOUSE_HF_JOBS_TOKEN` is unset the token falls back to `HF_TOKEN` (the single-token case: an org job managed by
a fine-grained org token, or a lake-only setup with no restricted bucket).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError

from lakehouse.errors import HttpStatusError
from lakehouse.schedule.base import ScheduleStatus, parse_cron
from lakehouse.storage.roots import HF_TOKEN_ENV

__all__ = ["JOBS_TOKEN_ENV", "SCHEDULED_JOB_ID_ENV", "HfJobsSchedule"]

SCHEDULED_JOB_ID_ENV: Final = "LAKEHOUSE_HF_SCHEDULED_JOB_ID"
# The job-control token (classic read/write role). Read ONLY here; it must never reach the lake data path (see the
# module docstring). Falls back to HF_TOKEN when unset (the single-token org / lake-only case).
JOBS_TOKEN_ENV: Final = "LAKEHOUSE_HF_JOBS_TOKEN"  # noqa: S105 - an environment variable name, not a secret


class HfJobsSchedule:
    def __init__(self, api: HfApi, scheduled_job_id: str) -> None:
        if not scheduled_job_id:
            raise ValueError(f"no scheduled job id: set {SCHEDULED_JOB_ID_ENV} (the owner creates the job once)")
        self._api: Any = api
        self._id = scheduled_job_id

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> HfJobsSchedule:
        """Build the schedule from the environment: the job-control token and the owner-created job id. This is the
        one place the Jobs-API client is built and the ONLY reader of `LAKEHOUSE_HF_JOBS_TOKEN`, so the job-control
        token never reaches the lake data path (the confinement the two-token design relies on). The token falls back
        to `HF_TOKEN` when `LAKEHOUSE_HF_JOBS_TOKEN` is unset (the single-token case)."""
        token = env.get(JOBS_TOKEN_ENV) or env.get(HF_TOKEN_ENV)
        return cls(HfApi(token=token), env.get(SCHEDULED_JOB_ID_ENV, ""))

    def reschedule(self, cron: str) -> None:
        """Change the schedule (a five-field cron expression or a macro such as `@daily`)."""
        spec = parse_cron(cron)
        self._api.update_scheduled_job_schedule(scheduled_job_id=self._id, schedule=spec.text)

    def suspend(self) -> None:
        self._api.suspend_scheduled_job(scheduled_job_id=self._id)

    def resume(self) -> None:
        self._api.resume_scheduled_job(scheduled_job_id=self._id)

    def trigger(self) -> str:
        """Start one run of the scheduled job now ("Run now"); the new job's id. A refusal (for example under
        `--no-concurrency` while a run is active) raises `HttpStatusError` with HF's status."""
        try:
            job = self._api.trigger_scheduled_job(scheduled_job_id=self._id)
        except HfHubHTTPError as exc:
            raise HttpStatusError(exc.response.status_code, f"scheduled job {self._id}: trigger") from exc
        return str(job.id)

    def status(self) -> ScheduleStatus:
        try:
            info = self._api.inspect_scheduled_job(scheduled_job_id=self._id)
            status = info.status
            last = status.last_job if status is not None else None
            result = None
            if last is not None:
                stage = self._api.inspect_job(job_id=last.id).status.stage
                result = None if stage is None else str(stage)
        except HfHubHTTPError as exc:
            # a refused read (for example the job-control token lacks job access) reaches the services as
            # HttpStatusError, so the Settings page shows the schedule as unavailable and still renders its config
            raise HttpStatusError(exc.response.status_code, f"scheduled job {self._id}: status") from exc
        return ScheduleStatus(
            backend="hf_jobs",
            installed=True,
            enabled=not bool(info.suspend),
            cron=info.schedule,
            next_run=status.next_job_run_at if status is not None else None,
            last_result=result,
        )
