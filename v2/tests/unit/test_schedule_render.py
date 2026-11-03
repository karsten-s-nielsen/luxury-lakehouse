"""Scheduling backends, rendered and installed with a recorded `subprocess.run` (spec §17.5, §18, §20; plan Task 24).
The real installs on each OS are `tests/os/test_schedule_*.py`."""

from __future__ import annotations

import plistlib
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from defusedxml import ElementTree

from lakehouse.limits import OPEN_FILES_TARGET
from lakehouse.schedule.base import STOP_GRACE_SECONDS, TASK_NAME, backend_for_host, check_env, next_run, parse_cron
from lakehouse.schedule.cron import CronBackend, render_cron_file, render_cron_line
from lakehouse.schedule.hf_jobs import HfJobsSchedule
from lakehouse.schedule.launchd import LaunchdBackend, render_plist
from lakehouse.schedule.systemd import SystemdBackend, render_units
from lakehouse.schedule.windows import WindowsBackend, render_task_xml

COMMAND = ["C:/Program Files/Python/python.exe", "-m", "lakehouse.cli", "run", "--trigger", "schedule"]
ENV = {"LAKEHOUSE_LAKE_ROOT": "D:/My Lake", "HF_HOME": "/Volumes/Development/hf"}
NS = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}


class Recorder:
    """A `subprocess.run` stand-in: records each call and answers from a table of (argv prefix -> result)."""

    def __init__(self, answers: dict[tuple[str, ...], tuple[int, str]] | None = None) -> None:
        self.calls: list[list[str]] = []
        self.inputs: list[str | None] = []
        self.answers = answers or {}

    def __call__(self, args: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert kwargs.get("shell", False) is False  # never through a shell (§20)
        argv = list(args)
        self.calls.append(argv)
        self.inputs.append(kwargs.get("input"))
        code, out = next((v for k, v in self.answers.items() if tuple(argv[: len(k)]) == k), (0, ""))
        if code != 0 and kwargs.get("check"):
            raise subprocess.CalledProcessError(code, argv, out, "")
        return subprocess.CompletedProcess(argv, code, out, "")


# --- cron, env -------------------------------------------------------------------------------------------------------


def test_parse_cron_and_macros() -> None:
    spec = parse_cron("*/15 6-8 * * 1,3")
    assert spec.minutes == (0, 15, 30, 45) and spec.hours == (6, 7, 8) and spec.weekdays == (1, 3)
    assert parse_cron("@daily").minutes == (0,) and parse_cron("@daily").hours == (0,)
    assert parse_cron("0 0 * * 7").weekdays == (0,)  # 7 is Sunday too
    for bad in ("61 * * * *", "* * *", "a * * * *", "*/0 * * * *"):
        with pytest.raises(ValueError, match="cron"):
            parse_cron(bad)


def test_next_run() -> None:
    after = datetime(2026, 10, 27, 6, 30, tzinfo=UTC)  # a Tuesday
    assert next_run(parse_cron("0 6 * * *"), after) == datetime(2026, 10, 28, 6, 0, tzinfo=UTC)
    assert next_run(parse_cron("45 6 * * *"), after) == datetime(2026, 10, 27, 6, 45, tzinfo=UTC)
    assert next_run(parse_cron("0 9 * * 1"), after) == datetime(2026, 11, 2, 9, 0, tzinfo=UTC)
    # both day fields restricted: either one matches (cron's rule)
    assert next_run(parse_cron("0 0 1 * 3"), after) == datetime(2026, 10, 28, 0, 0, tzinfo=UTC)


def test_render_env_no_secrets(tmp_path: Path) -> None:
    assert check_env(ENV) == dict(sorted(ENV.items()))
    for key in ("HF_TOKEN", "PINING_FOR_THE_DATA_TOKEN", "HF_S3_SECRET_ACCESS_KEY", "HF_S3_ACCESS_KEY_ID", "PASSWORD"):
        with pytest.raises(ValueError, match="secret"):
            check_env({**ENV, key: "x"})
    with pytest.raises(ValueError, match="line break"):
        check_env({"LAKEHOUSE_LAKE_ROOT": "a\nb"})
    # a secret-like key raises before anything is written
    run = Recorder()
    backend = SystemdBackend(command=COMMAND, run=run, tool=str, home=tmp_path)
    with pytest.raises(ValueError, match="secret"):
        backend.install("0 6 * * *", system=False, env={**ENV, "GITHUB_TOKEN": "x"})
    assert run.calls == [] and list(tmp_path.rglob("*")) == []


# --- renders ---------------------------------------------------------------------------------------------------------


def _task(xml: str) -> Any:
    return ElementTree.fromstring(xml.encode("utf-16"))


def test_render_windows_task() -> None:
    xml = render_task_xml("30 6 * * *", command=COMMAND, env=ENV, system=False, run_as=None, user="LAPTOP\\karsten")
    task = _task(xml)
    action = task.find("t:Actions/t:Exec", NS)
    assert action is not None
    assert action.findtext("t:Command", namespaces=NS) == COMMAND[0]
    arguments = action.findtext("t:Arguments", namespaces=NS) or ""
    assert "lakehouse.schedule.windows" in arguments and '"--env" "LAKEHOUSE_LAKE_ROOT=D:/My Lake"' in arguments
    assert arguments.endswith('"--" "run" "--trigger" "schedule"')
    assert "cron: 30 6 * * *" in (task.findtext("t:RegistrationInfo/t:Description", namespaces=NS) or "")
    [trigger] = task.findall("t:Triggers/t:CalendarTrigger", NS)
    assert (trigger.findtext("t:StartBoundary", namespaces=NS) or "").endswith("T06:30:00")
    assert trigger.find("t:ScheduleByDay", NS) is not None
    weekly = _task(render_task_xml("0 18 * * 1,5", command=COMMAND, env={}, system=False, run_as=None, user="u"))
    days = weekly.find("t:Triggers/t:CalendarTrigger/t:ScheduleByWeek/t:DaysOfWeek", NS)
    assert days is not None and [d.tag.split("}")[1] for d in days] == ["Monday", "Friday"]
    every = _task(render_task_xml("*/15 * * * *", command=COMMAND, env={}, system=False, run_as=None, user="u"))
    assert every.findtext("t:Triggers/t:CalendarTrigger/t:Repetition/t:Interval", namespaces=NS) == "PT15M"
    with pytest.raises(ValueError, match="Task Scheduler"):
        render_task_xml("7,8,9 * * * *", command=COMMAND, env={}, system=False, run_as=None, user="u")


def test_render_plist() -> None:
    plist = plistlib.loads(
        render_plist("30 6 * * 1", label=TASK_NAME, command=COMMAND, env=ENV, system=False, run_as=None)
    )
    assert plist["Label"] == TASK_NAME and plist["ProgramArguments"] == COMMAND
    assert plist["EnvironmentVariables"] == ENV
    assert plist["StartCalendarInterval"] == [{"Minute": 30, "Hour": 6, "Weekday": 1}]
    assert "UserName" not in plist
    both = plistlib.loads(
        render_plist("0 0 1 * 3", label=TASK_NAME, command=COMMAND, env={}, system=False, run_as=None)
    )
    assert both["StartCalendarInterval"] == [{"Minute": 0, "Hour": 0, "Day": 1}, {"Minute": 0, "Hour": 0, "Weekday": 3}]


def test_render_systemd() -> None:
    service, timer = render_units("30 6 * * 1,5", name=TASK_NAME, command=COMMAND, env=ENV, system=False, run_as=None)
    assert "ExecStart='C:/Program Files/Python/python.exe' -m lakehouse.cli run --trigger schedule" in service
    assert 'Environment="LAKEHOUSE_LAKE_ROOT=D:/My Lake"' in service and "Type=oneshot" in service
    assert "OnCalendar=Mon,Fri *-*-* 06:30:00" in timer and "cron: 30 6 * * 1,5" in timer
    assert "User=" not in service
    _, both = render_units("0 0 1 * 3", name=TASK_NAME, command=COMMAND, env={}, system=False, run_as=None)
    assert "OnCalendar=*-*-01 00:00:00" in both and "OnCalendar=Wed *-*-* 00:00:00" in both


def test_stop_grace_is_explicit() -> None:
    """A stopped run publishes its pending work at the run end (T3 spec D1). launchd's own ExitTimeOut is
    "system-defined" (launchd.plist(5)), so the rendered plist and systemd unit name the grace explicitly: 60 s
    between SIGTERM and SIGKILL (owner decision, 2026-10-31)."""
    assert STOP_GRACE_SECONDS == 60
    for system in (False, True):
        run_as = "karsten" if system else None
        plist = plistlib.loads(
            render_plist("30 6 * * 1", label=TASK_NAME, command=COMMAND, env={}, system=system, run_as=run_as)
        )
        assert plist["ExitTimeOut"] == STOP_GRACE_SECONDS
        service, _ = render_units("30 6 * * 1", name=TASK_NAME, command=COMMAND, env={}, system=system, run_as=run_as)
        assert f"TimeoutStopSec={STOP_GRACE_SECONDS}" in service.splitlines()


def test_plist_raises_the_soft_open_file_limit_only() -> None:
    """launchd gives a task a soft open-file limit of 256 (T12 spec D2). The plist raises the soft limit to the target
    and leaves launchd's unlimited hard limit alone (Reviewer B T12-SPEC-04)."""
    for system in (False, True):
        run_as = "karsten" if system else None
        plist = plistlib.loads(
            render_plist("30 6 * * 1", label=TASK_NAME, command=COMMAND, env={}, system=system, run_as=run_as)
        )
        assert plist["SoftResourceLimits"] == {"NumberOfFiles": OPEN_FILES_TARGET}
        assert "HardResourceLimits" not in plist


def test_unit_sets_limit_nofile() -> None:
    """The systemd unit names the open-file limit: soft 65,536, hard 524,288 (systemd's own default hard limit)."""
    for system in (False, True):
        run_as = "karsten" if system else None
        service, _ = render_units("30 6 * * 1", name=TASK_NAME, command=COMMAND, env={}, system=system, run_as=run_as)
        assert f"LimitNOFILE={OPEN_FILES_TARGET}:524288" in service.splitlines()


def test_render_cron() -> None:
    line = render_cron_line("30 6 * * *", name=TASK_NAME, command=COMMAND, env=ENV)
    assert line.startswith("30 6 * * * env HF_HOME=/Volumes/Development/hf 'LAKEHOUSE_LAKE_ROOT=D:/My Lake' ")
    assert "lakehouse.cli run --trigger schedule" in line and line.endswith(f"# {TASK_NAME}")
    system = render_cron_file("30 6 * * *", name=TASK_NAME, command=COMMAND, env={}, run_as="karsten")
    assert "\n30 6 * * * karsten " in system


def test_render_system_runs_as_user(tmp_path: Path) -> None:
    """§18, §20: a system-level task runs as the operator, never as root, with no stored password."""
    task = _task(render_task_xml("0 6 * * *", command=COMMAND, env={}, system=True, run_as="MINI\\ops", user="admin"))
    assert task.findtext("t:Principals/t:Principal/t:UserId", namespaces=NS) == "MINI\\ops"
    assert task.findtext("t:Principals/t:Principal/t:LogonType", namespaces=NS) == "S4U"
    assert task.findtext("t:Principals/t:Principal/t:RunLevel", namespaces=NS) == "LeastPrivilege"
    plist = plistlib.loads(
        render_plist("0 6 * * *", label=TASK_NAME, command=COMMAND, env={}, system=True, run_as="ops:staff")
    )
    assert (plist["UserName"], plist["GroupName"]) == ("ops", "staff")
    service, _ = render_units("0 6 * * *", name=TASK_NAME, command=COMMAND, env={}, system=True, run_as="ops")
    assert "User=ops" in service and "Group=ops" in service
    for backend in (
        WindowsBackend(command=COMMAND, run=Recorder(), tool=str),
        LaunchdBackend(command=COMMAND, run=Recorder(), tool=str, home=tmp_path, system_dir=tmp_path / "daemons"),
        SystemdBackend(command=COMMAND, run=Recorder(), tool=str, home=tmp_path, system_dir=tmp_path / "system"),
        CronBackend(command=COMMAND, run=Recorder(), tool=str, system_dir=tmp_path / "cron.d"),
    ):
        with pytest.raises(ValueError, match="run_as"):
            backend.install("0 6 * * *", system=True, env={})
        for name in ("root", "SYSTEM", "NT AUTHORITY\\SYSTEM"):
            with pytest.raises(ValueError, match="root"):
                backend.install("0 6 * * *", system=True, env={}, run_as=name)


def test_system_install_never_opens_the_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`install(system=True)` only writes and loads the task definition: any storage call fails the test."""
    import lakehouse.storage.local as local
    import lakehouse.storage.roots as roots

    def refuse(*_: object, **__: object) -> None:
        raise AssertionError("install touched the lake")

    monkeypatch.setattr(local.LocalStorage, "__init__", refuse)
    monkeypatch.setattr(roots, "resolve_roots", refuse)
    LaunchdBackend(command=COMMAND, run=Recorder(), tool=str, home=tmp_path, system_dir=tmp_path / "d").install(
        "0 6 * * *", system=True, env=ENV, run_as="ops"
    )
    SystemdBackend(command=COMMAND, run=Recorder(), tool=str, home=tmp_path, system_dir=tmp_path / "s").install(
        "0 6 * * *", system=True, env=ENV, run_as="ops"
    )
    WindowsBackend(command=COMMAND, run=Recorder(), tool=str).install(
        "0 6 * * *", system=True, env=ENV, run_as="MINI\\ops"
    )


def test_missed_runs_coalesce() -> None:
    """§17.5: a missed run is not replayed, and at most one run starts on wake."""
    task = _task(render_task_xml("0 6 * * *", command=COMMAND, env={}, system=False, run_as=None, user="u"))
    assert task.findtext("t:Settings/t:StartWhenAvailable", namespaces=NS) == "true"
    assert task.findtext("t:Settings/t:MultipleInstancesPolicy", namespaces=NS) == "IgnoreNew"
    _, timer = render_units("0 6 * * *", name=TASK_NAME, command=COMMAND, env={}, system=False, run_as=None)
    assert "Persistent=true" in timer
    plist = plistlib.loads(
        render_plist("0 6 * * *", label=TASK_NAME, command=COMMAND, env={}, system=False, run_as=None)
    )
    assert "StartCalendarInterval" in plist and "StartInterval" not in plist  # launchd coalesces missed calendar runs


# --- installs with a recorded subprocess -----------------------------------------------------------------------------


def test_install_idempotent(tmp_path: Path) -> None:
    """Installing twice gives one task definition; status reads it back; uninstall removes it."""
    run = Recorder()
    systemd = SystemdBackend(command=COMMAND, run=run, tool=str, home=tmp_path)
    systemd.install("0 6 * * *", system=False, env=ENV)
    systemd.install("0 7 * * *", system=False, env=ENV)
    units = tmp_path / ".config" / "systemd" / "user"
    assert sorted(p.name for p in units.iterdir()) == [f"{TASK_NAME}.service", f"{TASK_NAME}.timer"]
    assert "OnCalendar=*-*-* 07:00:00" in (units / f"{TASK_NAME}.timer").read_text(encoding="utf-8")
    assert ["systemctl", "--user", "enable", "--now", f"{TASK_NAME}.timer"] in run.calls
    status = systemd.status()
    assert (status.backend, status.installed, status.cron) == ("systemd", True, "0 7 * * *")
    systemd.uninstall()
    assert not (units / f"{TASK_NAME}.timer").exists()

    launchd_run = Recorder({("launchctl", "bootout"): (5, "")})  # nothing loaded yet: bootout fails, install goes on
    launchd = LaunchdBackend(command=COMMAND, run=launchd_run, tool=str, home=tmp_path, uid=501)
    launchd.install("0 6 * * *", system=False, env=ENV)
    launchd.install("0 6 * * *", system=False, env=ENV)
    agent = tmp_path / "Library" / "LaunchAgents" / f"{TASK_NAME}.plist"
    assert agent.is_file() and launchd.status().cron == "0 6 * * *"
    assert launchd_run.calls.count(["launchctl", "bootstrap", "gui/501", str(agent)]) == 2

    win_run = Recorder()
    windows = WindowsBackend(command=COMMAND, run=win_run, tool=str)
    windows.install("0 6 * * *", system=False, env=ENV)
    windows.install("0 6 * * *", system=False, env=ENV)
    creates = [c for c in win_run.calls if c[:2] == ["schtasks", "/Create"]]
    assert len(creates) == 2 and all("/F" in c and c[c.index("/TN") + 1] == TASK_NAME for c in creates)

    cron_run = Recorder({("crontab", "-l"): (0, "5 4 * * * other-job\n")})
    crontab = CronBackend(command=COMMAND, run=cron_run, tool=str)
    crontab.install("0 6 * * *", system=False, env=ENV)
    written = [text for text in cron_run.inputs if text]
    assert written and written[-1].count(f"# {TASK_NAME}") == 1 and "5 4 * * * other-job" in written[-1]


def test_backend_for_host() -> None:
    assert type(backend_for_host(platform="win32")).__name__ == "WindowsBackend"
    assert type(backend_for_host(platform="darwin")).__name__ == "LaunchdBackend"
    assert type(backend_for_host(platform="linux", has_systemd=True)).__name__ == "SystemdBackend"
    assert type(backend_for_host(platform="linux", has_systemd=False)).__name__ == "CronBackend"


# --- HF --------------------------------------------------------------------------------------------------------------


class FakeHfApi:
    """Records every call; refuses any call that creates a job or carries secrets."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        def call(*args: object, **kwargs: Any) -> Any:
            assert not args, "keyword arguments only"
            assert "secrets" not in kwargs, f"{name} was given secrets"
            assert not name.startswith(("create_", "run_")), f"{name} creates a job"
            self.calls.append((name, kwargs))
            if name == "inspect_scheduled_job":
                return _scheduled(suspend=False)
            if name == "trigger_scheduled_job":
                return type("JobInfo", (), {"id": "job-42"})()
            if name == "inspect_job":
                return type("JobInfo", (), {"status": type("S", (), {"stage": "COMPLETED"})()})()
            return None

        return call


def _scheduled(*, suspend: bool) -> Any:
    last = type("Last", (), {"id": "job-41", "at": datetime(2026, 10, 27, 6, tzinfo=UTC)})()
    status = type("Status", (), {"last_job": last, "next_job_run_at": datetime(2026, 10, 28, 6, tzinfo=UTC)})()
    return type("ScheduledJobInfo", (), {"schedule": "0 6 * * *", "suspend": suspend, "status": status})()


def test_hf_schedule_never_sends_secrets() -> None:
    api = FakeHfApi()
    schedule = HfJobsSchedule(api, "sched-1")  # type: ignore[arg-type]
    schedule.reschedule("0 7 * * *")
    schedule.suspend()
    schedule.resume()
    assert schedule.trigger() == "job-42"
    status = schedule.status()
    assert (status.backend, status.installed, status.enabled, status.cron) == ("hf_jobs", True, True, "0 6 * * *")
    assert status.next_run == datetime(2026, 10, 28, 6, tzinfo=UTC) and status.last_result == "COMPLETED"
    names = [name for name, _ in api.calls]
    assert names[:4] == [
        "update_scheduled_job_schedule",
        "suspend_scheduled_job",
        "resume_scheduled_job",
        "trigger_scheduled_job",
    ]
    assert all(kwargs.get("scheduled_job_id", "sched-1") == "sched-1" for _, kwargs in api.calls)
    with pytest.raises(ValueError, match="cron"):
        schedule.reschedule("not a cron")


def test_from_env_prefers_the_jobs_token_then_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """`from_env` builds the Jobs-API client from `LAKEHOUSE_HF_JOBS_TOKEN` (the confined job-control token), and
    falls back to `HF_TOKEN` only when the jobs token is unset (the single-token org / lake-only case). The job id
    comes from `LAKEHOUSE_HF_SCHEDULED_JOB_ID`."""
    import lakehouse.schedule.hf_jobs as hf_jobs

    seen: list[str | None] = []

    def fake_api(token: str | None = None) -> FakeHfApi:
        seen.append(token)
        return FakeHfApi()

    monkeypatch.setattr(hf_jobs, "HfApi", fake_api)

    full = {"LAKEHOUSE_HF_JOBS_TOKEN": "jobs", "HF_TOKEN": "lake", "LAKEHOUSE_HF_SCHEDULED_JOB_ID": "s"}
    hf_jobs.HfJobsSchedule.from_env(full)
    assert seen[-1] == "jobs"  # the jobs token wins over the lake token
    hf_jobs.HfJobsSchedule.from_env({"HF_TOKEN": "lake", "LAKEHOUSE_HF_SCHEDULED_JOB_ID": "s"})
    assert seen[-1] == "lake"  # falls back to HF_TOKEN when the jobs token is unset
    with pytest.raises(ValueError, match="no scheduled job id"):
        hf_jobs.HfJobsSchedule.from_env({"LAKEHOUSE_HF_JOBS_TOKEN": "jobs"})  # a missing job id is a loud failure


def test_tools_are_absolute_system_paths() -> None:
    """A planted `schtasks` or `crontab` on PATH is never run: each tool resolves to its fixed system path."""
    from lakehouse.schedule.base import _TOOLS, resolve_tool  # pyright: ignore[reportPrivateUsage]

    resolved = 0
    for name in _TOOLS:
        try:
            path = resolve_tool(name)
        except FileNotFoundError:
            continue  # a tool of another OS
        assert Path(path).is_absolute() and Path(path).is_file()
        resolved += 1
    assert resolved >= 1  # every CI host has its own scheduler tool


def test_cron_escapes_percent() -> None:
    line = render_cron_line("0 6 * * *", name=TASK_NAME, command=[*COMMAND, "--note", "100%"], env={})
    assert r"100\%" in line  # cron reads an unescaped % as a line break


def test_hf_trigger_refusal_is_an_http_status_error() -> None:
    """A trigger that HF refuses (for example under `--no-concurrency` while a run is active) reaches the services as
    `HttpStatusError`, so the Ops page can show it (the services never import huggingface_hub)."""
    import httpx2
    from huggingface_hub.errors import HfHubHTTPError

    from lakehouse.errors import HttpStatusError

    class Busy(FakeHfApi):
        def trigger_scheduled_job(self, **kwargs: Any) -> Any:
            request = httpx2.Request("POST", "https://huggingface.co/api/scheduled-jobs/sched-1/trigger")
            raise HfHubHTTPError("conflict", response=httpx2.Response(409, request=request))

    with pytest.raises(HttpStatusError) as raised:
        HfJobsSchedule(Busy(), "sched-1").trigger()  # type: ignore[arg-type]
    assert raised.value.status == 409


def test_hf_status_refusal_is_an_http_status_error() -> None:
    """A refused status read (for example the job-control token lacks job access, HTTP 403) reaches the services as
    `HttpStatusError`, so `settings.refresh` can show the schedule as unavailable and still render the config section.
    Seen to fail: leave `status()` unwrapped and the raw `HfHubHTTPError` leaks into the UI (and blanks the page)."""
    import httpx2
    from huggingface_hub.errors import HfHubHTTPError

    from lakehouse.errors import HttpStatusError

    class Forbidden(FakeHfApi):
        def inspect_scheduled_job(self, **kwargs: Any) -> Any:
            request = httpx2.Request("GET", "https://huggingface.co/api/scheduled-jobs/sched-1")
            raise HfHubHTTPError("forbidden", response=httpx2.Response(403, request=request))

    with pytest.raises(HttpStatusError) as raised:
        HfJobsSchedule(Forbidden(), "sched-1").status()  # type: ignore[arg-type]
    assert raised.value.status == 403
