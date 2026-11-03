"""launchd on macOS (spec §17.5, §18; plan Task 24).

User mode writes a LaunchAgent (`~/Library/LaunchAgents/<label>.plist`, domain `gui/<uid>`); system mode writes a
LaunchDaemon (`/Library/LaunchDaemons/<label>.plist`, domain `system`) that runs as the operator through `UserName`
and `GroupName`, never as root (§20). The schedule is `StartCalendarInterval`: launchd coalesces the runs it missed
while asleep into one run on wake (§17.5). The cron text is kept next to the plist (`<label>.cron`), which launchd
does not read.
"""

from __future__ import annotations

import itertools
import os
import plistlib
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from lakehouse.limits import OPEN_FILES_TARGET
from lakehouse.schedule.base import (
    STOP_GRACE_SECONDS,
    TASK_NAME,
    CronSpec,
    ScheduleStatus,
    check_env,
    check_run_as,
    default_command,
    next_run,
    parse_cron,
    resolve_tool,
)

__all__ = ["LaunchdBackend", "render_plist"]

_MAX_INTERVALS: Final = 500
_LAST_EXIT: Final = re.compile(r"last exit code = (\S+)")
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _intervals(spec: CronSpec) -> list[dict[str, int]]:
    fields: list[tuple[str, tuple[int, ...], bool]] = [
        ("Minute", spec.minutes, spec.minutes == tuple(range(60))),
        ("Hour", spec.hours, spec.hours == tuple(range(24))),
        ("Month", spec.months, spec.months == tuple(range(1, 13))),
    ]
    day_fields: list[list[tuple[str, tuple[int, ...]]]]
    if spec.dom_any and spec.dow_any:
        day_fields = [[]]
    elif spec.dom_any:
        day_fields = [[("Weekday", spec.weekdays)]]
    elif spec.dow_any:
        day_fields = [[("Day", spec.days)]]
    else:
        day_fields = [[("Day", spec.days)], [("Weekday", spec.weekdays)]]  # cron: either day field matches
    restricted = [(key, values) for key, values, every in fields if not every]
    intervals: list[dict[str, int]] = []
    for days in day_fields:
        chosen = [*restricted, *days]
        keys = [key for key, _ in chosen]
        for combo in itertools.product(*(values for _, values in chosen)):
            intervals.append(dict(zip(keys, combo, strict=True)))
    if len(intervals) > _MAX_INTERVALS:
        raise ValueError(f"the cron expression {spec.text!r} needs more than {_MAX_INTERVALS} launchd intervals")
    return intervals


def _user_group(run_as: str) -> tuple[str, str]:
    user, _, group = run_as.partition(":")
    return user, group or "staff"


def render_plist(
    cron: str,
    *,
    label: str,
    command: Sequence[str],
    env: Mapping[str, str],
    system: bool,
    run_as: str | None,
) -> bytes:
    spec = parse_cron(cron)
    checked = check_env(env)
    document: dict[str, Any] = {
        "Label": label,
        "ProgramArguments": list(command),
        "StartCalendarInterval": _intervals(spec),
        "ProcessType": "Background",
        "ExitTimeOut": STOP_GRACE_SECONDS,  # SIGTERM to SIGKILL: a stopped run publishes its pending work (T3 D1)
        # launchd's soft open-file limit is 256; the hard limit stays launchd's unlimited (T12 spec D2)
        "SoftResourceLimits": {"NumberOfFiles": OPEN_FILES_TARGET},
    }
    if checked:
        document["EnvironmentVariables"] = checked
    if system:
        user, group = _user_group(check_run_as(run_as, system=True) or "")
        document["UserName"], document["GroupName"] = user, group
    return plistlib.dumps(document, sort_keys=True)


class LaunchdBackend:
    """A LaunchAgent or LaunchDaemon through `launchctl bootstrap` and `bootout` (never a shell)."""

    def __init__(
        self,
        *,
        name: str = TASK_NAME,
        command: Sequence[str] | None = None,
        run: Runner = subprocess.run,
        tool: Callable[[str], str] = resolve_tool,
        home: Path | None = None,
        system_dir: Path = Path("/Library/LaunchDaemons"),
        uid: int | None = None,
    ) -> None:
        self._name = name
        self._command = list(command) if command is not None else default_command()
        self._run = run
        self._tool = tool
        self._home = home if home is not None else Path.home()
        self._system_dir = system_dir
        getuid: Callable[[], int] | None = getattr(os, "getuid", None)
        self._uid = uid if uid is not None else (getuid() if getuid is not None else 0)

    def _place(self, system: bool) -> tuple[Path, str]:
        if system:
            return self._system_dir / f"{self._name}.plist", "system"
        return self._home / "Library" / "LaunchAgents" / f"{self._name}.plist", f"gui/{self._uid}"

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        document = render_plist(cron, label=self._name, command=self._command, env=env, system=system, run_as=run_as)
        path, domain = self._place(system)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(document)
        path.chmod(0o644)  # launchd refuses a daemon plist that others can write
        path.with_suffix(".cron").write_text(parse_cron(cron).text + "\n", encoding="utf-8")
        self._run(
            [self._tool("launchctl"), "bootout", domain, str(path)],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
        )
        self._run(
            [self._tool("launchctl"), "bootstrap", domain, str(path)],
            shell=False,
            check=True,
            capture_output=True,
            text=True,
        )

    def uninstall(self) -> None:
        for system in (False, True):
            path, domain = self._place(system)
            if path.exists():
                self._run(
                    [self._tool("launchctl"), "bootout", domain, str(path)],
                    shell=False,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                path.unlink()
                path.with_suffix(".cron").unlink(missing_ok=True)

    def status(self) -> ScheduleStatus:
        for system in (False, True):
            path, domain = self._place(system)
            if not path.exists():
                continue
            sidecar = path.with_suffix(".cron")
            cron = sidecar.read_text(encoding="utf-8").strip() if sidecar.exists() else None
            shown = self._run(
                [self._tool("launchctl"), "print", f"{domain}/{self._name}"],
                shell=False,
                check=False,
                capture_output=True,
                text=True,
            )
            loaded = shown.returncode == 0
            found = _LAST_EXIT.search(shown.stdout) if loaded else None
            upcoming = next_run(parse_cron(cron), datetime.now().astimezone()) if cron and loaded else None
            return ScheduleStatus("launchd", True, loaded, cron, upcoming, found.group(1) if found else None)
        return ScheduleStatus("launchd", False, False, None, None, None)
