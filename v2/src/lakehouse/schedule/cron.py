"""cron on Linux without systemd (spec §17.5, §18; plan Task 24).

User mode keeps one line, marked `# <name>`, in the user's crontab (`crontab -l` / `crontab -`); the other lines stay.
System mode writes `/etc/cron.d/<name>` with the operator as the user field, never root (§20). cron does not replay a
run it missed while the machine was off (§17.5). The command runs through `env` with the non-secret variables.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path

from lakehouse.schedule.base import (
    TASK_NAME,
    ScheduleStatus,
    check_env,
    check_run_as,
    command_line,
    default_command,
    next_run,
    parse_cron,
    resolve_tool,
)

__all__ = ["CronBackend", "render_cron_file", "render_cron_line"]

Runner = Callable[..., subprocess.CompletedProcess[str]]


def _job(command: Sequence[str], env: Mapping[str, str]) -> str:
    checked = check_env(env)
    pairs = [f"{key}={value}" for key, value in checked.items()]
    text = command_line(["env", *pairs, *command]) if pairs else command_line(command)
    return text.replace("%", "\\%")  # cron reads an unescaped % as a line break


def render_cron_line(cron: str, *, name: str, command: Sequence[str], env: Mapping[str, str]) -> str:
    spec = parse_cron(cron)
    return f"{spec.text} {_job(command, env)} # {name}"


def render_cron_file(
    cron: str, *, name: str, command: Sequence[str], env: Mapping[str, str], run_as: str | None
) -> str:
    spec = parse_cron(cron)
    user = (check_run_as(run_as, system=True) or "").partition(":")[0]
    return (
        f"# {name}: lakehouse v2 ingestion, managed by `lakehouse schedule install`\n"
        "SHELL=/bin/sh\n"
        f"{spec.text} {user} {_job(command, env)} # {name}\n"
    )


def _cron_of(line: str) -> str:
    fields = line.lstrip("#").split()
    return fields[0] if fields and fields[0].startswith("@") else " ".join(fields[:5])


class CronBackend:
    """The user's crontab, or a file in /etc/cron.d (never a shell)."""

    def __init__(
        self,
        *,
        name: str = TASK_NAME,
        command: Sequence[str] | None = None,
        run: Runner = subprocess.run,
        tool: Callable[[str], str] = resolve_tool,
        system_dir: Path = Path("/etc/cron.d"),
    ) -> None:
        self._name = name
        self._command = list(command) if command is not None else default_command()
        self._run = run
        self._tool = tool
        self._system_dir = system_dir

    def _crontab(self) -> list[str]:
        listed = self._run([self._tool("crontab"), "-l"], shell=False, check=False, capture_output=True, text=True)
        return listed.stdout.splitlines() if listed.returncode == 0 else []

    def _write(self, lines: list[str]) -> None:
        text = "\n".join(lines) + "\n" if lines else ""
        self._run([self._tool("crontab"), "-"], shell=False, check=True, input=text, capture_output=True, text=True)

    def _others(self) -> list[str]:
        return [line for line in self._crontab() if not line.rstrip().endswith(f"# {self._name}")]

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        if system:
            text = render_cron_file(cron, name=self._name, command=self._command, env=env, run_as=run_as)
            self._system_dir.mkdir(parents=True, exist_ok=True)
            (self._system_dir / self._name).write_text(text, encoding="utf-8")
            return
        line = render_cron_line(cron, name=self._name, command=self._command, env=env)
        self._write([*self._others(), line])

    def uninstall(self) -> None:
        (self._system_dir / self._name).unlink(missing_ok=True)
        if any(line.rstrip().endswith(f"# {self._name}") for line in self._crontab()):
            self._write(self._others())

    def status(self) -> ScheduleStatus:
        system_file = self._system_dir / self._name
        lines = system_file.read_text(encoding="utf-8").splitlines() if system_file.exists() else self._crontab()
        mine = [line for line in lines if line.rstrip().endswith(f"# {self._name}") and not line.startswith("# ")]
        if not mine:
            return ScheduleStatus("cron", False, False, None, None, None)
        line = mine[0]
        enabled = not line.lstrip().startswith("#")
        cron = _cron_of(line)
        upcoming = next_run(parse_cron(cron), datetime.now().astimezone()) if enabled else None
        return ScheduleStatus("cron", True, enabled, cron, upcoming, None)
