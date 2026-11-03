"""Shared CLI test helpers (a helper module, not a test). `Cli` runs `lakehouse <argv>` with injected deps and
returns the exit code plus the JSON lines; `FakeBackend` records scheduler calls; `Recorder` records any call. They
live in `tests/support` (on `pythonpath`) so every test imports them as `from clikit import …`, the same form as
`from lakekit import …` — an absolute `from tests.unit.test_cli import …` does not import under `uv run pytest`,
which does not put the working directory on `sys.path`.
"""

from __future__ import annotations

import dataclasses
import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from lakehouse import cli
from lakehouse.schedule.base import ScheduleStatus


class Cli:
    """`lakehouse <argv>` with the given dependencies; each call returns the exit code and the JSON lines written."""

    def __init__(self, env: Mapping[str, str], stdin: str = "", **overrides: Any) -> None:
        self.out = io.StringIO()
        self.env = dict(env)
        self.deps = dataclasses.replace(cli.Deps(env=self.env, stdout=self.out, stdin=io.StringIO(stdin)), **overrides)

    def __call__(self, *argv: str) -> tuple[int, list[dict[str, Any]]]:
        start = len(self.out.getvalue())
        code = cli.main(list(argv), deps=self.deps)
        return code, [json.loads(line) for line in self.out.getvalue()[start:].splitlines()]


class Recorder:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append({"args": args, **kwargs})
        return self.result


class FakeBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        self.calls.append(("install", (cron, system, dict(env), run_as)))

    def uninstall(self) -> None:
        self.calls.append(("uninstall", None))

    def status(self) -> ScheduleStatus:
        return ScheduleStatus("systemd", True, True, "0 6 * * *", datetime(2026, 10, 28, 6, tzinfo=UTC), None)
