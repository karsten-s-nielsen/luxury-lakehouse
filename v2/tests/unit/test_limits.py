"""The process raises its own open-file limit (T12 spec D1).

One DuckDB scan holds a descriptor for many of a table's files at once; under macOS's default soft limit of 256 (what
a launchd task gets) a run failed with "Too many open files". The raise goes to `OPEN_FILES_TARGET`, capped by the hard
limit and macOS's `kern.maxfilesperproc`, halves on a refusal, never lowers a limit and never raises an exception; the
minimum check stops a catalog command below `OPEN_FILES_MINIMUM`.
"""

from __future__ import annotations

import pytest

from lakehouse import limits
from lakehouse.errors import ConfigError
from lakehouse.limits import OPEN_FILES_MINIMUM, OPEN_FILES_TARGET, OpenFileLimit, raise_open_file_limit

INFINITY = 2**63 - 1


class _Kernel:
    """A stub of the three kernel seams: the current limits, `setrlimit` (with refused values) and the per-process
    maximum."""

    def __init__(self, soft: int, hard: int, *, refuse: tuple[int, ...] = (), per_process: int | None = None) -> None:
        self.soft, self.hard, self.refuse, self.per_process = soft, hard, refuse, per_process
        self.calls: list[int] = []

    def install(self, monkeypatch: pytest.MonkeyPatch, platform: str) -> None:
        monkeypatch.setattr(limits, "_platform", lambda: platform)
        monkeypatch.setattr(limits, "_infinity", lambda: INFINITY)
        monkeypatch.setattr(limits, "_current_limits", lambda: (self.soft, self.hard))
        monkeypatch.setattr(limits, "_set_soft", self._set)
        monkeypatch.setattr(limits, "_max_files_per_process", lambda: self.per_process)

    def _set(self, soft: int, hard: int) -> None:
        self.calls.append(soft)
        if soft in self.refuse:
            raise ValueError("not allowed")
        assert hard == self.hard, "the hard limit is kept"
        self.soft = soft


def test_soft_below_target_is_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = _Kernel(256, INFINITY)
    kernel.install(monkeypatch, "linux")
    assert raise_open_file_limit() == OpenFileLimit(256, OPEN_FILES_TARGET, INFINITY)


def test_limit_capped_by_finite_hard(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = _Kernel(1024, 20_000)
    kernel.install(monkeypatch, "linux")
    assert raise_open_file_limit() == OpenFileLimit(1024, 20_000, 20_000)


def test_limit_capped_by_maxfilesperproc(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = _Kernel(256, INFINITY, per_process=24_576)
    kernel.install(monkeypatch, "darwin")
    assert raise_open_file_limit() == OpenFileLimit(256, 24_576, INFINITY)


def test_soft_at_or_above_target_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = _Kernel(1_048_576, INFINITY)  # what `uv run` gives its child
    kernel.install(monkeypatch, "darwin")
    assert raise_open_file_limit() == OpenFileLimit(1_048_576, 1_048_576, INFINITY)
    assert kernel.calls == [], "a higher limit is never lowered"


def test_windows_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(limits, "_platform", lambda: "win32")
    assert raise_open_file_limit() is None


def test_fallback_halves_until_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = _Kernel(256, INFINITY, refuse=(65_536, 32_768), per_process=None)
    kernel.install(monkeypatch, "darwin")
    assert raise_open_file_limit() == OpenFileLimit(256, 16_384, INFINITY)
    assert kernel.calls == [65_536, 32_768, 16_384]


def test_every_value_refused_keeps_the_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    refused = tuple(65_536 >> n for n in range(10))
    kernel = _Kernel(256, INFINITY, refuse=refused)
    kernel.install(monkeypatch, "linux")
    assert raise_open_file_limit() == OpenFileLimit(256, 256, INFINITY)


def test_minimum_check_names_limit_and_fix() -> None:
    with pytest.raises(ConfigError) as raised:
        limits.require_open_file_minimum(OpenFileLimit(256, 1024, 1024))
    text = str(raised.value)
    assert "1024" in text and str(OPEN_FILES_MINIMUM) in text
    assert "ulimit -n" in text and "NumberOfFiles" in text and "LimitNOFILE" in text


def test_minimum_check_passes_at_the_minimum_and_on_windows() -> None:
    limits.require_open_file_minimum(OpenFileLimit(256, OPEN_FILES_MINIMUM, INFINITY))
    limits.require_open_file_minimum(None)
