"""The process's own open-file limit (T12 spec D1).

One DuckDB scan of a DuckLake table holds a descriptor for many of the table's files at once (a 10,000-file table:
834 descriptors). Under macOS's default soft limit of 256, which a launchd task gets, a run failed with "Too many open
files"; `uv run` hid it by raising the limit for its child. So every v2 entry point raises its own soft
`RLIMIT_NOFILE` first:

- to `OPEN_FILES_TARGET`, capped by the hard limit and, on macOS, by `kern.maxfilesperproc`, which macOS enforces;
- on a refusal it halves the value, down to the current soft limit;
- it never lowers a limit and never raises an exception.

A command that opens a catalog then calls `require_open_file_minimum`, which stops it with a `ConfigError` below
`OPEN_FILES_MINIMUM`. Windows has no POSIX descriptor limit (DuckDB opens Win32 handles), so both are no-ops there.
Standard library only.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import logging
import sys
from dataclasses import dataclass
from typing import Final

from lakehouse.errors import ConfigError

__all__ = [
    "OPEN_FILES_MINIMUM",
    "OPEN_FILES_TARGET",
    "OpenFileLimit",
    "raise_open_file_limit",
    "require_open_file_minimum",
]

OPEN_FILES_TARGET: Final = 65_536
OPEN_FILES_MINIMUM: Final = 4_096

# the standard logger in the `lakehouse` namespace, not `lakehouse.logging`: that module imports `lakehouse.mask`,
# which calls this module first (an import cycle otherwise)
_log = logging.getLogger("lakehouse.limits")


@dataclass(frozen=True)
class OpenFileLimit:
    soft_before: int
    soft_after: int
    hard: int


def _platform() -> str:
    return sys.platform


def _infinity() -> int:
    if sys.platform == "win32":  # the type checker then never sees `resource` off-POSIX
        raise OSError("Windows has no RLIMIT_NOFILE")
    import resource  # POSIX only; never imported on Windows

    return resource.RLIM_INFINITY


def _current_limits() -> tuple[int, int]:
    if sys.platform == "win32":  # the type checker then never sees `resource` off-POSIX
        raise OSError("Windows has no RLIMIT_NOFILE")
    import resource

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    return soft, hard


def _set_soft(soft: int, hard: int) -> None:
    if sys.platform == "win32":  # the type checker then never sees `resource` off-POSIX
        raise OSError("Windows has no RLIMIT_NOFILE")
    import resource

    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def _max_files_per_process() -> int | None:
    """macOS `kern.maxfilesperproc` through `sysctlbyname`, or `None` when it cannot be read."""
    path = ctypes.util.find_library("c")
    if path is None:
        return None
    try:
        libc = ctypes.CDLL(path, use_errno=True)
        value = ctypes.c_int(0)
        size = ctypes.c_size_t(ctypes.sizeof(value))
        if libc.sysctlbyname(b"kern.maxfilesperproc", ctypes.byref(value), ctypes.byref(size), None, 0) != 0:
            return None
    except (OSError, AttributeError):  # no libc symbol: the halving fallback still finds an accepted value
        return None
    return value.value if value.value > 0 else None


def raise_open_file_limit() -> OpenFileLimit | None:
    """Raise this process's soft open-file limit (see the module docstring); `None` on Windows."""
    if _platform() == "win32":
        return None
    soft, hard = _current_limits()
    if soft >= OPEN_FILES_TARGET:
        limit = OpenFileLimit(soft, soft, hard)
    else:
        target = OPEN_FILES_TARGET if hard == _infinity() else min(OPEN_FILES_TARGET, hard)
        if _platform() == "darwin":
            per_process = _max_files_per_process()
            if per_process is not None:
                target = min(target, per_process)
        accepted = soft
        value = target
        while value > soft:
            try:
                _set_soft(value, hard)
            except (ValueError, OSError):
                value //= 2
                continue
            accepted = value
            break
        limit = OpenFileLimit(soft, accepted, hard)
    _log.info(
        "open-file limit",
        extra={"soft_before": limit.soft_before, "soft_after": limit.soft_after, "hard": limit.hard},
    )
    return limit


def require_open_file_minimum(limit: OpenFileLimit | None) -> None:
    """Stop a catalog command whose soft open-file limit is below `OPEN_FILES_MINIMUM` (no-op on Windows)."""
    if limit is None or limit.soft_after >= OPEN_FILES_MINIMUM:
        return
    raise ConfigError(
        f"the open-file limit is {limit.soft_after}, below the {OPEN_FILES_MINIMUM} a lake needs, and the hard limit "
        f"({limit.hard}) does not allow more: raise it with `ulimit -n {OPEN_FILES_TARGET}` in the shell, "
        "`NumberOfFiles` in the launchd plist or `LimitNOFILE` in the systemd unit (`lakehouse schedule install` sets "
        "them)"
    )
