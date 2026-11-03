"""The run's own memory peaks (§26 item 9; audit T11).

`peak_rss_mb` is `ru_maxrss`, the peak resident size: KiB on Linux, bytes on macOS. On macOS the resident size also
counts freed pages the kernel has not reclaimed yet, so it can far exceed what the run used (the T3 Task 14 run:
15.7 GB resident, 8.2 GB phys_footprint). `peak_footprint_mb` is the lifetime peak phys_footprint, the measure macOS
itself uses for memory pressure; it exists on macOS only.
"""

from __future__ import annotations

import ctypes
import os
import sys
from typing import Final

__all__ = ["peak_footprint_mb", "peak_rss_mb"]

_MIB: Final = 1024 * 1024
_RUSAGE_INFO_V4: Final = 4
_LIFETIME_MAX_PHYS_FOOTPRINT: Final = 28  # the index after ri_uuid in rusage_info_v4 (sys/resource.h)


class _RusageInfoV4(ctypes.Structure):
    _fields_ = (("uuid", ctypes.c_uint8 * 16), ("values", ctypes.c_uint64 * 40))  # v4 holds 35 values; room to spare


def peak_rss_mb() -> float | None:
    """Peak resident set size of this process in MiB, or `None` on Windows (no stdlib `resource` module). The
    `win32` guard is first so the type checker never sees `resource` off-Unix."""
    if sys.platform == "win32":
        return None
    import resource

    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(maxrss / (_MIB if sys.platform == "darwin" else 1024), 1)


def peak_footprint_mb() -> float | None:
    """The lifetime peak phys_footprint of this process in MiB on macOS (`proc_pid_rusage`), else `None`."""
    if sys.platform != "darwin":
        return None
    info = _RusageInfoV4()
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    if libproc.proc_pid_rusage(os.getpid(), _RUSAGE_INFO_V4, ctypes.byref(info)) != 0:
        raise OSError(ctypes.get_errno(), "proc_pid_rusage failed")
    return round(info.values[_LIFETIME_MAX_PHYS_FOOTPRINT] / _MIB, 1)
