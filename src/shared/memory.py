"""Driver-memory probe — stdlib only, hexagonal (ADR-074).

The pure core (:func:`sample_memory`, :func:`format_memory`) is separated from the OS
adapters (:func:`peak_rss_bytes`, :func:`current_rss_bytes`) so the logic is testable
with injected fakes. Both adapters return ``None`` where unsupported (Windows), and
every consumer must tolerate that.

WHY THIS EXISTS: hf_sync's driver was OOM-killed (``exit code 137 (SIGKILL)``) on
2026-08-07 while running nine sub-operations in ONE process. The publisher that died
was afterwards measured at **6.97 GB alone in a ~16 GB driver** (diagnostic run
939215830803445), and the three sub-operations preceding it are Spark-native with no
``.toPandas()`` — so the consumer of the remaining memory is UNIDENTIFIED. Three
theories were advanced during diagnosis and all three were wrong. Rather than guess a
fourth time, every ``@workflow`` now reports memory via
``ingestion.memory_hook.MemoryHook``, and the next real run names the consumer.

READ THE TWO NUMBERS DIFFERENTLY — this is the single most misread thing here:

``peak``
    High-water mark; it NEVER falls. A delta means "this unit of work pushed the
    ceiling up by X". A light workflow shows ``+0.00 GB``, not a decrease.

``resident``
    In memory right now. This is what reveals RETENTION: a workflow that ENDS with a
    high resident value left something behind, which is the shape of a leak.
"""

from __future__ import annotations

import dataclasses
import sys
from collections.abc import Callable

RssProbe = Callable[[], "int | None"]

_BYTES_PER_GB = 1024**3


def peak_rss_bytes() -> int | None:
    """Peak RSS of this process in bytes, or ``None`` where unsupported (e.g. Windows)."""
    try:
        import resource
    except ImportError:
        return None
    # The suppression below is needed because `resource` is Unix-only: typeshed exposes
    # no attributes for it on win32, where pyright runs. Guarded at runtime by the
    # ImportError above, so this is a platform-analysis artefact, not a real unknown.
    raw = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)  # type: ignore[attr-defined]
    # ru_maxrss is KiB on Linux but BYTES on macOS. Databricks serverless is Linux;
    # the branch exists so this module does not lie by 1024x on a developer's laptop.
    return raw if sys.platform == "darwin" else raw * 1024


def current_rss_bytes() -> int | None:
    """Currently-resident RSS in bytes, or ``None`` where unsupported."""
    try:
        with open("/proc/self/statm", encoding="ascii") as fh:
            fields = fh.read().split()
    except OSError:
        return None
    if len(fields) < 2:
        return None
    import os

    # os.sysconf is Unix-only (same typeshed situation); the /proc read above cannot
    # succeed on a platform that lacks it, so this line is unreachable there.
    return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")  # type: ignore[attr-defined]


@dataclasses.dataclass(frozen=True)
class MemorySample:
    """One observation of driver memory, taken around a named unit of work."""

    label: str
    peak_bytes: int | None
    current_bytes: int | None
    peak_delta_bytes: int | None


def sample_memory(
    label: str,
    previous_peak: int | None,
    *,
    peak_probe: RssProbe = peak_rss_bytes,
    current_probe: RssProbe = current_rss_bytes,
) -> MemorySample:
    """Observe memory for ``label``, with the peak delta against ``previous_peak``."""
    peak = peak_probe()
    current = current_probe()
    delta = peak - previous_peak if (peak is not None and previous_peak is not None) else None
    return MemorySample(label=label, peak_bytes=peak, current_bytes=current, peak_delta_bytes=delta)


def _gb(value: int | None) -> str:
    return "unavailable" if value is None else f"{value / _BYTES_PER_GB:.2f} GB"


def format_memory(sample: MemorySample) -> str:
    """Render a sample for the structured log."""
    if sample.peak_bytes is None and sample.current_bytes is None:
        return f"driver memory unavailable on this platform (after {sample.label})"
    delta = "n/a" if sample.peak_delta_bytes is None else f"+{sample.peak_delta_bytes / _BYTES_PER_GB:.2f} GB"
    suffix = ""
    if sample.peak_bytes is not None and sample.current_bytes is not None and sample.peak_bytes < sample.current_bytes:
        # Physically impossible: a high-water mark cannot sit below a live reading. This is
        # the signature of the two adapters disagreeing on units (KiB vs bytes), which would
        # otherwise surface as a plausible-looking number nobody questions.
        suffix = " [WARNING: peak < resident — probe units are inconsistent]"
    return f"peak={_gb(sample.peak_bytes)} (delta {delta}), resident={_gb(sample.current_bytes)}{suffix}"
