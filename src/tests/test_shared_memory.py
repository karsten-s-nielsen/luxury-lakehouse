"""Tests for the stdlib-only driver-memory probe (ADR-074).

Core tests inject fake probes — ``resource`` and ``/proc`` do not exist on Windows,
so a core test that touched the real adapters could not run on the dev box.

The ADAPTER test is Linux-gated and DOES run on CI (ubuntu-latest). It is not
optional: the entire value of this module is a number, and ``ru_maxrss`` is KiB on
Linux but BYTES on macOS. A wrong unit would make every logged figure wrong by
1024x while still looking plausible — and the memory investigation this module
exists to support would produce confident garbage.
"""

from __future__ import annotations

import sys

import pytest

from shared.memory import current_rss_bytes, format_memory, peak_rss_bytes, sample_memory

_GB = 1024**3


def test_sample_computes_peak_delta_against_previous() -> None:
    s = sample_memory("publish_x", 2 * _GB, peak_probe=lambda: 7 * _GB, current_probe=lambda: 5 * _GB)
    assert (s.label, s.peak_bytes, s.current_bytes, s.peak_delta_bytes) == ("publish_x", 7 * _GB, 5 * _GB, 5 * _GB)


def test_first_sample_has_no_delta() -> None:
    s = sample_memory("first", None, peak_probe=lambda: 3 * _GB, current_probe=lambda: 3 * _GB)
    assert s.peak_delta_bytes is None


def test_unsupported_platform_degrades_to_none_not_crash() -> None:
    s = sample_memory("x", None, peak_probe=lambda: None, current_probe=lambda: None)
    assert (s.peak_bytes, s.current_bytes, s.peak_delta_bytes) == (None, None, None)
    assert "unavailable" in format_memory(s)


def test_delta_is_none_when_probe_unavailable_even_with_known_previous() -> None:
    s = sample_memory("x", 2 * _GB, peak_probe=lambda: None, current_probe=lambda: None)
    assert s.peak_delta_bytes is None


def test_format_reports_peak_resident_and_delta() -> None:
    s = sample_memory("op", 1 * _GB, peak_probe=lambda: 4 * _GB, current_probe=lambda: 3 * _GB)
    text = format_memory(s)
    assert "4.00 GB" in text
    assert "3.00 GB" in text
    assert "+3.00 GB" in text


def test_flat_peak_reads_as_zero_delta_not_a_drop() -> None:
    """``ru_maxrss`` is a high-water mark: a light op shows +0.00 GB, never negative."""
    s = sample_memory("light", 8 * _GB, peak_probe=lambda: 8 * _GB, current_probe=lambda: 2 * _GB)
    assert s.peak_delta_bytes == 0
    assert "+0.00 GB" in format_memory(s)


def test_small_peak_below_resident_is_not_flagged() -> None:
    """Sampling skew between the two kernel interfaces must not read as a units bug.

    Regression for the CI failure that produced this tolerance: peak trailed current by
    151 KB on a ~1.8 GB process — healthy, and the exact comparison called it broken.
    """
    s = sample_memory("skew", None, peak_probe=lambda: 1_960_009_728, current_probe=lambda: 1_960_161_280)
    assert "WARNING" not in format_memory(s)


def test_peak_below_resident_is_flagged_as_a_units_mismatch() -> None:
    """Physically impossible, and the exact signature of the two adapters disagreeing on units.

    Cheap runtime self-check: without it a 1024x error looks like a plausible number.
    """
    s = sample_memory("bad", None, peak_probe=lambda: 1 * _GB, current_probe=lambda: 9 * _GB)
    assert "WARNING" in format_memory(s)
    assert "units" in format_memory(s)


@pytest.mark.skipif(sys.platform == "win32", reason="resource / proc are Linux-only; CI is ubuntu-latest")
def test_adapters_report_plausible_real_numbers() -> None:
    """The ONE number the whole ADR depends on. A 1024x unit error dies here.

    Allocates ~256 MB and asserts resident RSS rises by roughly that. Without this the
    adapters are never executed on any platform — they were `# pragma: no cover` in an
    earlier draft, which is how a units bug would have shipped invisibly.
    """
    before = current_rss_bytes()
    assert before is not None
    assert before > 8 * 1024**2, f"implausible baseline RSS: {before}"

    blob = bytearray(256 * 1024**2)
    try:
        after = current_rss_bytes()
        assert after is not None
        grew = after - before
        assert 128 * 1024**2 < grew < 512 * 1024**2, f"256MB alloc moved RSS by {grew} bytes — check units"
    finally:
        del blob

    peak = peak_rss_bytes()
    assert peak is not None
    # NOT an exact `>=`: ru_maxrss and /proc/self/statm are different kernel interfaces
    # sampled at different instants, so peak can trail current by a page or two. CI hit
    # exactly this — peak=1,960,009,728 vs current=1,960,161,280, 151 KB apart — and an
    # exact comparison called a healthy reading a units bug. 5% still catches 1024x.
    assert peak >= after * 0.95, f"peak {peak} materially below resident {after} — check units"
    assert peak < 64 * _GB, f"implausible peak {peak} — units are probably wrong"
