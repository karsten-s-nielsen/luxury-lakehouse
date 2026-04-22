"""Assert ``_normalize_mixed_types`` does not silently coerce string values to NaN.

Wyscout ingestion's mixed-type normalisation (``_normalize_mixed_types``) runs
``pd.to_numeric(df[col], errors="coerce")`` on any column whose first non-null
element is ``int | float``. If a LATER element in the same column is a string
that cannot parse as numeric, it becomes NaN silently — Mode 3 (unguarded
cast) failure from the PR #173 bronze drop-safety audit (G2).

Fix: ``_normalize_mixed_types`` accepts an optional ``logger`` parameter; when
provided, it emits an ERROR-level log for every column where
``pd.to_numeric(errors="coerce")`` reduces the non-null count. Callers treat
ERROR logs as blocking — per ADR-002 and BLE enforcement, warning-level
silent-swallow is forbidden for telemetry of this kind.

When ``logger`` is ``None`` (legacy default), the audit is silent — preserves
backward compatibility with the existing call sites.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from ingestion.wyscout import _normalize_mixed_types


def test_clean_numeric_column_passes(caplog: pytest.LogCaptureFixture) -> None:
    """All-numeric object column: coerce succeeds, no ERROR logs."""
    df = pd.DataFrame({"x": [1, 2.5, 3, None, 4]}, dtype=object)
    logger = logging.getLogger("wyscout.audit.clean")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit.clean"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records, f"unexpected ERROR logs for clean numeric column: {[r.message for r in caplog.records]}"


def test_mixed_numeric_and_bad_string_emits_error(caplog: pytest.LogCaptureFixture) -> None:
    """Numeric-first column with a later bad-string row: ERROR log fires with col name + loss count."""
    df = pd.DataFrame({"y": [1, 2, "not-a-number", 4, None]}, dtype=object)
    logger = logging.getLogger("wyscout.audit.mixed")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit.mixed"):
        _normalize_mixed_types(df, logger=logger)
    matching = [r for r in caplog.records if r.levelno == logging.ERROR and "coerce" in r.message.lower()]
    assert matching, f"expected ERROR log about coerce loss; got records: {[r.message for r in caplog.records]}"
    # Column name surfaced in the log record (either in message or args)
    rendered = matching[0].getMessage()
    assert "'y'" in rendered or '"y"' in rendered or ": y" in rendered, (
        f"column name 'y' not surfaced in log: {rendered!r}"
    )
    # Loss count of exactly 1 surfaced
    assert " 1 " in rendered or rendered.endswith(" 1") or "= 1" in rendered, (
        f"loss count of 1 not surfaced in log: {rendered!r}"
    )


def test_all_null_column_no_error(caplog: pytest.LogCaptureFixture) -> None:
    """Pure-null object column: no coerce branch, no ERROR."""
    df = pd.DataFrame({"z": [None, None, None]}, dtype=object)
    logger = logging.getLogger("wyscout.audit.null")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit.null"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records


def test_string_only_column_no_coerce(caplog: pytest.LogCaptureFixture) -> None:
    """First element is string → astype(str) branch; no numeric coerce."""
    df = pd.DataFrame({"s": ["alpha", None, "gamma"]}, dtype=object)
    logger = logging.getLogger("wyscout.audit.strings")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit.strings"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records


def test_default_logger_none_preserves_legacy_behaviour() -> None:
    """Without logger=, coerce happens silently (legacy call sites still work)."""
    df = pd.DataFrame({"y": [1, "bad", 3]}, dtype=object)
    result = _normalize_mixed_types(df)  # logger defaults to None
    # "bad" → NaN, 1 → 1.0, 3 → 3.0
    assert pd.isna(result["y"].iloc[1])
    assert result["y"].iloc[0] == 1.0
    assert result["y"].iloc[2] == 3.0
