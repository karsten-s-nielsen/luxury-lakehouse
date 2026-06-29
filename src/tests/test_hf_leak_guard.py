"""Every-run leak guard: no public artifact may carry a non-public row; the registry is exhaustive (spec §9.7)."""

from __future__ import annotations

import glob
import os

import pandas as pd
import pytest

from ingestion.hf_leak_guard import PUBLISHER_REGISTRY, LeakDetectedError, assert_no_private_leak


def test_public_frame_with_private_row_fails_closed() -> None:
    df = pd.DataFrame({"access_tier": ["public", "restricted"], "v": [1, 2]})
    with pytest.raises(LeakDetectedError):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_null_tier_fails_closed() -> None:
    df = pd.DataFrame({"access_tier": ["public", None], "v": [1, 2]})
    with pytest.raises(LeakDetectedError):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_all_public_passes() -> None:
    df = pd.DataFrame({"access_tier": ["public", "public"], "v": [1, 2]})
    assert_no_private_leak(df, publisher="publish_action_context_hf")  # no raise


def test_unregistered_publisher_fails_closed() -> None:
    df = pd.DataFrame({"access_tier": ["public"]})
    with pytest.raises(LeakDetectedError):
        assert_no_private_leak(df, publisher="publish_brand_new_hf")


def test_missing_access_tier_column_fails_closed() -> None:
    df = pd.DataFrame({"v": [1, 2]})
    with pytest.raises(LeakDetectedError):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_registry_covers_every_publisher_module() -> None:
    """A publisher in EITHER scripts/ or src/ingestion/ with no registry entry FAILS (B2 — the
    src/ingestion/ twins are the wired pyproject entry points; the guard must not be blind to them)."""
    paths = glob.glob("scripts/publish_*_hf.py") + glob.glob("src/ingestion/publish_*_hf.py")
    modules = {os.path.basename(p)[: -len(".py")] for p in paths}  # basename de-dupes scripts/ vs src/ twins
    missing = modules - set(PUBLISHER_REGISTRY)
    assert not missing, f"publishers missing from PUBLISHER_REGISTRY (leak guard would skip them): {sorted(missing)}"
