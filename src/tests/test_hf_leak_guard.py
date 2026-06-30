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


def test_divergence_non_allowlisted_public_row_with_nonpublic_visibility_fails_closed() -> None:
    # H1.3 approach A: a skillcorner (non-allowlisted) row that is access_tier='public' but whose true visibility
    # is NOT public is a stamp divergence — the on-the-guard, fail-closed backstop must catch it.
    df = pd.DataFrame(
        {
            "access_tier": ["public", "public"],
            "data_source": ["skillcorner", "skillcorner"],
            "visibility": ["public", None],
        }
    )
    with pytest.raises(LeakDetectedError, match="divergence"):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_divergence_allowlisted_provider_needs_no_visibility() -> None:
    # statsbomb is open-data (allowlist) — access_tier='public' with NULL visibility is fine, no divergence.
    df = pd.DataFrame({"access_tier": ["public"], "data_source": ["statsbomb"], "visibility": [None]})
    assert_no_private_leak(df, publisher="publish_action_context_hf")  # no raise


def test_divergence_non_allowlisted_with_public_visibility_passes() -> None:
    df = pd.DataFrame({"access_tier": ["public"], "data_source": ["skillcorner"], "visibility": ["public"]})
    assert_no_private_leak(df, publisher="publish_action_context_hf")  # no raise


def test_divergence_check_inert_when_visibility_column_absent() -> None:
    # Row-level marts without a visibility column rely on access_tier (which post-P1 encodes the decision) + the
    # dim_matches source dbt test; the per-row divergence check simply does not fire (it never falsely passes a
    # non-public access_tier — that is still caught above).
    df = pd.DataFrame({"access_tier": ["public", "public"], "data_source": ["skillcorner", "statsbomb"]})
    assert_no_private_leak(df, publisher="publish_action_context_hf")  # no raise


def test_registry_covers_every_publisher_module() -> None:
    """A publisher in EITHER scripts/ or src/ingestion/ with no registry entry FAILS (B2 — the
    src/ingestion/ twins are the wired pyproject entry points; the guard must not be blind to them)."""
    paths = glob.glob("scripts/publish_*_hf.py") + glob.glob("src/ingestion/publish_*_hf.py")
    modules = {os.path.basename(p)[: -len(".py")] for p in paths}  # basename de-dupes scripts/ vs src/ twins
    missing = modules - set(PUBLISHER_REGISTRY)
    assert not missing, f"publishers missing from PUBLISHER_REGISTRY (leak guard would skip them): {sorted(missing)}"
