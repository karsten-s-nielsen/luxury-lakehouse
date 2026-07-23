"""Tests for the ADR-046 pin-policy core (scripts/_tf_env_pins.py) + sync CLI."""

from __future__ import annotations

import pytest

from scripts._tf_env_pins import (
    EXEMPT,
    Drift,
    ExemptStrategy,
    PinForkError,
    PinResolutionError,
    find_cross_env_divergences,
    find_pin_drift,
    parse_lock_versions,
    parse_sdk_extra_pin,
    resolve_desired_version,
)
from scripts.sync_tf_env_pins import rewrite_tf_text

_SDK_PIN = "0.121.0"


# --- resolver policy -------------------------------------------------------


def test_resolve_single_lock_version() -> None:
    lock = {"numba": {"0.66.0"}}
    assert resolve_desired_version("numba", lock=lock, sdk_extra_pin=_SDK_PIN) == "0.66.0"


def test_resolve_name_normalization() -> None:
    # TF underscore form must resolve against the canonical hyphen lock name (M6).
    lock = {"huggingface-hub": {"1.6.0"}}
    assert resolve_desired_version("huggingface_hub", lock=lock, sdk_extra_pin=_SDK_PIN) == "1.6.0"


def test_resolve_leave_as_is_returns_none() -> None:
    assert EXEMPT["statsbombpy"].strategy is ExemptStrategy.LEAVE_AS_IS
    assert resolve_desired_version("statsbombpy", lock={}, sdk_extra_pin=_SDK_PIN) is None


def test_resolve_missing_and_not_exempt_raises() -> None:
    with pytest.raises(PinResolutionError):
        resolve_desired_version("no-such-pkg", lock={}, sdk_extra_pin=_SDK_PIN)


def test_resolve_nonexempt_fork_raises() -> None:
    # R1: the package under test must be NON-exempt to reach the fork branch.
    lock = {"forked-pkg": {"0.117.0", "0.121.0"}}
    with pytest.raises(PinForkError):
        resolve_desired_version("forked-pkg", lock=lock, sdk_extra_pin=_SDK_PIN)


def test_resolve_exempt_beats_fork() -> None:
    # R1: databricks-sdk with the SAME multi-version lock set resolves via the [sdk] pin
    # WITHOUT raising — proves exempt-check precedes fork detection.
    lock = {"databricks-sdk": {"0.117.0", "0.121.0"}}
    assert EXEMPT["databricks-sdk"].strategy is ExemptStrategy.SDK_EXTRA
    assert resolve_desired_version("databricks-sdk", lock=lock, sdk_extra_pin=_SDK_PIN) == _SDK_PIN


def test_parse_lock_versions_keeps_forks_as_sets() -> None:
    text = (
        '[[package]]\nname = "databricks-sdk"\nversion = "0.117.0"\nsource = { registry = "x" }\n\n'
        '[[package]]\nname = "databricks-sdk"\nversion = "0.121.0"\nsource = { registry = "x" }\n'
    )
    assert parse_lock_versions(text)["databricks-sdk"] == {"0.117.0", "0.121.0"}


def test_parse_sdk_extra_pin() -> None:
    assert parse_sdk_extra_pin('  "databricks-sdk==0.121.0",\n') == "0.121.0"


# --- drift finder ----------------------------------------------------------

_TF_FIXTURE = """
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "numba==0.66.0",
        "scipy==1.15.3"
      ]
    }
  }
"""


def test_find_pin_drift_flags_stale_pin() -> None:
    lock = {"numba": {"0.66.0"}, "scipy": {"1.99.0"}}  # scipy drifted
    drifts = find_pin_drift(_TF_FIXTURE, lock, _SDK_PIN)
    assert drifts == [Drift(env_key="analytics", pkg="scipy", current="1.15.3", desired="1.99.0")]


def test_find_pin_drift_empty_when_in_sync() -> None:
    lock = {"numba": {"0.66.0"}, "scipy": {"1.15.3"}}
    assert find_pin_drift(_TF_FIXTURE, lock, _SDK_PIN) == []


# --- cross-env divergence --------------------------------------------------


def test_find_cross_env_divergences_flags_split() -> None:
    envs = {"env_a": {"databricks-sdk": "==0.117.0"}, "env_b": {"databricks-sdk": "==0.121.0"}}
    assert find_cross_env_divergences(envs) == {"databricks-sdk": {"env_a": "0.117.0", "env_b": "0.121.0"}}


def test_find_cross_env_divergences_ignores_agreement() -> None:
    envs = {"env_a": {"huggingface-hub": "==1.6.0"}, "env_b": {"huggingface-hub": "==1.6.0"}}
    assert find_cross_env_divergences(envs) == {}


# --- sync CLI rewrite ------------------------------------------------------

_TF_WITH_COMMENT = """
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        # historical drift: "scipy==9.9.9" must NOT be rewritten (it is a comment)
        "silly-kicks[das,ghost-gk]==4.43.0",
        "scipy==1.15.3"
      ]
    }
  }
"""


def test_rewrite_updates_stale_and_preserves_extras() -> None:
    lock = {"silly-kicks": {"4.44.0"}, "scipy": {"1.15.3"}}
    new_text, changes = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"silly-kicks[das,ghost-gk]==4.44.0"' in new_text  # bumped, extras preserved
    assert '"scipy==1.15.3"' in new_text  # untouched (in sync)
    assert [(c.pkg, c.current, c.desired) for c in changes] == [("silly-kicks", "4.43.0", "4.44.0")]


def test_rewrite_never_touches_comment_versions() -> None:  # M4 (full-line comment)
    lock = {"silly-kicks": {"4.43.0"}, "scipy": {"1.15.3"}}
    new_text, changes = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"scipy==9.9.9"' in new_text  # the comment's version string survives verbatim
    assert changes == []


_TF_TRAILING_COMMENT = """
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "scipy==1.15.3"  # historical: was "scipy==9.9.9"
      ]
    }
  }
"""


def test_rewrite_never_touches_trailing_comment_versions() -> None:  # M4 / P2 (trailing inline comment)
    lock = {"scipy": {"1.20.0"}}  # the real (code) pin should bump
    new_text, changes = rewrite_tf_text(_TF_TRAILING_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"scipy==1.20.0"' in new_text  # code pin bumped
    assert '"scipy==9.9.9"' in new_text  # trailing-comment pin survives verbatim
    assert [(c.pkg, c.current, c.desired) for c in changes] == [("scipy", "1.15.3", "1.20.0")]


def test_rewrite_is_idempotent() -> None:  # M5
    lock = {"silly-kicks": {"4.44.0"}, "scipy": {"1.15.3"}}
    once, _ = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    twice, changes2 = rewrite_tf_text(once, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert twice == once and changes2 == []


def test_rewrite_leaves_statsbombpy_untouched() -> None:
    tf = """
  environment {
    environment_key = "statsbomb"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "statsbombpy==1.19.0"
      ]
    }
  }
"""
    new_text, changes = rewrite_tf_text(tf, lock={}, sdk_extra_pin=_SDK_PIN)
    assert new_text == tf and changes == []


# --- true drift e2e (M8 / R4) ----------------------------------------------


def test_drift_fixture_synced_then_parity_clean() -> None:
    """A deliberately-drifted fixture -> sync rewrite -> find_pin_drift is empty. Drives the
    core drift-finder on the FIXTURE text (not the zero-arg sentinel that reads hardcoded
    repo paths, which would be vacuous)."""
    drifted = _TF_WITH_COMMENT  # pins silly-kicks==4.43.0, scipy==1.15.3
    lock = {"silly-kicks": {"4.50.0"}, "scipy": {"1.15.3"}}  # silly-kicks bumped in "lock"

    before = find_pin_drift(drifted, lock, _SDK_PIN)
    assert before == [Drift(env_key="analytics", pkg="silly-kicks", current="4.43.0", desired="4.50.0")]

    synced, _ = rewrite_tf_text(drifted, lock=lock, sdk_extra_pin=_SDK_PIN)
    after = find_pin_drift(synced, lock, _SDK_PIN)
    assert after == []  # the fix path actually closed the drift
