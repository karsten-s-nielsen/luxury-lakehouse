"""Enforcement test — Champion-registering trainers use the ADR-012 §4 helpers.

Every training script that registers an MLflow ``@Champion`` consumed by the
Databricks Spark inference path MUST go through ``ingestion.artifact_deploy``:

- ``require_mlflow_env()`` at the top of ``main()`` — fail loud on a missing
  registration env var instead of silently skipping (the ``if tracking_uri:``
  bug class, ADR-002 + ADR-012 §4).
- ``set_and_verify_mlflow_champion(...)`` after ``mlflow.pyfunc.log_model`` —
  round-trips the alias read to catch the zombie-``@Champion`` state, instead
  of calling ``client.set_registered_model_alias(...)`` directly.

This generalizes the per-script AST regression in ``test_train_xg_v2_hf.py`` to
the WHOLE set of Champion-registering trainers, so a NEW trainer (or a future
edit to an existing one) cannot reintroduce the silent-skip / zombie-alias bug
class. AST-based source inspection (no execution) — these are PEP 723 scripts
whose runtime deps are not installed in the unit-test env.

Discovery (2026-06-06): ``grep -l 'set_registered_model_alias\\|alias="Champion"
\\|set_and_verify_mlflow_champion' scripts/train_*.py``. Keep ``_CHAMPION_TRAINERS``
in sync — adding a new Champion-registering trainer REQUIRES adding it here.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"

# Trainers that register an MLflow @Champion consumed by a Databricks scorer.
_CHAMPION_TRAINERS: tuple[str, ...] = (
    # train_xg_v2_hf.py retired 2026-07-10 with the v2 producer chain (ADR-066).
    "train_xg_v3_hf.py",
    "train_football2vec.py",
    "train_vaep_model_hf.py",
    "train_football2vec_v2.py",
    "train_football2vec_360.py",
    "train_scoutgpt_hf.py",
)

_REQUIRED_HELPERS = ("require_mlflow_env", "set_and_verify_mlflow_champion")


def _parse(name: str) -> tuple[str, ast.Module]:
    source = (_SCRIPTS_DIR / name).read_text(encoding="utf-8")
    return source, ast.parse(source)


def _names_imported_from_artifact_deploy(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "ingestion.artifact_deploy":
            names.update(alias.name for alias in node.names)
    return names


def _called_func_names(tree: ast.Module) -> set[str]:
    """Every callee name appearing in a Call node (bare name or attribute tail)."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


@pytest.mark.parametrize("trainer", _CHAMPION_TRAINERS)
def test_imports_required_artifact_deploy_helpers(trainer: str) -> None:
    _source, tree = _parse(trainer)
    imported = _names_imported_from_artifact_deploy(tree)
    missing = [h for h in _REQUIRED_HELPERS if h not in imported]
    assert not missing, (
        f"scripts/{trainer} must import {missing} from ingestion.artifact_deploy "
        "(ADR-012 §4). Champion-registering trainers go through the hardened "
        "delivery helpers, not direct MLflow registry calls."
    )


@pytest.mark.parametrize("trainer", _CHAMPION_TRAINERS)
def test_calls_required_artifact_deploy_helpers(trainer: str) -> None:
    _source, tree = _parse(trainer)
    called = _called_func_names(tree)
    missing = [h for h in _REQUIRED_HELPERS if h not in called]
    assert not missing, (
        f"scripts/{trainer} imports but does not CALL {missing} (ADR-012 §4). "
        "require_mlflow_env() must run at the top of main(); "
        "set_and_verify_mlflow_champion(...) must run after log_model."
    )


@pytest.mark.parametrize("trainer", _CHAMPION_TRAINERS)
def test_no_direct_set_registered_model_alias(trainer: str) -> None:
    """The zombie-alias guard lives in set_and_verify_mlflow_champion — the
    trainer must not call set_registered_model_alias directly."""
    _source, tree = _parse(trainer)
    assert "set_registered_model_alias" not in _called_func_names(tree), (
        f"scripts/{trainer} calls set_registered_model_alias directly. "
        "Use set_and_verify_mlflow_champion(...) (ADR-012 §4) so the alias "
        "is round-trip-verified against the registered version."
    )


@pytest.mark.parametrize("trainer", _CHAMPION_TRAINERS)
def test_no_silent_mlflow_tracking_uri_default(trainer: str) -> None:
    """``os.environ.get("MLFLOW_TRACKING_URI", "")`` is the mechanism that let
    the ``if tracking_uri:`` branch silently skip registration. Forbid it; the
    URI must be read via subscript so a missing value fails loud."""
    source, _tree = _parse(trainer)
    assert 'os.environ.get("MLFLOW_TRACKING_URI"' not in source, (
        f'scripts/{trainer} uses os.environ.get("MLFLOW_TRACKING_URI", ...) — '
        "the silent-empty-default footgun (ADR-012 §4). Read it via "
        'os.environ["MLFLOW_TRACKING_URI"] (subscript) after require_mlflow_env() '
        "has already proven it is present."
    )


@pytest.mark.parametrize("trainer", _CHAMPION_TRAINERS)
def test_no_bare_tracking_uri_if_gate(trainer: str) -> None:
    """No ``if tracking_uri:`` / ``if uri:`` gate wrapping MLflow registration."""
    _source, tree = _parse(trainer)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id in {"tracking_uri", "uri", "mlflow_tracking_uri"}
        ):
            raise AssertionError(
                f"scripts/{trainer}:{node.lineno} has a forbidden `if {node.test.id}:` "
                "gate. MLflow registration is mandatory (ADR-012 §4) — call "
                "require_mlflow_env() at entry and register unconditionally."
            )
