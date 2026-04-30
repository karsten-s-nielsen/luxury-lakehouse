"""Meta-test: every dbt intermediate model materialized as a ``view`` must
explicitly set ``schema='silver'`` (or another non-default schema).

Without this directive, dbt falls through to the profile's default target
schema (``soccer_analytics.dev`` for the daily-job's serverless target),
where the dbt-runner SP has no ``CREATE TABLE`` / ``USE SCHEMA`` grants.
The view-creation step then fails with::

    PERMISSION_DENIED: User does not have CREATE TABLE and USE SCHEMA on
    Schema 'soccer_analytics.dev'

Session 69 (2026-04-30) discovered this on ``int_player_xref`` and
``int_team_xref`` — both had the bare ``materialized='view'`` config copied
from the working ``int_unified_passes`` / ``int_unified_shots`` pattern, but
the latter pair includes ``schema='silver'``. The drift was invisible to
PR-CI because the post-PR-CI dbt build is the first time the offending
schema is touched.

This test scans every ``.sql`` file under ``dbt_project/models/intermediate/``
and asserts that any file declaring a non-ephemeral materialization also
declares an explicit ``schema=...`` value. Pure Python — no Databricks
connection required.

Reference pair (the canonical pattern this test enforces)::

    {{ config(materialized='view', schema='silver') }}    -- int_unified_passes.sql:1
    {{ config(materialized='view', schema='silver') }}    -- int_unified_shots.sql:1
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_INTERMEDIATE_DIR = _REPO / "dbt_project" / "models" / "intermediate"

# Match a dbt config()/config(... materialized=...) call. Tolerates whitespace,
# single or double quotes, and config args in any order.
_CONFIG_RE = re.compile(
    r"\{\{\s*config\s*\((?P<args>.*?)\)\s*\}\}",
    re.DOTALL | re.IGNORECASE,
)
_MATERIALIZED_RE = re.compile(
    r"materialized\s*=\s*['\"](?P<value>\w+)['\"]",
    re.IGNORECASE,
)
_SCHEMA_RE = re.compile(
    r"schema\s*=\s*['\"](?P<value>\w+)['\"]",
    re.IGNORECASE,
)


def _read_intermediate_models() -> list[Path]:
    """Return every .sql file under intermediate/, skipping documentation."""
    if not _INTERMEDIATE_DIR.is_dir():
        pytest.fail(f"intermediate dir not found: {_INTERMEDIATE_DIR}")
    return sorted(p for p in _INTERMEDIATE_DIR.glob("*.sql"))


def _model_config(model_path: Path) -> tuple[str | None, str | None]:
    """Return ``(materialized, schema)`` from the model's first ``{{ config(...) }}``,
    or ``(None, None)`` if no config block is present (model uses dbt-project defaults)."""
    text = model_path.read_text(encoding="utf-8")
    cfg = _CONFIG_RE.search(text)
    if not cfg:
        return None, None
    args = cfg.group("args")
    materialized = _MATERIALIZED_RE.search(args)
    schema = _SCHEMA_RE.search(args)
    return (
        materialized.group("value") if materialized else None,
        schema.group("value") if schema else None,
    )


def test_every_intermediate_view_has_explicit_schema() -> None:
    """Every intermediate model with ``materialized='view'`` (or 'table',
    'incremental' — anything non-ephemeral) MUST set ``schema=`` explicitly.

    The ``intermediate:`` block in ``dbt_project.yml`` defaults to
    ``materialized: ephemeral`` (which has no schema, so the directive is moot).
    The instant a model overrides materialization to a physical relation,
    the absence of ``schema=`` lands it in the ``dev`` profile schema where
    grants are not provisioned.
    """
    violations: list[str] = []
    for model_path in _read_intermediate_models():
        materialized, schema = _model_config(model_path)
        if materialized in (None, "ephemeral"):
            continue  # ephemeral models don't materialize — schema is irrelevant
        if not schema:
            violations.append(f"{model_path.name}: materialized={materialized!r} but schema= is unset")
    assert not violations, (
        "Intermediate models materialized as a physical relation must declare "
        "schema= explicitly (dbt would otherwise place them in the 'dev' default "
        "schema where the runtime SP lacks CREATE TABLE / USE SCHEMA grants).\n\n"
        + "\n".join(f"  - {v}" for v in violations)
        + "\n\nReference: dbt_project/models/intermediate/int_unified_passes.sql:1 — "
        "{{ config(materialized='view', schema='silver') }}"
    )


def test_intermediate_view_schemas_are_in_known_set() -> None:
    """Defence-in-depth: the explicit ``schema=`` value must be one of the
    Terraform-provisioned dbt schemas. Catches typos like ``schema='silvr'``."""
    known = {"silver", "gold"}
    violations: list[str] = []
    for model_path in _read_intermediate_models():
        materialized, schema = _model_config(model_path)
        if materialized in (None, "ephemeral") or schema is None:
            continue
        if schema not in known:
            violations.append(f"{model_path.name}: schema={schema!r} not in {known}")
    assert not violations, (
        "Intermediate model schema=... values must be among the Terraform-"
        "provisioned schemas:\n" + "\n".join(f"  - {v}" for v in violations)
    )
