"""Anti-drift (review P1 guardrail 1): the dbt-side public-by-license allowlist MUST equal
``shared.access_tier.PUBLIC_BY_LICENSE_PROVIDERS`` — one policy, three enforcement points (classifier, leak guard,
dbt consistency test). The dbt side is a single var ``public_by_license_providers`` in ``dbt_project.yml`` (consumed
by ``tests/assert_access_tier_visibility_consistency.sql``); this test fails if it drifts from the Python constant.
"""

from __future__ import annotations

import pathlib

import yaml

from shared.access_tier import PUBLIC_BY_LICENSE_PROVIDERS

_DBT_PROJECT = pathlib.Path("dbt_project/dbt_project.yml")


def test_dbt_var_allowlist_matches_the_shared_constant() -> None:
    cfg = yaml.safe_load(_DBT_PROJECT.read_text(encoding="utf-8"))
    in_dbt = frozenset(cfg["vars"]["public_by_license_providers"])
    assert in_dbt == PUBLIC_BY_LICENSE_PROVIDERS, (
        f"dbt var public_by_license_providers {sorted(in_dbt)} has DRIFTED from PUBLIC_BY_LICENSE_PROVIDERS "
        f"{sorted(PUBLIC_BY_LICENSE_PROVIDERS)} — one policy, three enforcement points (review P1 guardrail 1)"
    )


def test_consistency_test_references_the_var_not_a_hardcoded_list() -> None:
    # The SQL must source the allowlist from the var (single source), never hardcode provider literals — otherwise
    # the anti-drift guard above is bypassed.
    sql = pathlib.Path("dbt_project/tests/assert_access_tier_visibility_consistency.sql").read_text(encoding="utf-8")
    assert "var('public_by_license_providers')" in sql
    for p in ("'statsbomb'", "'skillcorner'", "'gradientsports'"):
        assert f"not in ({p}" not in sql, "consistency test hardcodes a provider list — use the var"
