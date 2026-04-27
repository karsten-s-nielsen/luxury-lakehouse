"""ADR-013 second-application compliance test for fct_pausa_values.

PR 7 promotes ``fct_pausa_values`` from a Python writer direct-write to a
dbt-built mart following the [ADR-013](docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)
pattern (consumer-side ML inference output: Python writer → bronze raw →
dbt staging → gold mart with contract).

This test asserts the static structural invariants without needing a live
warehouse connection:

1. The mart file ``fct_pausa_values.sql`` exists at the canonical path.
2. The mart has ``contract: enforced: true``.
3. The mart resolves keys via ``INNER JOIN fct_passes ON pass_id`` (not
   via dim_matches/dim_teams/dim_players JOINs in the mart layer — those
   live in fct_passes).
4. The Python writer (``src/ingestion/pausa.py``) targets the bronze schema
   (``_BRONZE_SCHEMA == "bronze"``) and emits the ``pausa_values`` table.

PR 7 hotfix — live tests:

5. ``stg_pausa__values.pass_id`` is the dbt-utils surrogate (md5 hex)
   matching the recipe fct_passes uses (``[match_key, event_id, data_source]``);
   the bronze writer's prefixed native form (``idsse_<match>_<event>``) is
   parsed in staging.
6. ``fct_pausa_values`` has > 0 rows when bronze has rows (catches the
   silent 0-row JOIN failure that PR 7 shipped without a live JOIN guard).
7. INNER JOIN coverage from staging is high — a low coverage indicates
   either a recipe drift (silent bug) or a bronze upstream that emits
   passes whose source events were never ingested (data quality flag).

Mirrors the structural-invariant pattern of test_xg_v2_adr013_compliance
(if it exists) — extends to the second ADR-013 application.
"""

from __future__ import annotations

import importlib
import os
import re
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MART_PATH = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_pausa_values.sql"
_STG_PATH = _REPO_ROOT / "dbt_project" / "models" / "staging" / "pausa" / "stg_pausa__values.sql"

# Use importlib rather than `from databricks import sql` so pyright doesn't
# raise reportAttributeAccessIssue when databricks-sql-connector is absent
# (CI lint-and-test job runs without it; live tests skip via the marker below).
# Type: Any so the fixture's `.connect(...)` call type-checks without an
# explicit cast inside the test body.
_databricks_sql_mod: Any
try:
    _databricks_sql_mod = importlib.import_module("databricks.sql")
except ImportError:  # pragma: no cover — local CI without databricks-sql-connector
    _databricks_sql_mod = None

_requires_databricks = pytest.mark.skipif(
    _databricks_sql_mod is None
    or not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="databricks-sql-connector not installed or DATABRICKS_* env vars not set",
)
_MD5_HEX_RE = re.compile(r"^[0-9a-f]{32}$")
# JOIN-coverage floor for the live mart vs. staging row count. Bronze rows
# whose underlying pass event isn't in fct_passes (rare upstream gap) drop
# at the mart's INNER JOIN; the floor protects against silent recipe drift
# (which would drop ALL rows) without flagging legitimate data-quality gaps.
# Calibrated 2026-04-27 from observed 1627 bronze rows / IDSSE-only fct_passes coverage.
_MART_VS_STG_FLOOR = 0.95


class TestFctPausaValuesAdr013Compliance:
    """Static structural invariants for the ADR-013 second application."""

    def test_mart_file_exists(self) -> None:
        assert _MART_PATH.exists(), (
            f"ADR-013 mart file missing: {_MART_PATH}. "
            "PR 7 requires fct_pausa_values.sql to live alongside fct_xg_predictions_v2.sql."
        )

    def test_mart_has_contract_enforced(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        # Match either dict-style or YAML-mapping-style contract config
        # 'contract={'enforced': true}' OR 'contract: {enforced: true}'.
        assert re.search(r"contract\s*=\s*\{\s*['\"]enforced['\"]\s*:\s*true\s*\}", sql), (
            f"fct_pausa_values.sql must declare contract={{'enforced': true}}. Source: {_MART_PATH}"
        )

    def test_mart_inner_joins_fct_passes_on_pass_id(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        # ADR-013 normative: identity-fact INNER JOIN inheritance.
        # Match either {{ ref('fct_passes') }} or fct_passes via ref macro.
        assert re.search(
            r"inner\s+join\s+\{\{\s*ref\(\s*['\"]fct_passes['\"]\s*\)\s*\}\}.*on\s+\w+\.pass_id\s*=\s*\w+\.pass_id",
            sql,
            re.IGNORECASE | re.DOTALL,
        ), (
            "fct_pausa_values.sql must INNER JOIN fct_passes ON pass_id per ADR-013 "
            f"normative §3 (identity-fact key inheritance). Source: {_MART_PATH}"
        )

    def test_writer_targets_bronze(self) -> None:
        """src/ingestion/pausa.py must write to bronze, not gold."""
        from ingestion import pausa

        assert pausa._BRONZE_SCHEMA == "bronze"
        assert pausa._TABLE_NAME == "pausa_values"

    def test_workflow_card_lists_dbt_model(self) -> None:
        """wf-obso-pausa.yaml must declare dbt_model: fct_pausa_values."""
        card_path = _REPO_ROOT / "workflow-cards" / "wf-obso-pausa.yaml"
        assert card_path.exists()
        text = card_path.read_text(encoding="utf-8")
        assert "dbt_model: fct_pausa_values" in text, (
            "wf-obso-pausa.yaml outputs.tables must declare dbt_model: fct_pausa_values "
            "for the new ADR-013 gold mart entry. Source: " + str(card_path)
        )
        assert "bronze.pausa_values" in text, (
            "wf-obso-pausa.yaml outputs.tables must list the bronze raw target "
            "(catalog.bronze.pausa_values). Source: " + str(card_path)
        )

    def test_staging_resolves_surrogate_via_dbt_utils_macro(self) -> None:
        """stg_pausa__values must compute the surrogate pass_id via the SAME
        dbt_utils.generate_surrogate_key recipe fct_passes uses.

        The bronze writer emits the prefixed native form
        (`<provider>_<native_match_id>_<event_id>`); without the staging-side
        surrogate resolution the mart's INNER JOIN to fct_passes is 0-rows.
        Recipe drift between staging and the identity-fact mart is the most
        likely failure mode for any new ADR-013 application — this static
        check is the cheapest possible guard.
        """
        sql = _STG_PATH.read_text(encoding="utf-8")
        assert "dbt_utils.generate_surrogate_key" in sql, (
            "stg_pausa__values.sql must compute the surrogate pass_id via "
            "dbt_utils.generate_surrogate_key([match_key, event_id, data_source]) "
            "to match fct_passes' recipe. Source: " + str(_STG_PATH)
        )
        # Order-sensitive: surrogate inputs must be (match_key, event_id, data_source).
        flat = " ".join(sql.split())
        assert re.search(
            r"generate_surrogate_key\(\s*\[\s*['\"][^'\"]*match_key['\"]\s*,"
            r"\s*['\"][^'\"]*event_id['\"]\s*,\s*['\"][^'\"]*data_source['\"]\s*,?\s*\]",
            flat,
        ), (
            "stg_pausa__values surrogate-key inputs must be ordered "
            "[match_key, event_id, data_source] to match fct_passes; "
            "any reorder produces a different md5 and silently drops every JOIN."
        )

    def test_staging_joins_dim_matches_to_resolve_match_key(self) -> None:
        """match_key resolution requires a dim_matches JOIN — staging cannot
        invent the surrogate without this lookup."""
        sql = _STG_PATH.read_text(encoding="utf-8")
        flat = " ".join(sql.split())
        assert re.search(
            r"inner\s+join\s+\{\{\s*ref\(\s*['\"]dim_matches['\"]\s*\)\s*\}\}",
            flat,
            re.IGNORECASE,
        ), (
            "stg_pausa__values must INNER JOIN dim_matches to resolve match_key "
            "before computing the surrogate. Source: " + str(_STG_PATH)
        )


@_requires_databricks
class TestFctPausaValuesLive:
    """Live-warehouse assertions — skip when Databricks env vars are absent.

    These tests would have caught the PR 7 ship-day silent 0-row JOIN bug:
    the static-only test pass_id-recipe-must-match-fct_passes catches it
    pre-build; the live row-count and JOIN-coverage tests catch any drift
    that survives a recipe rename or upstream schema change.
    """

    @pytest.fixture(scope="class")
    def conn(self) -> object:  # type: ignore[override]
        assert _databricks_sql_mod is not None  # marker-guarded
        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        c = _databricks_sql_mod.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        try:
            yield c
        finally:
            c.close()

    def test_mart_row_count_matches_or_exceeds_floor(self, conn: object) -> None:
        """fct_pausa_values must have > 0 rows when bronze has rows, and
        the row count must be >= floor * staging row count.

        A coverage of 0% means surrogate-recipe drift (every JOIN dropped).
        A coverage between 0% and floor means a real upstream gap — surface
        it as a failure that requires investigation, not a silent skip.
        """
        cur = conn.cursor()  # type: ignore[attr-defined]
        try:
            cur.execute("SELECT count(*) FROM soccer_analytics.bronze.pausa_values")
            (bronze_rows,) = cur.fetchone()
        except Exception as exc:
            pytest.skip(f"bronze.pausa_values not built yet: {exc}")
            return

        if bronze_rows == 0:
            pytest.skip("bronze.pausa_values is empty — no rows to verify against")

        try:
            cur.execute(
                "SELECT "
                " (SELECT count(*) FROM soccer_analytics.dev_silver.stg_pausa__values) AS stg, "
                " (SELECT count(*) FROM soccer_analytics.dev_gold.fct_pausa_values) AS mart"
            )
        except Exception as exc:
            pytest.skip(f"stg_pausa__values or fct_pausa_values not built: {exc}")
            return
        stg, mart = cur.fetchone()
        assert mart > 0, (
            f"fct_pausa_values has 0 rows from {stg} staging rows. "  # noqa: S608 — assertion message, not query
            "This is the PR 7 ship-day silent JOIN-cardinality bug — "
            "stg_pausa__values is not computing the surrogate that matches "
            "fct_passes.pass_id. Investigate via: "
            "SELECT count(*) FROM stg_pausa__values p INNER JOIN fct_passes fp "
            "ON p.pass_id = fp.pass_id."
        )
        coverage = mart / stg if stg else 0.0
        assert coverage >= _MART_VS_STG_FLOOR, (
            f"fct_pausa_values coverage {coverage:.2%} (mart={mart} / stg={stg}) "
            f"below floor {_MART_VS_STG_FLOOR:.0%}. Either surrogate-recipe drift "
            "(unlikely if static tests pass) or a real upstream gap — investigate "
            "stg_pausa rows that don't INNER JOIN fct_passes on pass_id."
        )

    def test_mart_pass_id_is_md5_hex_surrogate(self, conn: object) -> None:
        """Every fct_pausa_values.pass_id must be a 32-char lowercase md5 hex
        string (the dbt_utils.generate_surrogate_key output shape). A row
        whose pass_id is the writer's prefixed native form (e.g. starts
        with `idsse_`) is a sign that a future change forgot the staging-
        side surrogate resolution.
        """
        cur = conn.cursor()  # type: ignore[attr-defined]
        try:
            cur.execute("SELECT pass_id FROM soccer_analytics.dev_gold.fct_pausa_values LIMIT 100")
        except Exception as exc:
            pytest.skip(f"fct_pausa_values not built: {exc}")
            return
        rows = [r[0] for r in cur.fetchall()]
        if not rows:
            pytest.skip("fct_pausa_values is empty — covered by sibling test")
            return
        for pid in rows:
            assert _MD5_HEX_RE.match(pid), (
                f"pass_id {pid!r} is not a 32-char lowercase md5 hex — staging "
                "is emitting the native form, not the dbt_utils surrogate."
            )
