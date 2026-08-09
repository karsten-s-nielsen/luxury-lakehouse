"""Medallion layer schemas are named, not inherited — and watermark upstreams are tables.

ADR-073. Two rules, each of which was a production defect first (2026-08-07,
run 271010187183650 — five of nine hf_sync sub-workflows failed and a sixth
swallowed its miss at INFO, inside a task that reported SUCCESS):

1. **A workflow card must not template the schema of a gold mart.**
   ``{catalog}.{schema}.fct_x`` resolves against whatever ``--schema`` the
   invoking task was given. hf_sync is given ``bronze`` — correct for its one
   import leg, wrong for the six consumers that read gold. There is exactly ONE
   environment, so the layer is a constant: name it (``{catalog}.dev_gold.fct_x``).

2. **A workflow card must not declare a VIEW as a delta-table upstream.**
   ``record_watermarks`` runs ``DESCRIBE HISTORY``, which rejects views. Every
   dbt ``stg_*`` model is materialized as a view (``dbt_project.yml`` →
   ``staging: +materialized: view``), so declare the bronze table it selects
   from instead. This fired only AFTER a fully successful 1.12M-row publish,
   which is precisely why it needs a static gate rather than a runtime one.

Both rules are checked statically — no live catalog, no Spark.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS = _REPO / "workflow-cards"
_INGESTION = _REPO / "src" / "ingestion"

# A gold mart referenced with a templated schema. Restricted to fct_/dim_ so a
# genuinely bronze-layer templated read is not swept up by accident.
_TEMPLATED_GOLD_MART = re.compile(r"\{catalog\}\.\{schema\}\.(?:fct|dim)_\w+")

# dbt staging models are views (dbt_project.yml: staging → +materialized: view).
_STAGING_VIEW = re.compile(r"\{catalog\}\.[a-z_]+\.(stg_\w+)")


def _card_paths() -> list[Path]:
    paths = sorted(_CARDS.glob("*.yaml"))
    assert paths, f"no workflow cards found under {_CARDS}"
    return paths


def _delta_table_ids(card_path: Path) -> list[str]:
    """Return every ``source: delta-table`` id declared by a card."""
    content = card_path.read_text(encoding="utf-8")
    parts = content.split("---")
    card = yaml.safe_load(parts[1] if len(parts) >= 3 else content)
    if not isinstance(card, dict):
        return []
    ids: list[str] = []
    inputs = card.get("inputs") or {}
    for section in ("tables", "datasets"):
        for entry in inputs.get(section) or []:
            if isinstance(entry, dict) and entry.get("source") == "delta-table":
                ids.append(str(entry.get("id", "")))
    return ids


@pytest.mark.parametrize("card_path", _card_paths(), ids=lambda p: p.name)
def test_card_does_not_template_a_gold_mart_schema(card_path: Path) -> None:
    """Rule 1 — gold marts name their layer instead of inheriting the task's schema."""
    offenders = _TEMPLATED_GOLD_MART.findall(card_path.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{card_path.name} templates the schema of a gold mart: {offenders}. "
        "{schema} resolves to whatever --schema the invoking task passes — hf_sync "
        "passes 'bronze'. Pin the layer: {catalog}.dev_gold.<mart> (ADR-073)."
    )


@pytest.mark.parametrize("card_path", _card_paths(), ids=lambda p: p.name)
def test_card_does_not_declare_a_view_as_delta_upstream(card_path: Path) -> None:
    """Rule 2 — watermark upstreams must be tables; dbt stg_* models are views."""
    offenders = [tid for tid in _delta_table_ids(card_path) if _STAGING_VIEW.search(tid)]
    assert not offenders, (
        f"{card_path.name} declares a dbt staging VIEW as a delta-table upstream: "
        f"{offenders}. record_watermarks runs DESCRIBE HISTORY, which fails on a view "
        "(EXPECT_TABLE_NOT_VIEW) — and does so only AFTER the publish succeeds. "
        "Declare the bronze table the stg_ model selects from (ADR-073)."
    )


def test_staging_models_are_still_views() -> None:
    """The premise of rule 2 — if staging stops being views, rule 2 must be revisited.

    Chesterton's Fence in test form: rule 2 bans stg_* upstreams *because* dbt
    materializes staging as views. If that config changes, this fails and tells
    the next reader why the ban existed rather than leaving a mystery rule.
    """
    cfg = yaml.safe_load((_REPO / "dbt_project" / "dbt_project.yml").read_text(encoding="utf-8"))
    staging = cfg["models"]["soccer_analytics"]["staging"]
    assert staging["+materialized"] == "view", (
        "dbt staging is no longer materialized as a view — "
        "test_card_does_not_declare_a_view_as_delta_upstream exists only because it was."
    )


def test_layer_schema_constants_are_the_single_source() -> None:
    """The three layer constants exist and hold the one environment's real schemas."""
    from shared.constants import (
        DEFAULT_BRONZE_SCHEMA,
        DEFAULT_GOLD_SCHEMA,
        DEFAULT_SILVER_SCHEMA,
    )

    assert (DEFAULT_BRONZE_SCHEMA, DEFAULT_SILVER_SCHEMA, DEFAULT_GOLD_SCHEMA) == (
        "bronze",
        "dev_silver",
        "dev_gold",
    )


def test_layer_schema_constants_are_sql_safe_identifiers() -> None:
    """The constants are interpolated straight into SQL, so they must be identifier-safe.

    The gold-reading builders carry ``# noqa: S608`` justified by "these are
    validated identifiers". Since ADR-073 the schema half of that claim comes
    from a constant rather than from ``_validate_identifier`` on a CLI arg —
    so the guarantee is asserted here instead of merely asserted in a comment.
    """
    from shared.constants import (
        DEFAULT_BRONZE_SCHEMA,
        DEFAULT_GOLD_SCHEMA,
        DEFAULT_SILVER_SCHEMA,
        IDENTIFIER_RE,
    )

    for name, value in (
        ("DEFAULT_BRONZE_SCHEMA", DEFAULT_BRONZE_SCHEMA),
        ("DEFAULT_SILVER_SCHEMA", DEFAULT_SILVER_SCHEMA),
        ("DEFAULT_GOLD_SCHEMA", DEFAULT_GOLD_SCHEMA),
    ):
        assert IDENTIFIER_RE.match(value), f"{name}={value!r} is not a SQL-safe identifier"


def test_hf_sync_gold_consumers_do_not_use_the_passed_schema() -> None:
    """Every `src/ingestion` module that reads gold must name the layer.

    hf_sync is passed ``--schema bronze``. Any module that interpolates the
    passed ``schema`` into an ``fct_``/``dim_`` reference is reading the wrong
    layer — that is the exact 2026-08-07 defect, in the exact modules it hit.
    """
    offenders: list[str] = []
    for module in sorted(_INGESTION.glob("*.py")):
        for lineno, line in enumerate(module.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"\{catalog\}\.\{schema\}\.(?:fct|dim)_", line):
                offenders.append(f"{module.name}:{lineno}")
    assert not offenders, (
        "gold marts interpolated with the caller-passed schema: "
        f"{offenders}. Use DEFAULT_GOLD_SCHEMA from shared.constants (ADR-073)."
    )
