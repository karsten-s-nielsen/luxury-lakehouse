"""Meta-test: every dbt mart in marts/*.sql declares exactly one of the four
classification tags from ADR-019: ``dimension``, ``input_mart``,
``intermediate_mart``, or ``output_mart``.

Background: PR-Cycle-C (2026-05-01) introduces three-stage dbt_build
(input -> compute -> intermediate -> compute -> output -> refresh). Each
mart needs to declare which stage it belongs to. The tag is read by
the dbt selector in ``terraform/modules/workflows/main.tf`` (PR-beta).

This test runs at PR-CI time. Pure regex scan of marts/*.sql; no dbt
manifest dependency, no warehouse dependency.

References:
- ADR-019 - Three-Stage dbt_build
- docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md section 3
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_MARTS_DIR = _REPO / "dbt_project" / "models" / "marts"

# The 4-tag taxonomy. Every mart declares exactly one.
_CLASSIFICATION_TAGS: frozenset[str] = frozenset({"dimension", "input_mart", "intermediate_mart", "output_mart"})

# Matches the entire `{{ config(...) }}` block. Captures the body (kwargs).
# Lazy-matches the body so the first `) }}` closes the block. Tolerates
# nested braces inside kwargs (e.g. ``meta={'contains_pii': False}``).
_CONFIG_BLOCK_RE = re.compile(
    r"\{\{\s*config\s*\((.+?)\)\s*\}\}",
    re.DOTALL,
)
# Matches the `tags=[...]` kwarg inside the config body and captures the
# list contents.
_TAGS_KWARG_RE = re.compile(r"\btags\s*=\s*\[([^\]]*)\]", re.DOTALL)
# Matches a quoted tag literal inside the captured tags list.
_TAG_LITERAL_RE = re.compile(r"['\"]([^'\"]+)['\"]")


def _extract_tags(sql_path: Path) -> set[str]:
    """Return the set of tags declared in the mart's `{{ config(tags=[...]) }}` block.

    Returns an empty set if no `{{ config(...) }}` block is found OR if the
    block has no `tags=` kwarg (either case is treated as a missing
    classification - the file fails the assertion).
    """
    text = sql_path.read_text(encoding="utf-8")
    config_match = _CONFIG_BLOCK_RE.search(text)
    if not config_match:
        return set()
    tags_match = _TAGS_KWARG_RE.search(config_match.group(1))
    if not tags_match:
        return set()
    return set(_TAG_LITERAL_RE.findall(tags_match.group(1)))


def _all_mart_files() -> list[Path]:
    """List every .sql file under dbt_project/models/marts/ (sorted, deterministic)."""
    return sorted(_MARTS_DIR.glob("*.sql"))


def test_every_mart_has_classification_tag() -> None:
    """Every marts/*.sql file must declare exactly one of the four
    classification tags in its ``{{ config(tags=[...]) }}`` block.

    Extra tags (e.g., ``marts``) are allowed; only the count of
    classification tags is checked.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tags = _extract_tags(sql_path)
        classification_tags = tags & _CLASSIFICATION_TAGS
        if len(classification_tags) == 0:
            errors.append(
                f"{sql_path.name}: no classification tag in `{{{{ config(tags=[...]) }}}}` block. "
                f"Add ONE of {sorted(_CLASSIFICATION_TAGS)} per ADR-019."
            )
        elif len(classification_tags) > 1:
            errors.append(
                f"{sql_path.name}: declares {len(classification_tags)} classification tags "
                f"({sorted(classification_tags)}); ADR-019 requires exactly one."
            )
    assert not errors, "\n".join(errors)


# ----------------------------------------------------------------------------
# Tier 2: semantic correctness - the tag must match the model's lineage.
# ----------------------------------------------------------------------------

# Curated set of bronze table names written by compute_* Databricks tasks
# that are NOT hard-dependencies of stage 1 ``dbt_build_input_marts``. A mart
# whose lineage contains any of these reads stage-2-or-later compute output
# and therefore CANNOT be tagged ``input_mart`` under three-stage architecture
# (ADR-019). Names cross-referenced from actual ``source(...)`` calls in
# dbt_project/models/staging/**/*.sql; mirrors the semantic of
# ``_BRONZE_READ_REQUIREMENTS`` in
# src/tests/test_workflow_dag_bronze_reads.py - when adding a new compute
# writer, update both lists.
#
# Exemption: ``tracking_player_metadata`` is written by ``extract_tracking_metadata``
# (a compute_* task by label) but is treated as ingest output for stage-1
# purposes. Stage 1 hard-depends on ``extract_tracking_metadata`` so its
# bronze table is available before stage 1 runs. fct_tracking_frames
# (input_mart) reads it. Do NOT add ``tracking_player_metadata`` to this set.
_COMPUTE_OUTPUT_BRONZE_TABLES: frozenset[str] = frozenset(
    {
        "defcon_results",  # compute_defcon_lite
        "elastic_event_match",  # compute_elastic_sync
        "expected_threat_grids",  # compute_expected_threat
        "formation_labels",  # compute_formations_efpi
        "line_breaking_results",  # compute_line_breaking
        "off_ball_xt_results",  # compute_off_ball_xt
        "pausa_raw_scores",  # import_obso_results
        "pausa_values",  # compute_pausa
        "pitch_control_values",  # compute_pitch_control
        "player_embeddings_raw",  # compute_embeddings_v2 (and v1)
        "player_positions",  # compute_formations_shape_graph
        "psxg_predictions",  # compute_psxg
        "space_creation_values",  # compute_space_creation
        "vaep_action_values",  # compute_spadl_vaep
        "xg_predictions",  # compute_xg_model
        "xg_predictions_v2",  # compute_xg_model_v2
    }
)

# Marts that compute tasks read directly. Used to assert intermediate_mart
# placement is justified.
_COMPUTE_READ_MARTS: dict[str, str] = {
    "fct_action_values": "compute_embeddings_v2",
    # Future intermediate_mart cases register here.
}

_REF_RE = re.compile(r"ref\s*\(\s*['\"]([^'\"]+)['\"]")
_SOURCE_RE = re.compile(r"source\s*\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]")


def _bronze_table_names(model_path: Path) -> set[str]:
    """Return the set of bronze table names referenced (transitively) by a mart.

    Walks ``{{ source('<schema>', '<table>') }}`` references in the mart
    .sql file and any dbt staging / intermediate models it ``ref(...)``s.
    Returns the union of bronze table names that flow into this mart.

    Implementation: regex scan of the .sql file plus all transitively-ref'd
    staging / intermediate models. dbt manifest would be more accurate but
    adds a build dependency we don't want in unit tests.
    """
    bronze_tables: set[str] = set()
    seen_files: set[Path] = set()
    queue: list[Path] = [model_path]
    search_dirs = [
        _REPO / "dbt_project" / "models" / "staging",
        _REPO / "dbt_project" / "models" / "intermediate",
        _MARTS_DIR,
    ]
    while queue:
        current = queue.pop()
        if current in seen_files or not current.exists():
            continue
        seen_files.add(current)
        text = current.read_text(encoding="utf-8")
        for match in _SOURCE_RE.finditer(text):
            bronze_tables.add(match.group(1))
        for match in _REF_RE.finditer(text):
            ref_name = match.group(1)
            for search_dir in search_dirs:
                for candidate in search_dir.rglob(f"{ref_name}.sql"):
                    if candidate not in seen_files:
                        queue.append(candidate)
    return bronze_tables


def _classification_of(sql_path: Path) -> str | None:
    """Return the single classification tag for a mart, or None if not exactly one."""
    tags = _extract_tags(sql_path)
    classification_tags = tags & _CLASSIFICATION_TAGS
    if len(classification_tags) != 1:
        return None
    return next(iter(classification_tags))


def test_input_mart_and_dimension_have_no_compute_output_lineage() -> None:
    """``input_mart`` and ``dimension`` marts must not have any compute-output
    bronze table in their lineage. If they do, they should be reclassified
    as ``intermediate_mart`` (compute-consumed) or ``output_mart`` (not
    compute-consumed) per ADR-019.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tag = _classification_of(sql_path)
        if tag not in ("input_mart", "dimension"):
            continue
        bronze = _bronze_table_names(sql_path)
        offenders = bronze & _COMPUTE_OUTPUT_BRONZE_TABLES
        if offenders:
            errors.append(
                f"{sql_path.name} (tagged {tag}): lineage contains compute-output bronze "
                f"table(s) {sorted(offenders)}. Reclassify as 'intermediate_mart' (if a "
                f"compute task reads it) or 'output_mart' (if not)."
            )
    assert not errors, "\n".join(errors)


def test_intermediate_mart_has_known_compute_consumer() -> None:
    """Every ``intermediate_mart`` must be in the ``_COMPUTE_READ_MARTS``
    registry - that's the single source of truth for which marts compute
    tasks read directly. New intermediate_mart entries land in this dict
    in the same PR that tags the mart.
    """
    errors: list[str] = []
    for sql_path in _all_mart_files():
        tag = _classification_of(sql_path)
        if tag != "intermediate_mart":
            continue
        if sql_path.stem not in _COMPUTE_READ_MARTS:
            errors.append(
                f"{sql_path.name} (tagged intermediate_mart): not in _COMPUTE_READ_MARTS "
                f"registry. Add it with the consuming compute task."
            )
    assert not errors, "\n".join(errors)
