"""Tests for WorkflowCard Pydantic model."""

from __future__ import annotations

import textwrap

import pytest
from pydantic import ValidationError

from workflows.card import WorkflowCard

# ---------------------------------------------------------------------------
# Fixtures — reusable YAML fragments
# ---------------------------------------------------------------------------

MINIMAL_CARD = textwrap.dedent("""\
    ---
    name: Expected Goals Model
    id: wf-xg-v2
    version: "1.0.0"
    status: production
    type: training-and-inference
    domain: expected-goals
    owners:
      - karsten
    ---
    ## Overview
    Minimal card for testing.
""")

FULL_CARD = textwrap.dedent("""\
    ---
    name: Expected Goals Model
    id: wf-xg-v2
    version: "2.1.0"
    status: production
    type: training-and-inference
    domain: expected-goals
    tags:
      - xg
      - machine-learning
    owners:
      - karsten
      - ops-team

    references:
      - citation: "Anzer & Bauer (2021). Expected Passing."
        role: methodology
      - citation: "StatsBomb (2023). Open Data."
        role: dataset

    inputs:
      datasets:
        - id: luxury-lakehouse/statsbomb-spadl
          source: huggingface
          description: SPADL actions from StatsBomb open data
        - id: "{catalog}.silver.statsbomb_events"
          source: delta-table
      models:
        - id: luxury-lakehouse/football2vec-base
          source: huggingface

    outputs:
      models:
        - id: luxury-lakehouse/xg-model
          destination: huggingface
          format: json-base64
          alias: production
      tables:
        - id: "{catalog}.gold.expected_goals"
          destination: delta-table
          mart: mart_expected_goals
          synced: expected_goals
      datasets:
        - id: luxury-lakehouse/xg-predictions
          destination: huggingface

    execution:
      training:
        trigger: manual
        runtime: hf-jobs
        flavor: l40sx1
        script: scripts/train_xg.py
        timeout: "2h"
        also_trainable_via: databricks-notebook
      inference:
        trigger: scheduled
        runtime: databricks-workflow
        entry_point: compute_xg_shot_scores
        module: ingestion.xg_shot_scorer
        distribution: applyInPandas
        partition_key: competition_id
        schedule: "Every Sunday 06:00 UTC"
        timeout: "600s"
        environment: xg_task

    depends_on:
      - wf-spadl-v1
      - wf-statsbomb-v1

    idempotency:
      strategy: skip-guard
      key:
        - match_id
        - competition_id
      description: Skips matches already present in the output table.

    performance:
      training_time: "45 min on A10G"
      inference_timeout: "600s"
      memory_ceiling: "800 MB"

    cost:
      training:
        runtime: hf-jobs
        flavor: l40sx1
        rate_usd_per_hour: 3.15
        typical_duration_minutes: 45
        typical_cost_usd: 2.36
      inference:
        runtime: databricks
        sku: serverless
        typical_dbu: 12
        typical_cost_usd: 0.84

    monitoring:
      validator: model_validation
      metrics:
        - name: brier_score
          baseline: 0.08
          warn_above: 0.10
          alert_above: 0.15
        - name: log_loss
          baseline: 0.30
          warn_below: 0.20
          alert_below: 0.10
      freshness_sla_hours: 168

    links:
      model_card: docs/huggingface/model-cards/football2vec-statsbomb-wyscout.md
      dataset_cards:
        - docs/huggingface/statsbomb-spadl-card.md
      source_code:
        - src/ingestion/xg_shot_scorer.py
        - scripts/train_xg_v3_hf.py
      tests: src/tests/test_xg_shot_scorer.py
      hf_model: https://huggingface.co/luxury-lakehouse/xg-model
      hf_dataset: https://huggingface.co/datasets/luxury-lakehouse/xg-predictions
    ---

    ## Overview

    Full-featured xG workflow card for testing.
""")


# ---------------------------------------------------------------------------
# 1. Parse minimal valid card (only required fields)
# ---------------------------------------------------------------------------


def test_parse_minimal_valid_card() -> None:
    card = WorkflowCard.from_yaml_string(MINIMAL_CARD)
    assert card.name == "Expected Goals Model"
    assert card.id == "wf-xg-v2"
    assert card.version == "1.0.0"
    assert card.status == "production"
    assert card.type == "training-and-inference"
    assert card.domain == "expected-goals"
    assert card.owners == ["karsten"]
    assert card.body.strip() == "## Overview\nMinimal card for testing."


def test_minimal_card_optional_fields_default() -> None:
    card = WorkflowCard.from_yaml_string(MINIMAL_CARD)
    assert card.tags == []
    assert card.references == []
    assert card.inputs is None
    assert card.outputs is None
    assert card.execution is None
    assert card.depends_on == []
    assert card.idempotency is None
    assert card.performance is None
    assert card.cost is None
    assert card.monitoring is None
    assert card.links is None


# ---------------------------------------------------------------------------
# 2. Parse card with all fields populated
# ---------------------------------------------------------------------------


def test_parse_full_card() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.name == "Expected Goals Model"
    assert card.version == "2.1.0"
    assert card.tags == ["xg", "machine-learning"]
    assert card.owners == ["karsten", "ops-team"]
    assert len(card.references) == 2
    assert card.references[0].role == "methodology"


def test_full_card_inputs() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.inputs is not None
    assert len(card.inputs.datasets) == 2
    assert card.inputs.datasets[0].source == "huggingface"
    assert card.inputs.models is not None
    assert len(card.inputs.models) == 1


def test_full_card_outputs() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.outputs is not None
    assert card.outputs.models is not None
    assert len(card.outputs.models) == 1
    assert card.outputs.models[0].format == "json-base64"
    assert card.outputs.tables is not None
    assert card.outputs.tables[0].mart == "mart_expected_goals"
    assert card.outputs.datasets is not None
    assert len(card.outputs.datasets) == 1


def test_full_card_execution() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.execution is not None
    assert card.execution.training is not None
    assert card.execution.training.trigger == "manual"
    assert card.execution.training.runtime == "hf-jobs"
    assert card.execution.training.flavor == "l40sx1"
    assert card.execution.inference is not None
    assert card.execution.inference.distribution == "applyInPandas"
    assert card.execution.inference.entry_point == "compute_xg_shot_scores"


def test_full_card_depends_on() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.depends_on == ["wf-spadl-v1", "wf-statsbomb-v1"]


def test_full_card_cost() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.cost is not None
    assert card.cost.training is not None
    assert card.cost.training.rate_usd_per_hour == 3.15
    assert card.cost.training.typical_cost_usd == 2.36
    assert card.cost.inference is not None
    assert card.cost.inference.typical_dbu == 12


def test_full_card_monitoring() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.monitoring is not None
    assert card.monitoring.validator == "model_validation"
    assert card.monitoring.freshness_sla_hours == 168
    assert len(card.monitoring.metrics) == 2
    assert card.monitoring.metrics[0].name == "brier_score"
    assert card.monitoring.metrics[0].warn_above == 0.10


def test_full_card_links() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.links is not None
    assert card.links.model_card == "docs/huggingface/model-cards/football2vec-statsbomb-wyscout.md"
    assert len(card.links.source_code) == 2
    assert card.links.hf_model == "https://huggingface.co/luxury-lakehouse/xg-model"


# ---------------------------------------------------------------------------
# 3. Reject invalid status enum value
# ---------------------------------------------------------------------------


def test_reject_invalid_status() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: Bad Status
        id: wf-bad
        version: "1.0.0"
        status: archived
        type: training
        domain: test
        owners:
          - test
        ---
    """)
    with pytest.raises(ValidationError, match="status"):
        WorkflowCard.from_yaml_string(yaml_text)


# ---------------------------------------------------------------------------
# 4. Reject invalid type enum value
# ---------------------------------------------------------------------------


def test_reject_invalid_type() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: Bad Type
        id: wf-bad
        version: "1.0.0"
        status: draft
        type: deep-learning
        domain: test
        owners:
          - test
        ---
    """)
    with pytest.raises(ValidationError, match="type"):
        WorkflowCard.from_yaml_string(yaml_text)


# ---------------------------------------------------------------------------
# 5. Parse compound idempotency key (list of strings)
# ---------------------------------------------------------------------------


def test_idempotency_key_compound() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    assert card.idempotency is not None
    assert card.idempotency.key == ["match_id", "competition_id"]
    assert card.idempotency.strategy == "skip-guard"


# ---------------------------------------------------------------------------
# 6. Parse single idempotency key (string)
# ---------------------------------------------------------------------------


def test_idempotency_key_single() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: Single Key
        id: wf-single
        version: "1.0.0"
        status: development
        type: heuristic
        domain: test
        owners:
          - test
        idempotency:
          strategy: full-overwrite
          key: match_id
          description: Single key idempotency.
        ---
    """)
    card = WorkflowCard.from_yaml_string(yaml_text)
    assert card.idempotency is not None
    assert card.idempotency.key == "match_id"


# ---------------------------------------------------------------------------
# 7. Template variable {catalog} resolution
# ---------------------------------------------------------------------------


def test_resolve_templates() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    resolved = card.resolve_templates(catalog="soccer_analytics")

    # Input dataset with template
    assert resolved.inputs is not None
    assert resolved.inputs.datasets[1].id == "soccer_analytics.silver.statsbomb_events"

    # Output table with template
    assert resolved.outputs is not None
    assert resolved.outputs.tables is not None
    assert resolved.outputs.tables[0].id == "soccer_analytics.gold.expected_goals"

    # Non-template IDs unchanged
    assert resolved.inputs.datasets[0].id == "luxury-lakehouse/statsbomb-spadl"


def test_resolve_templates_returns_new_instance() -> None:
    card = WorkflowCard.from_yaml_string(FULL_CARD)
    resolved = card.resolve_templates(catalog="soccer_analytics")
    # Original card unmodified
    assert card.inputs is not None
    assert "{catalog}" in card.inputs.datasets[1].id
    # Resolved card has substitution
    assert resolved.inputs is not None
    assert "{catalog}" not in resolved.inputs.datasets[1].id


# ---------------------------------------------------------------------------
# 8. Parse Markdown body
# ---------------------------------------------------------------------------


def test_parse_markdown_body() -> None:
    card = WorkflowCard.from_yaml_string(MINIMAL_CARD)
    assert "## Overview" in card.body
    assert "Minimal card for testing." in card.body


def test_empty_body_when_no_markdown() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: No Body
        id: wf-nobody
        version: "1.0.0"
        status: draft
        type: validation
        domain: test
        owners:
          - test
        ---
    """)
    card = WorkflowCard.from_yaml_string(yaml_text)
    assert card.body.strip() == ""


# ---------------------------------------------------------------------------
# 9. Missing required field raises ValidationError
# ---------------------------------------------------------------------------


def test_missing_required_field_name() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        id: wf-missing
        version: "1.0.0"
        status: draft
        type: training
        domain: test
        owners:
          - test
        ---
    """)
    with pytest.raises(ValidationError, match="name"):
        WorkflowCard.from_yaml_string(yaml_text)


def test_missing_required_field_owners() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: Missing Owners
        id: wf-missing
        version: "1.0.0"
        status: draft
        type: training
        domain: test
        ---
    """)
    with pytest.raises(ValidationError, match="owners"):
        WorkflowCard.from_yaml_string(yaml_text)


def test_missing_required_field_id() -> None:
    yaml_text = textwrap.dedent("""\
        ---
        name: Missing ID
        version: "1.0.0"
        status: draft
        type: training
        domain: test
        owners:
          - test
        ---
    """)
    with pytest.raises(ValidationError, match="id"):
        WorkflowCard.from_yaml_string(yaml_text)


# ---------------------------------------------------------------------------
# Orchestrated trigger + bidirectional cross-reference (Task 2A)
# ---------------------------------------------------------------------------


_ORCHESTRATED_CARD = textwrap.dedent("""\
    ---
    name: Import OBSO
    id: wf-import-obso
    version: "1.0.0"
    status: production
    type: data-movement
    domain: soccer-analytics
    owners:
      - karsten
    execution:
      import:
        trigger: orchestrated
        orchestrated_by: wf-hf-sync
        runtime: databricks-workflow
        entry_point: import_obso_results
        module: ingestion.import_obso_results
        distribution: driver-bound
        timeout: "900s"
    ---
""")


_BAD_ORCHESTRATED_NO_PARENT = textwrap.dedent("""\
    ---
    name: Bad
    id: wf-bad
    version: "1.0.0"
    status: production
    type: data-movement
    domain: soccer-analytics
    owners:
      - karsten
    execution:
      import:
        trigger: orchestrated
        runtime: databricks-workflow
        entry_point: x
        module: y.z
        distribution: driver-bound
        timeout: "900s"
    ---
""")


_BAD_ORCHESTRATED_BY_WITHOUT_TRIGGER = textwrap.dedent("""\
    ---
    name: Bad
    id: wf-bad
    version: "1.0.0"
    status: production
    type: data-movement
    domain: soccer-analytics
    owners:
      - karsten
    execution:
      import:
        trigger: manual
        orchestrated_by: wf-hf-sync
        runtime: databricks-workflow
        entry_point: x
        module: y.z
        distribution: driver-bound
        timeout: "900s"
    ---
""")


_ORCHESTRATION_SUPERTASK_CARD = textwrap.dedent("""\
    ---
    name: HF Sync Super-task
    id: wf-hf-sync
    version: "1.0.0"
    status: production
    type: data-movement
    domain: soccer-analytics
    owners:
      - karsten
    execution:
      orchestration:
        trigger: scheduled
        runtime: databricks-workflow
        entry_point: hf_sync
        module: ingestion.hf_sync
        distribution: driver-bound
        timeout: "1800s"
        sub_operations:
          - wf-import-obso
          - wf-sync-hf-costs
    ---
""")


_BAD_ORCHESTRATION_EMPTY_SUB_OPS = textwrap.dedent("""\
    ---
    name: Bad
    id: wf-bad
    version: "1.0.0"
    status: production
    type: data-movement
    domain: soccer-analytics
    owners:
      - karsten
    execution:
      orchestration:
        trigger: scheduled
        runtime: databricks-workflow
        entry_point: hf_sync
        module: ingestion.hf_sync
        distribution: driver-bound
        timeout: "1800s"
        sub_operations: []
    ---
""")


def test_trigger_literal_accepts_orchestrated() -> None:
    card = WorkflowCard.from_yaml_string(_ORCHESTRATED_CARD)
    assert card.execution is not None
    # YAML `import:` key maps to Python attr `import_` via Pydantic alias.
    assert card.execution.import_ is not None
    assert card.execution.import_.trigger == "orchestrated"
    assert card.execution.import_.orchestrated_by == "wf-hf-sync"


def test_orchestrated_trigger_requires_orchestrated_by() -> None:
    with pytest.raises(ValidationError, match="orchestrated_by"):
        WorkflowCard.from_yaml_string(_BAD_ORCHESTRATED_NO_PARENT)


def test_orchestrated_by_requires_orchestrated_trigger() -> None:
    with pytest.raises(ValidationError, match="orchestrated"):
        WorkflowCard.from_yaml_string(_BAD_ORCHESTRATED_BY_WITHOUT_TRIGGER)


def test_orchestration_phase_exposes_sub_operations() -> None:
    card = WorkflowCard.from_yaml_string(_ORCHESTRATION_SUPERTASK_CARD)
    assert card.execution is not None
    assert card.execution.orchestration is not None
    assert card.execution.orchestration.sub_operations == ["wf-import-obso", "wf-sync-hf-costs"]
    assert card.execution.orchestration.entry_point == "hf_sync"


def test_orchestration_requires_non_empty_sub_operations() -> None:
    with pytest.raises(ValidationError, match="sub_operations"):
        WorkflowCard.from_yaml_string(_BAD_ORCHESTRATION_EMPTY_SUB_OPS)
