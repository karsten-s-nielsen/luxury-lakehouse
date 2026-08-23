"""Conformance guard for the M2M training grants (ADR-080).

The HF-Jobs trainers register MLflow runs + UC model versions as the ingestion SP
(ADR-079). That needs, per training model:
  * a Terraform ``databricks_permissions.<key>_experiment_acl`` granting the dbt-owners
    group ``CAN_EDIT`` on the experiment (durable across ``terraform apply``), and
  * the model in the ownership-normalization script's ``TRAINING_MODELS`` list.

Plus the dbt-owners group needs ``CREATE_MODEL`` on the gold schema.

These live in three files that must stay in sync; this test fails loudly if a new
trainer is added without wiring its grants (or if the ad-hoc-grant revert regresses the
TF). Text-based on purpose — it asserts the *declarations* exist, not a live workspace.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_DEV_TF = _REPO / "terraform" / "environments" / "dev" / "main.tf"
_CATALOG_TF = _REPO / "terraform" / "modules" / "catalog" / "main.tf"
_OWNERSHIP_SCRIPT = _REPO / "scripts" / "normalize_training_model_ownership.py"

# model name (UC + TRAINING_MODELS) -> the databricks_permissions resource key in dev/main.tf.
# xt_gk_v2 is intentionally absent: its experiment does not exist until its first training run,
# so its ACL is added once the experiment exists (ADR-080). The script list still carries it.
_MODEL_TO_ACL_RESOURCE = {
    "vaep_model": "vaep_experiment_acl",
    "scoutgpt": "scoutgpt_experiment_acl",
    "xg_model_v3": "xg_v3_experiment_acl",
}


def _resource_block(text: str, resource_type: str, name: str) -> str:
    """Return the brace-balanced body of `resource "<type>" "<name>" { ... }`."""
    start = re.search(rf'resource\s+"{resource_type}"\s+"{re.escape(name)}"\s*{{', text)
    assert start, f'resource "{resource_type}" "{name}" not found'
    i = start.end() - 1  # at the opening brace
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[i : j + 1]
    raise AssertionError(f"unbalanced braces for {name}")


def test_training_experiments_grant_dbt_owners_can_edit() -> None:
    tf = _DEV_TF.read_text(encoding="utf-8")
    for model, resource_name in _MODEL_TO_ACL_RESOURCE.items():
        block = _resource_block(tf, "databricks_permissions", resource_name)
        assert "CAN_EDIT" in block, f"{model}: {resource_name} missing CAN_EDIT (training run creation, ADR-080)"
        assert "dbt_owners_group_display_name" in block, (
            f"{model}: {resource_name} CAN_EDIT must be granted to the dbt-owners group, not an SP/user"
        )


def test_gold_schema_grants_dbt_owners_create_model() -> None:
    catalog = _CATALOG_TF.read_text(encoding="utf-8")
    block = _resource_block(catalog, "databricks_grant", "dbt_owners_gold_select")
    assert "CREATE_MODEL" in block, "dbt-owners needs CREATE_MODEL on gold to register training models (ADR-080)"
    assert "dbt_owners_group_name" in block


def _training_models_from_script() -> set[str]:
    """Extract the TRAINING_MODELS constant via AST (no import side effects)."""
    tree = ast.parse(_OWNERSHIP_SCRIPT.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        elif isinstance(node, ast.Assign) and node.targets:
            target, value = node.targets[0], node.value
        else:
            continue
        if isinstance(target, ast.Name) and target.id == "TRAINING_MODELS" and value is not None:
            return set(ast.literal_eval(value))
    raise AssertionError("TRAINING_MODELS constant not found in the ownership script")


def test_ownership_script_lists_every_training_model() -> None:
    models = _training_models_from_script()
    # Every TF-managed training experiment must be in the ownership-normalization list...
    for model in _MODEL_TO_ACL_RESOURCE:
        assert model in models, f"{model} has a TF experiment ACL but is missing from TRAINING_MODELS"
    # ...and xt_gk_v2 (the not-yet-created model) is tracked so its ownership is normalized on creation.
    assert "xt_gk_v2" in models
