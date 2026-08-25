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
# Every training model — including xt_gk_v2 — now has both a Terraform-managed experiment RESOURCE
# and its ACL: TF creates the experiment (ADR-081) so the grant always has a target, even before
# the model's first training run. The map covers the experiment ACLs; the resource presence is
# asserted separately below.
_MODEL_TO_ACL_RESOURCE = {
    "vaep_model": "vaep_experiment_acl",
    "scoutgpt": "scoutgpt_experiment_acl",
    "xg_model_v3": "xg_v3_experiment_acl",
    "xt_gk_v2": "xt_gk_v2_experiment_acl",
}

# model name -> the databricks_mlflow_experiment resource key in dev/main.tf (ADR-081). Terraform
# owns experiment creation so a fresh `apply` reproduces them (the training SP cannot create one).
_MODEL_TO_EXPERIMENT_RESOURCE = {
    "vaep_model": "vaep",
    "scoutgpt": "scoutgpt",
    "xg_model_v3": "xg_v3",
    "xt_gk_v2": "xt_gk_v2",
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


def test_training_experiments_are_terraform_managed_resources() -> None:
    """ADR-081: each training experiment is a `databricks_mlflow_experiment` RESOURCE (not a `data`
    lookup), so a fresh `terraform apply` CREATES it — the training SP is not a workspace admin and
    cannot create experiments itself. A `data` lookup would ERROR on a fresh workspace where the
    experiment does not exist, and leaves the ACL with nothing to grant on (the xt_gk_v2 failure)."""
    tf = _DEV_TF.read_text(encoding="utf-8")
    for model, resource_name in _MODEL_TO_EXPERIMENT_RESOURCE.items():
        block = _resource_block(tf, "databricks_mlflow_experiment", resource_name)
        assert "prevent_destroy" in block, (
            f"{model}: experiment resource {resource_name} must set lifecycle.prevent_destroy "
            f"to guard its run history against an accidental force-replace (ADR-081)"
        )
        # No stale `data "databricks_mlflow_experiment" "<name>"` lookup may remain for a managed model.
        assert not re.search(rf'data\s+"databricks_mlflow_experiment"\s+"{re.escape(resource_name)}"', tf), (
            f"{model}: a `data` lookup for {resource_name} remains — it must be a resource (ADR-081)"
        )
    # Every ACL must reference the resource, never a `data` source (which cannot exist post-ADR-081).
    assert "data.databricks_mlflow_experiment" not in tf, "experiment ACLs must reference the resource, not data"


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
    # Every TF-managed training experiment must be in the ownership-normalization list (xt_gk_v2
    # included — it is now a first-class TF-managed experiment like the others, ADR-081).
    for model in _MODEL_TO_ACL_RESOURCE:
        assert model in models, f"{model} has a TF experiment ACL but is missing from TRAINING_MODELS"
