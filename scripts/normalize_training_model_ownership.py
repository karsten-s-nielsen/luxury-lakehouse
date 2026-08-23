"""Normalize UC registered-model ownership for the HF-Jobs training models (ADR-080).

Since ADR-079, the HF-Jobs trainers register model versions + the ``@Champion`` alias
as the **ingestion service principal** via M2M OAuth. Writing a new version to a UC
registered model requires **ownership** - UC has no grantable "write model version"
privilege short of it. So every training model must be owned by a principal the SP
can act as: the ``dbt-owners-<env>`` group (the SP is a member).

This is a one-time / idempotent normalization, needed on two paths (ADR-080):

* **Existing workspaces** - the models were created historically by a *user* (training
  ran under a PAT), so they are user-owned. This script transfers them to the group.
* **Fresh installs** - the SP creates the models (schema ``CREATE_MODEL`` grant, ADR-080)
  and UC defaults ownership to the *creating principal* (the SP). This script normalizes
  that to the group so ownership is consistent regardless of who ran the first training.

The Terraform ``catalog`` module grants ``CREATE_MODEL`` and the experiment ``CAN_EDIT``
ACLs (durable across ``terraform apply``); model *ownership* is not a Terraform-managed
attribute for MLflow-created models, so it is normalized here instead. Run after a fresh
deploy, or any time a new training model is added.

Idempotent: a model already owned by the group is left untouched; a model that does not
exist yet (its first training has not run) is skipped with a log line.

Usage::

    uv run --extra sdk python scripts/normalize_training_model_ownership.py [--check] \
        [--catalog soccer_analytics] [--schema dev_gold] [--group dbt-owners-dev]

``--check`` reports drift without changing anything (exit 1 if any model is mis-owned).
"""

from __future__ import annotations

import argparse
import logging
import sys

from databricks.sdk.errors import NotFound

from ingestion.databricks_auth import workspace_client

logger = logging.getLogger("normalize_training_model_ownership")

# The registered models retrained via HF Jobs (ADR-079/ADR-080). Keep in sync with the
# experiment ACLs in terraform/environments/dev/main.tf and the trainers under scripts/.
TRAINING_MODELS: tuple[str, ...] = (
    "vaep_model",
    "scoutgpt",
    "xg_model_v3",
    "xt_gk_v2",
)


def normalize(*, catalog: str, schema: str, group: str, check_only: bool) -> int:
    """Set each training model's owner to ``group``. Returns the count of drifted models."""
    w = workspace_client()
    drifted = 0
    for name in TRAINING_MODELS:
        full_name = f"{catalog}.{schema}.{name}"
        try:
            model = w.registered_models.get(full_name=full_name)
        except NotFound:
            logger.info("skip %s - model does not exist yet (first training not run)", full_name)
            continue
        if model.owner == group:
            logger.info("ok %s - already owned by %s", full_name, group)
            continue
        drifted += 1
        if check_only:
            logger.warning("DRIFT %s - owner=%s, expected %s", full_name, model.owner, group)
            continue
        w.registered_models.update(full_name=full_name, owner=group)
        logger.info("fixed %s - owner %s -> %s", full_name, model.owner, group)
    return drifted


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="dev_gold")
    parser.add_argument("--group", default="dbt-owners-dev", help="Owning group (must match dbt_owners_group in TF)")
    parser.add_argument(
        "--check", action="store_true", help="Report drift without changing ownership (exit 1 on drift)"
    )
    args = parser.parse_args()

    drifted = normalize(catalog=args.catalog, schema=args.schema, group=args.group, check_only=args.check)
    if args.check and drifted:
        logger.error("%d training model(s) not owned by %s - run without --check to fix", drifted, args.group)
        return 1
    logger.info("done - %d model(s) %s", drifted, "would change" if args.check else "changed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
