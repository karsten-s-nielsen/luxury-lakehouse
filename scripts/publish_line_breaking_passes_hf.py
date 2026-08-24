# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.101-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Publish line-breaking passes (fct_passes with line-breaking enrichment) to HF Hub.

Migrated from the line-breaking cell of notebooks/publish_datasets.py per HF4
(SK3-MIG-B). Inventory-only — NOT fired by the SK3-MIG-B Group 3 republishes
(line-breaking detection runs from the canonical SPADL-LTR fct_passes; coord
correctness is preserved through SK3-MIG-A).

Dataset: luxury-lakehouse/line-breaking-passes
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.hf_upload_seam import GuardedFrame, prepare_public_upload, upload_guarded

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/line-breaking-passes"

_PASSES_SQL = """\
SELECT p.pass_id,
       p.match_key, p.team_key, p.passer_player_key, p.recipient_player_key,
       dm.native_match_id AS match_id,
       p.player_id, p.team_id, p.pass_recipient_id,
       p.competition_id, p.season_id, p.period, p.minute, p.second,
       p.start_x, p.start_y, p.end_x, p.end_y,
       p.pass_type, p.pass_height, p.body_part,
       p.pass_length, p.pass_angle_radians,
       p.pass_outcome, p.is_cross, p.is_switch, p.is_through_ball,
       p.is_complete, p.is_progressive,
       p.pass_direction, p.is_line_breaking, p.lines_broken, p.line_breaking_type,
       p.data_source,
       -- Per-match HF redistribution tier (spec §6.7/D11). fct_passes carries no SkillCorner
       -- today (safe-by-absence); derived from dim_matches so the fail-closed leak guard halts
       -- the publish if a restricted match ever appears in this mart. NULL (unmatched) → guard
       -- fails closed (never silently public).
       dm.access_tier
FROM soccer_analytics.dev_gold.fct_passes p
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm ON p.match_key = dm.match_key
"""


def publish_to_hf_hub(guarded: GuardedFrame, hf_token: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        for source, sub in guarded.groupby("data_source"):
            sub.drop_columns(["data_source"]).write_parquet(staging_dir / f"data_source={source}" / "data.parquet")
        # delete_patterns are matched RELATIVE to path_in_repo ("data"), so the only correct
        # whole-path sweep is ["**"] — this call previously passed ["data/*"], which matches
        # NOTHING and had silently no-opped since it was written (the ADR-049 stale-part-file
        # class; CLAUDE.md mandates ["**"]). Re-uploaded files are pruned from the delete set by
        # upload_folder itself, so the sweep removes stale siblings and keeps what we just wrote.
        return upload_guarded(
            staging_dir,
            frames=[guarded],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )


def main() -> None:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    db_token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    df = query_databricks_sql(host, db_token, _PASSES_SQL, warehouse_id)
    if df.empty:
        raise RuntimeError("0 rows from fct_passes — verify dbt build")
    logger.info("Retrieved %s passes (%s line-breaking)", f"{len(df):,}", f"{int(df['is_line_breaking'].sum()):,}")

    # Fail-closed leak guard (spec §6.7/D11): this mart carries no SkillCorner today, but the guard
    # halts the publish (rather than leaking) if a restricted row ever appears. Drop the internal
    # access_tier column AFTER the guard, before upload (R2).
    prepared = prepare_public_upload(df, publisher="publish_line_breaking_passes_hf")

    url = publish_to_hf_hub(prepared.public, hf_token)
    upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_hf_card_path("line-breaking-passes.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info("Pipeline complete: %s", url)


if __name__ == "__main__":
    main()
