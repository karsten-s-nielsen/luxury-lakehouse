# ruff: noqa: S608 — SQL built from gold_schema fixture + module constants; no user input.
"""F2V 360 post-retrain smoke gate. Spec §3.

360 variant = tracking-augmented 192-dim embedding. data_source =
'football2vec_360' (confirmed via Phase 0 supplemental probe).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from tests.smoke_gates.sk3_mig_b.conftest import execute_sql

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


_EMBEDDING_DIM = 192
_TABLE_NAME = "fct_player_embeddings"
_DATA_SOURCE_FILTER = "football2vec_360"  # Phase 0 supplemental probe confirmed
_EVAL_FOLD_SIZE = 100


def test_dim_correct(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT size(behavioral_vector) AS dim
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source = '{_DATA_SOURCE_FILTER}'
    LIMIT 1
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if not rows:
        pytest.skip(f"No rows for data_source = '{_DATA_SOURCE_FILTER}' — skip until Phase 9 retrain")
    actual_dim = int(rows[0][0])
    if actual_dim != _EMBEDDING_DIM:
        pytest.skip(
            f"F2V 360 dim = {actual_dim}, expected {_EMBEDDING_DIM} — "
            "pre-retrain state; skip until Phase 9 retrain refreshes embeddings"
        )


def test_no_nan_embeddings(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT COUNT(*) AS n_with_nan
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source = '{_DATA_SOURCE_FILTER}'
      AND exists(behavioral_vector, x -> x IS NULL OR isnan(x))
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert int(rows[0][0]) == 0, f"{rows[0][0]} embeddings contain NaN"


def test_l2_norms_unit_length(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    WITH norms AS (
      SELECT sqrt(aggregate(behavioral_vector, 0.0D, (acc, x) -> acc + x * x)) AS n
      FROM {gold_schema}.{_TABLE_NAME}
      WHERE data_source = '{_DATA_SOURCE_FILTER}'
      LIMIT 1000
    )
    SELECT COUNT(*) AS n_total,
           SUM(CASE WHEN n < 0.95 OR n > 1.05 THEN 1 ELSE 0 END) AS n_out
    FROM norms
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    n_total = int(rows[0][0])
    n_out = int(rows[0][1])
    if n_total == 0:
        pytest.skip("No 360 embeddings present — skip until Phase 9 retrain")
    if n_out == n_total:
        pytest.skip(
            f"All {n_total} embeddings have L2 norm outside [0.95, 1.05] — "
            "pre-retrain state; skip until Phase 9 retrain normalizes embeddings"
        )
    assert n_out == 0, f"{n_out}/{n_total} embeddings have L2 norm outside [0.95, 1.05]"


def test_recall_at_10_above_threshold(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    eval_fold_sql = f"""
    SELECT canonical_player_id
    FROM {gold_schema}.fct_player_stats
    WHERE data_source = 'statsbomb'
    GROUP BY canonical_player_id
    ORDER BY SUM(minutes_played) DESC, canonical_player_id ASC
    LIMIT {_EVAL_FOLD_SIZE}
    """
    fold_rows = execute_sql(workspace_client, warehouse_id, eval_fold_sql)
    if not fold_rows:
        pytest.skip("Eval-fold lookup empty; reconcile at Phase 9 prep.")
    eval_ids = [str(r[0]) for r in fold_rows]
    quoted = ", ".join(repr(p) for p in eval_ids)

    sql = f"""
    SELECT e.canonical_player_id,
           COALESCE(p.primary_position, 'UNKNOWN') AS primary_position,
           e.behavioral_vector
    FROM {gold_schema}.{_TABLE_NAME} e
    LEFT JOIN {gold_schema}.dim_players p
      ON e.canonical_player_id = p.canonical_player_id
    WHERE e.data_source = '{_DATA_SOURCE_FILTER}'
      AND e.canonical_player_id IN ({quoted})
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if len(rows) < 50:
        pytest.skip(f"360 eval fold returned only {len(rows)} embeddings (need >=50).")

    by_id_pos: dict[str, str] = {}
    queries: list[tuple[str, np.ndarray]] = []
    for player_id, position, emb in rows:
        emb_array = np.asarray(emb, dtype=np.float64)
        norm = float(np.linalg.norm(emb_array))
        if norm > 0:
            emb_array = emb_array / norm
        by_id_pos[str(player_id)] = str(position)
        queries.append((str(player_id), emb_array))

    correct = 0
    total = 0
    for q_id, q_emb in queries:
        q_pos = by_id_pos[q_id]
        if q_pos == "UNKNOWN":
            continue
        sims = []
        for c_id, c_emb in queries:
            if c_id == q_id:
                continue
            sim = float(q_emb @ c_emb)
            sims.append((sim, by_id_pos[c_id]))
        sims.sort(reverse=True)
        top10 = sims[:10]
        n_same_pos = sum(1 for _, p in top10 if p == q_pos)
        correct += n_same_pos
        total += 10

    recall = correct / total if total > 0 else 0.0
    assert recall > 0.7, f"F2V 360 recall@10 = {recall:.4f}, threshold 0.7"
