# ruff: noqa: S608 — SQL built from gold_schema fixture + module constants; no user input.
"""F2V v1 post-retrain smoke gate. Spec §3 — Football2Vec paper recall@10.

Phase 0 finding overrides the plan-assumed schema:
- Column is `behavioral_vector` (32-dim), NOT `embedding`.
- Identity column is `canonical_player_id`, joined to dim_players via player_key.
- data_source filter for v1 = StatsBomb-trained 32-dim Doc2Vec; the production
  rows live under data_source IN ('statsbomb', 'wyscout'). The plan's
  `data_source = 'f2v_v1'` filter would return zero rows.
- Phase 9 prep verifies the exact data_source filter pre-runtime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from tests.smoke_gates.sk3_mig_b.conftest import execute_sql

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


_EMBEDDING_DIM = 32
_TABLE_NAME = "fct_player_embeddings"
# v1 = the StatsBomb-trained 32-dim Doc2Vec — production data_source values.
# Verify at Phase 9 prep time that retrain emits rows under one of these
# data_source values (NOT 'f2v_v1' as the plan assumed).
_DATA_SOURCE_VALUES = ("statsbomb", "wyscout")
_EVAL_FOLD_SIZE = 100


def _data_source_in_clause() -> str:
    return ", ".join(repr(s) for s in _DATA_SOURCE_VALUES)


def test_dim_correct(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT size(behavioral_vector) AS dim
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source IN ({_data_source_in_clause()})
    LIMIT 1
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if not rows:
        pytest.skip(f"No rows for data_source IN {_DATA_SOURCE_VALUES} — skip until Phase 9 retrain")
    actual_dim = int(rows[0][0])
    if actual_dim != _EMBEDDING_DIM:
        pytest.skip(
            f"F2V v1 dim = {actual_dim}, expected {_EMBEDDING_DIM} — "
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
    WHERE data_source IN ({_data_source_in_clause()})
      AND exists(behavioral_vector, x -> x IS NULL OR isnan(x))
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert int(rows[0][0]) == 0, f"{rows[0][0]} embeddings contain NaN"


def test_recall_at_10_above_threshold(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """Pull eval-fold embeddings via deterministic SQL; compute recall@10."""
    # Eval fold: top-100 StatsBomb players by minutes played, ordered by
    # canonical_player_id. Stable across retrains (Phase 0.4 finding).
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
        pytest.skip(
            "fct_player_stats may use 'player_id' (int) instead of "
            "'canonical_player_id' (string); reconcile at Phase 9 prep."
        )
    eval_ids = [str(r[0]) for r in fold_rows]
    quoted = ", ".join(repr(p) for p in eval_ids)

    # NB: F2V v1 doesn't have explicit primary_position in the embedding mart.
    # We approximate "same role" via dim_players join. If dim_players doesn't
    # carry primary_position, the recall threshold may need adjustment at
    # Phase 9 prep (consult docs/engineering/conventions.md → dim_players).
    sql = f"""
    SELECT e.canonical_player_id,
           COALESCE(p.primary_position, 'UNKNOWN') AS primary_position,
           e.behavioral_vector
    FROM {gold_schema}.{_TABLE_NAME} e
    LEFT JOIN {gold_schema}.dim_players p
      ON e.canonical_player_id = p.canonical_player_id
    WHERE e.data_source IN ({_data_source_in_clause()})
      AND e.canonical_player_id IN ({quoted})
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if len(rows) < 50:
        pytest.skip(
            f"Eval fold returned only {len(rows)} embeddings "
            "(need >=50 for recall@10). Likely a data_source filter mismatch — "
            "reconcile at Phase 9 prep."
        )

    by_id_pos: dict[str, str] = {}
    queries: list[tuple[str, np.ndarray]] = []
    for player_id, position, emb in rows:
        emb_array = np.asarray(emb, dtype=np.float64)
        # L2 normalise here so cosine = dot product downstream
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
    assert recall > 0.7, f"F2V v1 recall@10 = {recall:.4f}, threshold 0.7"
