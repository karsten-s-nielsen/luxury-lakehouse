"""S1 Task 12: the §23.6 native-table policy is mode-aware (plan T12).

A dim/link/lineage/ops table may always be native in the lake root. A bronze table is native by design ONLY on an
encrypted-local lake (where `ENCRYPTED` protects its row-level data); on an unencrypted-local or HF lake a native
bronze table leaks row-level data into the lake root. A rogue dbt-materialized row-level table (no bronze comment)
is forbidden in every mode.
"""

from __future__ import annotations

from lakehouse.bronze.writer import BRONZE_COMMENT
from lakehouse.runs.dbt import allowed_native_table


def test_bronze_native_is_allowed_only_on_encrypted_local() -> None:
    assert allowed_native_table("skillcorner_tracking_players", BRONZE_COMMENT, encrypted_local=True) is True
    assert allowed_native_table("skillcorner_tracking_players", BRONZE_COMMENT, encrypted_local=False) is False


def test_a_rogue_dbt_table_is_rejected_in_both_modes() -> None:
    assert allowed_native_table("events_copy", None, encrypted_local=True) is False
    assert allowed_native_table("events_copy", None, encrypted_local=False) is False


def test_dim_link_lineage_ops_are_always_allowed() -> None:
    for name in ("dim_matches", "player_links", "lineage_runs", "ops_matches"):
        assert allowed_native_table(name, None, encrypted_local=False) is True
        assert allowed_native_table(name, None, encrypted_local=True) is True
