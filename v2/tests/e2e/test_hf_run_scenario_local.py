"""Local validation of the shared HF run helpers (plan Task 33 items 3, 15).

The same `run_scenario`, `paced_commits` and `sweep_scenario` that the HF tests run on the throwaway test lake, run
here on a local lake so their logic is proved in CI before the owner's HF run: dbt builds the contract dimensions, the
run writes a history row and registers a tracking file, the private match's bronze stays restricted-only, the paced
writer's published snapshots match its working copy, and the sweep deletes an old orphan while the age fence spares a
young one and every kept-manifest file.
"""

from __future__ import annotations

from hfkit import BARRIER_ARMED
from hfrunkit import BARRIER_BUILD_TABLE, PRIVATE_MATCH, assert_run_built, paced_commits, run_scenario, sweep_scenario
from lakekit import LakeKit

from lakehouse.catalog.reader import CatalogReader
from lakehouse.ids.keys import match_key
from lakehouse.runs.history import read_table


def test_run_scenario_builds_dbt_history_and_tracking_locally(kit: LakeKit) -> None:
    result = run_scenario(kit.storage, work_dir=kit.base / "work")
    tracking = assert_run_built(kit.storage, result)
    assert kit.storage.read_uri(tracking["root"], tracking["path"]), "the tracking file has a canonical read URI"
    private_key = match_key("skillcorner", PRIVATE_MATCH)
    with CatalogReader(kit.storage).session() as conn:
        files = read_table(conn, "ops_match_files")
    roots = {row["root"] for row in files if row["match_key"] == private_key}
    assert roots == {"restricted"}, f"the private match's files must be restricted-only, got {roots}"


def test_run_scenario_fills_the_dbt_tables_the_hf_reader_reads(kit: LakeKit) -> None:
    """The hf_space reader reads `dim_players` for the scenario provider by value (item 15), so the scenario must
    produce dbt rows; this local twin catches a scenario that cannot (Task 12, 2026-10-31: the fake parser emitted no
    players, so the HF reader's assertion could never pass)."""
    run_scenario(kit.storage, work_dir=kit.base / "work")
    with CatalogReader(kit.storage).session() as conn:
        players = conn.execute("SELECT count(*) FROM lake.main.dim_players WHERE provider = 'skillcorner'").fetchone()
        tracking = conn.execute(
            "SELECT count(*), coalesce(sum(x_mm), 0) FROM lake.main.skillcorner_tracking_players"
        ).fetchone()
    assert players is not None and players[0] > 0, "the scenario fills dim_players"
    assert tracking is not None and tracking[0] > 0 and tracking[1] != 0, "the scenario fills the tracking table"


def test_paced_commits_and_snapshot_parity_locally(kit: LakeKit) -> None:
    run_scenario(kit.storage, work_dir=kit.base / "work")
    nonce = b"local-campaign"  # the HF run passes its reader's campaign nonce; the local twin uses a fixed one
    final_seq, count, working = paced_commits(
        kit.storage, count=4, sleep_s=0.0, work_dir=kit.base / "paced", armed_value=nonce
    )
    assert count == 4 and final_seq >= 1 and working > 0
    # the writer arms the barrier with the campaign nonce (after publishing the empty table), so a reader of a reused
    # lake starts sampling only once THIS campaign is armed
    assert kit.storage.get("lake", BARRIER_ARMED) == nonce
    with CatalogReader(kit.storage).session() as conn:
        rows = conn.execute(f"SELECT count(*), coalesce(sum(i), 0) FROM lake.main.{BARRIER_BUILD_TABLE}").fetchone()  # noqa: S608 - constant name
        snaps = conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
    assert rows == (4, 6), "the reader sees the paced writer's final rows (sum(range(4)) == 6)"
    assert snaps is not None and snaps[0] == working, "the published version's snapshots match the working copy"


def test_sweep_age_fence_and_kept_safety_locally(kit: LakeKit) -> None:
    run_scenario(kit.storage, work_dir=kit.base / "work")
    outcome = sweep_scenario(kit.storage, work_dir=kit.base / "sweep")
    assert outcome["old_key"] in outcome["swept"], "the old orphan was not swept"
    assert outcome["young_key"] not in outcome["swept"], "the age fence let a young orphan be swept"
    assert outcome["young_exists"] is True, "the young orphan was deleted despite the age fence"
    assert outcome["kept_survived"] is True, "a CURRENT-manifest file was deleted by the sweep"
    assert not (set(outcome["kept"]) & set(outcome["swept"])), "the sweep's report lists a kept file as swept"
    # T33-20: the HF sweep test needs a DuckLake-written relative file in the catalog; the local twin checks that the
    # scenario writes one (Task 12, 2026-10-31: every native write of the fake scenario stayed inlined).
    assert "relative" in outcome["path_forms"], f"the scenario wrote no DuckLake-native file: {outcome['path_forms']}"
