"""The hf_job writer: one ingestion run plus paced catalog commits on the throwaway HF test lake (spec §26 items 3,
15; plan Task 33 step 3). It pairs with the hf_space reader through the barrier.

Order: wait for the reader to be ready; run the ingestion (builds the dbt dimensions, a run-history row and a
registered tracking file — the by-value reads the reader checks); then make N paced commits into `ops_barrier`, so
the reader is guaranteed to sample at least two consistent CURRENT sequences (overlap is not left to timing); then
publish `writer-done` carrying `<final_seq>:<N>` so the reader asserts against the writer's real numbers. The
published version attaches read-only from hf:// with the same snapshot count as the working copy. The `test_lake`
fixture refuses to run unless the roots are a throwaway `*-test` pair, so the production lake is never written.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hfkit import (
    BARRIER_ARMED,
    BARRIER_READER_READY,
    BARRIER_WRITER_DONE,
    WRITER_WAIT_S,
    clear_marker,
    reset_lease,
    wait_for_marker,
)
from hfrunkit import assert_run_built, paced_commits, run_scenario

from lakehouse.catalog.reader import CatalogReader
from lakehouse.storage.hf import HfStorage

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]

PACED_COMMITS = 5


def test_run_populates_the_test_lake_for_the_reader(test_lake: HfStorage, tmp_path: Path) -> None:
    storage = test_lake
    reset_lease(storage)  # the shared test lake may hold a lease from an earlier hf_job test; start free
    clear_marker(storage, BARRIER_WRITER_DONE)
    clear_marker(storage, BARRIER_ARMED)  # paced_commits sets this after it publishes an empty ops_barrier
    wait_for_marker(storage, BARRIER_READER_READY, timeout_s=WRITER_WAIT_S)
    nonce = storage.get("lake", BARRIER_READER_READY).decode()  # the reader's campaign nonce; echo it in both markers
    payload = f"{nonce}:error".encode()  # always release the reader, even on failure, so it never waits the full budget
    try:
        result = run_scenario(storage, work_dir=tmp_path / "work")
        tracking = assert_run_built(storage, result)
        uri = storage.read_uri(tracking["root"], tracking["path"])
        assert uri.startswith("hf://buckets/"), f"the tracking file must register under its hf:// URI, got {uri!r}"
        final_seq, count, working = paced_commits(
            storage, count=PACED_COMMITS, sleep_s=3.0, work_dir=tmp_path / "paced", armed_value=nonce.encode()
        )
        payload = f"{nonce}:{final_seq}:{count}".encode()
        reader = CatalogReader(storage)
        try:
            with reader.session() as conn:
                snaps = conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
        finally:
            reader.close()
        assert snaps is not None and snaps[0] == working, (
            f"the hf:// published version lists {snaps} snapshots, not the working copy's {working}"
        )
    finally:
        storage.put("lake", BARRIER_WRITER_DONE, payload)
