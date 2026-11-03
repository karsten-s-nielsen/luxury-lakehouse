"""The orphan sweep on the real gateway (spec §26 item 15, §23.6 "Sweep with all path forms"; plan Task 33 step 3).

After the run populated the test lake (test_run_hf_live), the sweep deletes an orphan older than the lease start, the
age fence spares a younger one, and no kept-manifest file is deleted. The last assertion is the data-loss guard A and
B both called out: a path-form mismatch between the gateway listing and the DuckLake/manifest references could delete
a file `CURRENT` names. This test runs on the real gateway, where those path forms are produced, behind `test_lake`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hfkit import reset_lease
from hfrunkit import sweep_scenario

from lakehouse.storage.hf import HfStorage

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]


def test_sweep_age_fence_and_kept_safety(test_lake: HfStorage, tmp_path: Path) -> None:
    reset_lease(test_lake)
    outcome = sweep_scenario(test_lake, work_dir=tmp_path / "sweep")
    assert outcome["old_key"] in outcome["swept"], "the sweep did not delete the orphan older than the lease start"
    assert outcome["young_key"] not in outcome["swept"], "the age fence let a younger orphan be swept"
    assert outcome["young_exists"] is True, "the younger orphan was deleted despite the age fence"
    assert outcome["kept_survived"] is True, "a CURRENT-manifest file was deleted by the sweep (path-form data loss)"
    assert not (set(outcome["kept"]) & set(outcome["swept"])), "the sweep's report lists a kept file as swept"
    # T33-20 / §23.6: the lake must actually hold all three path forms, so kept_survived is not trivially green on a
    # single-form lake — a DuckLake-written relative file, an hf://-registered file, and an s3:// data_path.
    assert {"relative", "hf", "s3_data_path"} <= set(outcome["path_forms"]), (
        f"the sweep did not exercise all three §23.6 path forms; saw {outcome['path_forms']}"
    )
