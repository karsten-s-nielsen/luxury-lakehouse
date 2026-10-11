"""R-PARITY (ADR-087 amendment follow-up 3): the fixture Probe C under serverless's pyarrow 8.0.0.

The recurring failure class is `local-env ≠ serverless-env`: the canary passed CI on pyarrow 19 (uv.lock)
and failed on serverless pyarrow 8.0.0 (base runtime). The in-CI guards close most of it — R-GATE
(`test_no_pd_read_parquet_on_serverless`, bans `pd.read_parquet`) + R-GUARD
(`test_tracking_marts_canary::test_build_inputs_has_no_pd_read_parquet_dependency`, env-independent). This
leg is the belt: it runs the WHOLE fixture Probe C (read + all six scorers) under an actual pyarrow 8.0.0
install, catching any scorer-level (not just read) pyarrow-8 incompatibility BEFORE merge.

It is SKIPPED unless ``LL_PYARROW8_PARITY=1`` (so it does not add its ~30 s to every CI run, and does not
run under the wrong pyarrow), and it ASSERTS it is actually on pyarrow 8 — a parity leg that silently ran
on pyarrow 19 would be vacuous. Run it in the pyspark-test image with pyarrow downgraded to serverless's
version (PyPI 8.0.0 is a proxy for the serverless build)::

    docker build -t ll-pyspark-test -f docker/pyspark-test/Dockerfile .
    MSYS_NO_PATHCONV=1 docker run --rm -e LL_PYARROW8_PARITY=1 -e PYTHONPATH=/work/src \\
      -v "D:/Development/karstenskyt__luxury-lakehouse:/work" -w /work ll-pyspark-test \\
      bash -c "pip install -q 'pyarrow==8.0.0' && python -m pytest \\
        src/tests/tracking_marts/test_canary_pyarrow8_parity.py -p no:cacheprovider -o addopts='' -q"
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("LL_PYARROW8_PARITY"),
    reason="serverless pyarrow-8 parity leg — set LL_PYARROW8_PARITY=1 in the pyarrow==8.0.0 Docker image",
)


def test_fixture_probe_c_end_to_end_under_pyarrow8() -> None:
    """Build + score the bundled fixture through all six pure cores under pyarrow 8.0.0; every mart ≥ 1 row.
    This is the exact path that failed on serverless (R6) — proving the engine-agnostic read AND the scorers
    run under the serverless pyarrow before merge, not in a post-merge operator run."""
    import pyarrow

    assert pyarrow.__version__.startswith("8."), (
        f"R-PARITY leg must run on pyarrow 8.x (serverless parity), got {pyarrow.__version__} — "
        "the leg is vacuous otherwise"
    )

    from ingestion import tracking_marts_canary as canary

    outputs = canary.score_fixture_marts()
    assert set(outputs) == set(canary.MART_KEYS), f"mart key set differs: {set(outputs)}"
    counts = {m: len(outputs[m]) for m in canary.MART_KEYS}
    empty = [m for m, n in counts.items() if n == 0]
    assert not empty, f"mart(s) {empty} empty under pyarrow 8 — counts={counts}"
