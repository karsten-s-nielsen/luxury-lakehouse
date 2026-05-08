"""OBSO post-retrain smoke gate. Spec §3 — Spearman 2018 OBSO definition.

Reads the obso-pausa-values HF dataset (the post-republish artifact from Group 3).
Per-frame surface should integrate to 1.0 (probability surface invariant).
"""

from __future__ import annotations

import io
import math

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_TOLERANCE = 0.01  # +/-1% per spec §3
_SAMPLE_SIZE = 100


def _load_obso_sample() -> pa.Table:
    """Helper — pulls the published OBSO surfaces, returns a 100-row pyarrow Table.

    Phase 9 prep adds a public `download_obso_parquet` wrapper around the
    existing `_download_from_hf` helper in `ingestion.import_obso_results`.
    Until then, this gate is skipped via the AttributeError fallback.
    """
    import importlib

    try:
        module = importlib.import_module("ingestion.import_obso_results")
        download_fn = getattr(module, "download_obso_parquet", None)
        if download_fn is None:
            pytest.skip(
                "ingestion.import_obso_results.download_obso_parquet not yet "
                "exported. Phase 9 prep: add a public wrapper around "
                "_download_from_hf."
            )
    except ImportError as exc:
        pytest.skip(f"ingestion.import_obso_results unavailable: {exc}")
    parquet_bytes = download_fn()
    return pq.read_table(io.BytesIO(parquet_bytes)).slice(0, _SAMPLE_SIZE)


def test_obso_surface_integrates_to_one() -> None:
    """Sample 100 frames; each frame's per-cell surface integrates to 1.0 +/- 1%."""
    table = _load_obso_sample()
    sample = table.to_pandas()
    n_failures = 0
    for _, row in sample.iterrows():
        surface = np.asarray(row["obso_surface"])
        integrated = float(surface.sum())
        if not math.isclose(integrated, 1.0, abs_tol=_TOLERANCE):
            n_failures += 1
    assert n_failures == 0, f"{n_failures}/{_SAMPLE_SIZE} OBSO frames violated surface-integrates-to-1.0 invariant"


def test_obso_no_nan() -> None:
    table = _load_obso_sample()
    sample = table.to_pandas()
    n_nan = sum(bool(np.any(np.isnan(np.asarray(row["obso_surface"])))) for _, row in sample.iterrows())
    assert n_nan == 0, f"{n_nan}/{_SAMPLE_SIZE} OBSO surfaces contain NaN"
