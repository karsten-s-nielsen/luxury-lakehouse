"""kde_backend survives the queue round trip; NULL reads back as the default (pure — no Spark)."""

from ingestion.drain_adapters import _QUEUE_COLUMNS, _row_to_work_unit


def test_queue_has_kde_backend_column():
    assert "kde_backend" in [name for name, _, _ in _QUEUE_COLUMNS]


def test_null_kde_backend_reads_back_as_default():
    row = {
        "provider": "metrica",
        "match_id": "X",
        "period": 1,
        "frame_range_lo": None,
        "frame_range_hi": None,
        "kde_backend": None,
    }
    assert _row_to_work_unit(row).kde_backend == "fft-cic"


def test_explicit_kde_backend_roundtrips():
    row = {
        "provider": "skillcorner",
        "match_id": "Y",
        "period": 2,
        "frame_range_lo": None,
        "frame_range_hi": None,
        "kde_backend": "cpu-numba",
    }
    u = _row_to_work_unit(row)
    assert u.kde_backend == "cpu-numba"
    assert u.period == 2
