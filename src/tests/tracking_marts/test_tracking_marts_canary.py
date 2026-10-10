"""The bundled-fixture tracking-marts Probe C (ADR-087 amendment 3).

Proves, offline (no Spark, no bronze catalog), that:
- the WHEEL-bundled canary fixture resolves via ``importlib.resources`` (ships in the package, R2);
- scoring it through the six pure cores yields **>= 1 row for EVERY mart** — the R-N / R3 non-vacuity BAR
  (a 0-row mart would be a vacuous canary), watched to be non-vacuous and deterministic;
- ``run_fixture_probe_c`` RAISES (naming the offender) on an empty mart OR a scorer crash — the red-green
  that proves the detector is not vacuous;
- the mart-key set is single-sourced (``_oracle`` imports the lifted ``MART_KEYS`` — no drift).
"""

from __future__ import annotations

import importlib.resources
import logging

import pandas as pd
import pytest

from ingestion import tracking_marts_canary as canary

_LOG = logging.getLogger("test-canary")


@pytest.fixture(scope="module")
def fixture_outputs() -> dict[str, pd.DataFrame]:
    """Score the bundled fixture ONCE for the whole module (the real build+score is the slow part)."""
    return canary.score_fixture_marts()


def test_canary_fixture_ships_in_the_package() -> None:
    """R2: the fixture parquet set resolves from the INSTALLED ``ingestion`` package via
    ``importlib.resources`` (NOT a ``src/tests`` path), so it travels in the built wheel."""
    root = importlib.resources.files("ingestion") / "canary_fixture"
    xt = root / "xt_grid.parquet"
    unit = root / "idsse" / "J03WMXcanary_p1"
    assert xt.is_file(), f"canary xt_grid missing at {xt}"
    for name in ("frames.parquet", "actions.parquet", "meta.parquet"):
        assert (unit / name).is_file(), f"canary {name} missing at {unit / name}"
    # Load one to prove it is a readable parquet, not an empty placeholder.
    with importlib.resources.as_file(xt) as p:
        assert len(pd.read_parquet(p)) > 0


def test_fixture_yields_every_mart_non_empty(fixture_outputs: dict[str, pd.DataFrame]) -> None:
    """R-N / R3 (the BAR): the bundled fixture produces >= 1 row for EVERY one of the six marts.

    NON-vacuous by construction: the assertion names the exact per-mart counts, so a fixture that
    silently no-ops a mart (a scorer defect, or an over-trimmed window) fails HERE with the offender,
    not as a false-green preflight."""
    counts = {m: len(fixture_outputs[m]) for m in canary.MART_KEYS}
    assert set(fixture_outputs) == set(canary.MART_KEYS), f"mart key set differs: {set(fixture_outputs)}"
    empty = [m for m, n in counts.items() if n == 0]
    assert not empty, f"vacuous mart(s) {empty} — counts={counts}"
    # gk_decision is the binding mart (single shot at t~102 s); [0,130] s keeps it at its stable 2.
    assert counts["gk_decision"] >= 2, f"gk_decision margin eroded (counts={counts}) — widen the window"


def test_fixture_score_is_deterministic(fixture_outputs: dict[str, pd.DataFrame]) -> None:
    """A re-score yields identical per-mart row counts (the preflight must be reproducible)."""
    again = canary.score_fixture_marts()
    assert {m: len(again[m]) for m in canary.MART_KEYS} == {m: len(fixture_outputs[m]) for m in canary.MART_KEYS}


def test_run_fixture_probe_c_passes_on_real_fixture() -> None:
    """The happy path: the real bundled fixture clears Probe C without raising."""
    canary.run_fixture_probe_c(_LOG)  # must not raise


def test_run_fixture_probe_c_raises_on_an_empty_mart(monkeypatch) -> None:
    """RED-GREEN non-vacuity: a mart with 0 rows (valid schema) makes Probe C FAIL, naming it — the
    vacuous-green the canary exists to prevent. Proves the >=1-row bar is actually enforced."""
    good = pd.DataFrame({"x": [1]})
    doctored = {m: good for m in canary.MART_KEYS}
    doctored["gk_decision"] = pd.DataFrame({"x": []})  # one mart no-ops
    monkeypatch.setattr(canary, "score_fixture_marts", lambda **_k: doctored)
    with pytest.raises(RuntimeError, match=r"canary FAILED .fixture correctness.*gk_decision"):
        canary.run_fixture_probe_c(_LOG)


def test_run_fixture_probe_c_raises_on_a_scorer_crash(monkeypatch) -> None:
    """A scorer raising (the gk_decision is_actor class) surfaces as a ``canary FAILED`` preflight abort."""

    def _boom(**_k):
        raise ValueError("is_actor boom")

    monkeypatch.setattr(canary, "score_fixture_marts", _boom)
    with pytest.raises(RuntimeError, match=r"canary FAILED .fixture correctness.*is_actor boom"):
        canary.run_fixture_probe_c(_LOG)


def test_mart_keys_are_single_sourced() -> None:
    """``_oracle`` imports the lifted ``MART_KEYS`` (the CODE is single-sourced — no drift)."""
    from tests.tracking_marts import _oracle

    assert _oracle.MART_KEYS is canary.MART_KEYS
