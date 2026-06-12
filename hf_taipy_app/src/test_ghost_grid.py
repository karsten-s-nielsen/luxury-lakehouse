"""GhostGridProvider port contract + stored-spread adapter math (ADR-051 section 3)."""

import numpy as np
import pandas as pd
from services.ghost_grid import GhostGrid, StoredSpreadProvider, resolve_provider


def test_stored_spread_grid_peaks_at_optimum():
    p = StoredSpreadProvider()
    g = p.grid(ghost_x=10.0, ghost_y=30.0, density_spread=9.0, frame_players=None)
    assert isinstance(g, GhostGrid)
    iy, ix = np.unravel_index(np.argmax(g.z), g.z.shape)
    assert abs(g.xs[ix] - 10.0) < 1.0 and abs(g.ys[iy] - 30.0) < 1.0
    assert g.source == "stored"


def test_resolve_provider_defaults_to_stored(monkeypatch):
    monkeypatch.delenv("LL_GHOST_GRID", raising=False)
    assert isinstance(resolve_provider(), StoredSpreadProvider)


def test_model_provider_failure_degrades_loudly(monkeypatch):
    # model mode but the loader blows up -> stored result with source='stored-fallback'
    monkeypatch.setenv("LL_GHOST_GRID", "model")
    p = resolve_provider(model_loader=lambda: (_ for _ in ()).throw(RuntimeError("no model")))
    g = p.grid(ghost_x=5.0, ghost_y=34.0, density_spread=4.0, frame_players=pd.DataFrame({"x": [1.0], "y": [2.0]}))
    assert g.source == "stored-fallback"


def test_model_provider_without_frame_is_stored_fallback(monkeypatch):
    # A2: adapters are PURE — no frame passed in means the model CANNOT run; loud fallback.
    monkeypatch.setenv("LL_GHOST_GRID", "model")
    p = resolve_provider(model_loader=lambda: object())
    g = p.grid(ghost_x=5.0, ghost_y=34.0, density_spread=4.0, frame_players=None)
    assert g.source == "stored-fallback"
