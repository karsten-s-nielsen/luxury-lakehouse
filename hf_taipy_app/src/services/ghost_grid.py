"""Ghost-GK density grid service (hexagonal port; ADR-051 section 3).

Port: GhostGridProvider.grid(...) -> GhostGrid. Adapters are PURE computation — the STATE layer
fetches the frame from Lakebase (queries.gk_tracking.fetch_scene_frame) and passes it in
(architecture-audit A2: no service-side I/O; both adapters unit-testable without a DB).

Adapters:
- StoredSpreadProvider: Gaussian blob from the stored optimum + density_spread. Always works.
- ModelGridProvider: true silly-kicks conditional-density grid. v1 ships the SHELL only — the
  render is implemented in the fast-follow PR once silly-kicks exposes a PUBLIC loader
  entrypoint (spec section 9 resolution 3). In model mode, ANY failure (including the v1
  not-implemented state) degrades LOUDLY to a stored-fallback grid: ERROR log + `source`
  carried to the on-chart caption — never silent substitution (ADR-002 telemetry rule).
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

_GRID_X, _GRID_Y = 64, 60  # matches the ghost-gk-v1 grid shape


@dataclass(frozen=True)
class GhostGrid:
    xs: np.ndarray  # (64,) pitch x in [0, 36] (defensive third, canonical)
    ys: np.ndarray  # (60,) pitch y in [0, 68]
    z: np.ndarray  # (60, 64) density
    source: str  # 'stored' | 'model' | 'stored-fallback'


class GhostGridProvider(Protocol):
    """A2: adapters are PURE — the caller supplies frame data; no adapter performs I/O."""

    def grid(
        self, *, ghost_x: float, ghost_y: float, density_spread: float, frame_players: pd.DataFrame | None
    ) -> GhostGrid: ...


def _stored_grid(ghost_x: float, ghost_y: float, density_spread: float, source: str) -> GhostGrid:
    xs = np.linspace(0.0, 36.0, _GRID_X)
    ys = np.linspace(0.0, 68.0, _GRID_Y)
    gx, gy = np.meshgrid(xs, ys)
    sigma = float(np.clip(np.sqrt(max(density_spread, 1e-6)) / 3.0, 1.5, 6.0))
    z = np.exp(-(((gx - ghost_x) / sigma) ** 2 + ((gy - ghost_y) / (sigma * 1.15)) ** 2))
    return GhostGrid(xs=xs, ys=ys, z=z, source=source)


class StoredSpreadProvider:
    """Blob from the stored optimum + spread — always available, no model, no I/O."""

    def grid(
        self, *, ghost_x: float, ghost_y: float, density_spread: float, frame_players: pd.DataFrame | None = None
    ) -> GhostGrid:
        return _stored_grid(ghost_x, ghost_y, density_spread, "stored")


class ModelGridProvider:
    """Renders the true model grid from a CALLER-SUPPLIED frame (pure — no I/O here);
    degrades to stored on ANY failure (ERROR log). Grids are memoized per call key (S3)."""

    def __init__(self, model_loader: Callable[[], object]) -> None:
        self._loader = model_loader
        self._model: object | None = None
        self._cache: dict[tuple, GhostGrid] = {}

    def grid(
        self, *, ghost_x: float, ghost_y: float, density_spread: float, frame_players: pd.DataFrame | None = None
    ) -> GhostGrid:
        key = (round(ghost_x, 3), round(ghost_y, 3), len(frame_players) if frame_players is not None else -1)
        if key in self._cache:
            return self._cache[key]
        t0 = time.perf_counter()
        try:
            if frame_players is None or frame_players.empty:
                raise RuntimeError("no frame rows supplied")
            if self._model is None:
                self._model = self._loader()
            z, xs, ys = self._render(frame_players)
            result = GhostGrid(xs=xs, ys=ys, z=z, source="model")
        except Exception:
            logger.exception("ghost model grid failed — stored fallback")
            result = _stored_grid(ghost_x, ghost_y, density_spread, "stored-fallback")
        # O1: the render sits on the user path (<=500 ms cached-interaction budget)
        logger.info(
            "ghost grid rendered: source=%s cache=miss duration_ms=%d",
            result.source,
            int((time.perf_counter() - t0) * 1000),
        )
        self._cache[key] = result
        return result

    def _render(self, frame_players: pd.DataFrame) -> tuple:
        # Fast-follow (spec section 9 resolution 3): implemented against a PUBLIC silly-kicks
        # loader entrypoint once it exists — the current upstream symbol is private and is not
        # a dependable contract. Until then this raises and the loud fallback above serves.
        raise NotImplementedError("ModelGridProvider render lands in the fast-follow PR (ADR-051)")


def resolve_provider(model_loader: Callable[[], object] | None = None) -> GhostGridProvider:
    """Env-selected adapter: LL_GHOST_GRID=model -> ModelGridProvider, else StoredSpreadProvider."""
    if os.environ.get("LL_GHOST_GRID") == "model":
        if model_loader is None:
            # v1: no public silly-kicks loader yet — a loader that fails loudly keeps the
            # fallback path (and its ERROR log) as the single degradation route.
            def _no_public_loader() -> object:
                raise RuntimeError("no public ghost-GK loader available yet (fast-follow, ADR-051)")

            model_loader = _no_public_loader

        return ModelGridProvider(model_loader)
    return StoredSpreadProvider()
