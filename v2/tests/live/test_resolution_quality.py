"""Resolution quality against the truth set (marker `live`; spec §15.4, §29.3; plan Tasks 22 and 34).

Per entity type: precision of at least 0.99 for links in the automatic band and recall of at least 0.95 after queue
review. The truth set holds ids only (`tests/fixtures/truthset/`); names come from a live lake, named by
`LAKEHOUSE_LAKE_ROOT`. Until the owner's labels exist (Task 34, Gate G-E) every case skips with a visible reason.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from lakehouse.catalog.reader import CatalogReader
from lakehouse.resolve.model import ENTITIES, PLURALS, Entity
from lakehouse.resolve.truthset import measure, read_truthset, truthset_file, validate_truthset
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import LAKE_ROOT_ENV, default_bootstrap_file, resolve_roots

pytestmark = pytest.mark.live

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "truthset"
PRECISION_TARGET = 0.99
RECALL_TARGET = 0.95


@pytest.mark.parametrize("entity", ENTITIES)
def test_resolution_quality(entity: Entity) -> None:
    rows = read_truthset(FIXTURES / truthset_file(entity))
    if not rows:
        pytest.skip(f"no truth-set labels for {PLURALS[entity]} yet: plan Task 34 (Gate G-E) creates them")
    owner = os.environ.get("LAKEHOUSE_TRUTHSET_OWNER")
    validate_truthset(rows, owner=owner)
    if not os.environ.get(LAKE_ROOT_ENV):
        pytest.skip(f"set {LAKE_ROOT_ENV} to a live lake: the quality test reads names and links from it")
    roots = resolve_roots(
        cli_lake=None, cli_restricted=None, env=os.environ, bootstrap_file=default_bootstrap_file(os.environ)
    )
    with CatalogReader(LocalStorage(roots.roots)).session() as conn:
        report = measure(conn, rows, entity)
    assert report.auto_band_precision is None or report.auto_band_precision >= PRECISION_TARGET
    assert report.recall is not None and report.recall >= RECALL_TARGET
