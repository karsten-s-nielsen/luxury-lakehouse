"""CI gate: no bronze ``sources.yml`` has drifted from its DESCRIBE snapshot.

The fixer is ``scripts/sync_bronze_sources_yml.py``; this is its ``--check`` half, and both call
the same pure core, so the gate cannot pass on something the fixer would change. Same
arrangement as ``test_terraform_env_dep_parity.py`` over ``sync_tf_env_pins.py``.

Covers **every snapshotted provider**, not just the ones a given cycle happens to edit. Scoping
a gate to the files being changed leaves the class open everywhere else — which is how
``visibility``/``access_tier`` went undocumented on Gradient Sports while the identical rule was
enforced two directories away.
"""

from __future__ import annotations

import pytest

from scripts._bronze_table_inventory import PROVIDERS
from scripts.sync_bronze_sources_yml import SNAPSHOTTED_PROVIDERS, drift


@pytest.mark.parametrize("provider", SNAPSHOTTED_PROVIDERS)
def test_sources_yml_matches_the_snapshot(provider: str) -> None:
    """Every column in the snapshot is documented in sources.yml."""
    pending = drift(provider)
    assert not pending, (
        f"{provider}: {sum(len(v) for v in pending.values())} undocumented column(s) in "
        f"{ {t: len(c) for t, c in pending.items()} }. "
        "Run: uv run python scripts/sync_bronze_sources_yml.py"
    )


def test_snapshotted_set_is_pinned_and_explained() -> None:
    """A provider silently dropped from the gate is a provider silently unguarded.

    Metrica is the one deliberate omission: it is CSV/EPTS files rather than Delta tables, so its
    coverage test reads a header enumeration instead of a DESCRIBE snapshot. Asserting the exact
    set — rather than a count — means adding OR removing a provider is a visible, reviewed change.
    """
    assert set(SNAPSHOTTED_PROVIDERS) == set(PROVIDERS) - {"metrica"}


def test_the_gate_is_not_vacuous() -> None:
    """A parametrised gate with zero cases passes while asserting nothing."""
    assert len(SNAPSHOTTED_PROVIDERS) == 5
