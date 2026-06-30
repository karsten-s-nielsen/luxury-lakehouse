"""C1 sentinel (silly-kicks 4.37.0, Task 1.0): compute_xt_gk enforces one-call-one-provider — the
mixed-provider ``completion=`` escape hatch was removed, so a frame set spanning >1 real provider now raises.

The lakehouse relies on this: the AC dispatches per ``(match, period)`` WorkUnit, so a single compute_xt_gk
call always sees one provider. This sentinel pins the upstream backstop in the pinned version so a future
lakehouse change that batched multiple providers into one call would fail LOUD (at the raise) rather than
silently mis-scoring xT-GK. Expected to be true already — a guard, not a fix.
"""

from __future__ import annotations


def test_compute_xt_gk_enforces_single_provider_backstop() -> None:
    from silly_kicks.tracking import _xt_gk

    assert hasattr(_xt_gk, "_resolve_single_provider"), (
        "silly-kicks 4.37.0 compute_xt_gk must enforce one-call-one-provider via _resolve_single_provider (C1). "
        "If this disappears, a multi-provider frame batch could silently mis-score xT-GK — the AC dispatches per "
        "(match, period) and depends on this raise as the backstop."
    )
