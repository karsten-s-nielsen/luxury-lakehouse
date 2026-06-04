"""Ghost-GK KDE backend selection policy (domain layer — stdlib only).

Resolved ONCE at the adapter boundary (preflight / oneshot), stamped onto each WorkUnit; the processor
consumes the resolved value. Precedence: explicit per-run flag > per-installation default > fallback.

Raises ``ValueError`` on an unknown value — a pure domain function must not raise the process-control
``SystemExit``; the CLI boundary (``ingestion.action_context._resolve_backend_or_exit``) translates it.
See docs/superpowers/specs/2026-06-03-ac1-ghost-gk-backend-and-period-units-design.md.
"""

from __future__ import annotations

# The 5 ``kde_backend`` values silly-kicks ``add_ghost_gk`` accepts. Exact (raw-grid argmax):
# scipy / vectorized / cpu-numba. Fast-approx: fft (~78% mode-exact, NGP) / fft-cic (~95%, CIC bilinear —
# the AC-1 production default). silly-kicks' own default is "vectorized"; AC-1 overrides to "fft-cic".
GHOST_GK_KDE_BACKENDS: frozenset[str] = frozenset({"scipy", "vectorized", "cpu-numba", "fft", "fft-cic"})

DEFAULT_GHOST_GK_BACKEND = "fft-cic"


def resolve_ghost_gk_backend(explicit: str | None, installation_default: str | None) -> str:
    """Resolve the ghost-GK KDE backend by precedence: explicit > installation default > fallback.

    Empty and whitespace-only strings are treated as "unset" at each level (Databricks job-parameter
    substitution yields "" for an unset ``{{job.parameters.*}}``). Raises ``ValueError`` on an unknown
    value.
    """
    for candidate in (explicit, installation_default, DEFAULT_GHOST_GK_BACKEND):
        val = candidate.strip() if candidate and candidate.strip() else None
        if val is None:
            continue
        if val not in GHOST_GK_KDE_BACKENDS:
            raise ValueError(f"Unknown ghost-GK backend {val!r}. Valid: {sorted(GHOST_GK_KDE_BACKENDS)}")
        return val
    return DEFAULT_GHOST_GK_BACKEND
