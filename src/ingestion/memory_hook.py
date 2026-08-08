"""Driver-memory LifecycleHook (ADR-074).

Registered by :func:`ingestion.bootstrap.bootstrap_hooks`, so EVERY
``@workflow``-decorated pipeline reports driver memory — not just hf_sync's
sub-operations. That breadth is the point: the 2026-08-07 OOM
(``exit code 137 (SIGKILL)``) is still unexplained, and the publisher being split
into its own task would otherwise become the platform's only blind spot, which is
exactly where a ``.toPandas()``-shaped leak is most likely to land next.

Sibling of :class:`ingestion.cost_hook.CostEstimateHook` — same port, same
registration site. ``LifecycleHook`` is a ``Protocol``, so this implementation lives
in ``ingestion/`` (it imports ``shared.memory``) and ``src/workflows/`` is untouched;
``.importlinter`` forbids ``workflows -> shared``.

DELIBERATE: :meth:`on_skip` emits nothing. A skipped workflow consumed nothing, and
``hf_sync`` already logs ``"Watermark skip: %s"``. Note this differs from a loop-body
probe, which would emit one line per skip.

ARITY IS LOAD-BEARING. ``workflows/runner.py`` dispatches ``on_skip(ctx, str(exc))``
— two arguments. A one-argument version raises ``TypeError`` on EVERY skip;
``_dispatch`` swallows it but logs at ERROR **with a traceback**, burying the very
memory lines this hook exists to produce. Guarded by
``test_registered_hooks_match_the_lifecycle_protocol``, because structural typing
gives no compile-time check: nothing declares this class as a ``LifecycleHook``.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.memory import RssProbe, current_rss_bytes, format_memory, peak_rss_bytes, sample_memory


class MemoryHook:
    """Report driver peak/resident memory around every workflow."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        peak_probe: RssProbe = peak_rss_bytes,
        current_probe: RssProbe = current_rss_bytes,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._peak_probe = peak_probe
        self._current_probe = current_probe
        self._start_peak: dict[str, int | None] = {}

    def _safe(self, probe: RssProbe) -> int | None:
        """Run a probe without ever propagating.

        ``except Exception`` is deliberate and BROAD: the class docstring promises
        telemetry cannot fail the work it observes, and a narrow tuple would not keep
        that promise (an injected probe can raise anything; ``current_rss_bytes`` can
        raise ``ValueError`` on a malformed ``/proc`` field). ERROR level, never
        warning — ADR-002: a warning-level swallow hid the cost-hook blocker for 62+
        hours. ``run_workflow``'s ``_dispatch`` also wraps hook calls, so this is
        belt-and-braces; it exists so a failure is ATTRIBUTED to the probe rather than
        surfacing as an anonymous hook error.
        """
        try:
            return probe()
        except Exception as exc:
            # Deliberately NOT suppressed with a blind-except waiver: ruff does not treat
            # a handler as blind when it logs the exception with `exc_info=True`. That is
            # not a loophole — it is exactly what ADR-002 requires (raise, typed return,
            # or ERROR-level log), so this catch COMPLIES rather than suppresses.
            self._logger.error("MemoryHook probe failed: %s", exc, exc_info=True)
            return None

    def _report(self, ctx: Any, outcome: str) -> None:
        wid = str(ctx.workflow_id)
        sample = sample_memory(
            wid,
            self._start_peak.pop(wid, None),
            peak_probe=lambda: self._safe(self._peak_probe),
            current_probe=lambda: self._safe(self._current_probe),
        )
        if sample.peak_bytes is None and sample.current_bytes is None:
            return  # unsupported platform — one useless line per workflow helps nobody
        self._logger.info("driver memory %s %s: %s", outcome, wid, format_memory(sample))

    def on_start(self, ctx: Any) -> None:
        self._start_peak[str(ctx.workflow_id)] = self._safe(self._peak_probe)

    def on_complete(self, ctx: Any, row_count: int | None) -> None:
        _ = row_count
        self._report(ctx, "after")

    def on_error(self, ctx: Any, error: Exception) -> None:
        _ = error
        self._report(ctx, "at failure of")

    def on_skip(self, ctx: Any, reason: str) -> None:
        _ = reason
        self._start_peak.pop(str(ctx.workflow_id), None)
