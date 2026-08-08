"""Tests for MemoryHook (ADR-074) — driver memory at the LifecycleHook seam."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from ingestion.memory_hook import MemoryHook

_GB = 1024**3


def _ctx(workflow_id: str = "wf-publish-spadl-vaep") -> MagicMock:
    ctx = MagicMock()
    ctx.workflow_id = workflow_id
    return ctx


def test_logs_memory_on_complete_with_delta_across_the_workflow() -> None:
    logger = MagicMock()
    probes = iter([2 * _GB, 7 * _GB])
    hook = MemoryHook(logger=logger, peak_probe=lambda: next(probes), current_probe=lambda: 5 * _GB)

    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 9_756_155)

    logged = " ".join(str(c) for c in logger.info.call_args_list)
    assert "wf-publish-spadl-vaep" in logged
    assert "+5.00 GB" in logged


def test_logs_memory_on_error_too() -> None:
    """The workflow that FAILED is the one whose memory matters most."""
    logger = MagicMock()
    probes = iter([2 * _GB, 15 * _GB])
    hook = MemoryHook(logger=logger, peak_probe=lambda: next(probes), current_probe=lambda: 15 * _GB)

    ctx = _ctx("wf-hf-sync")
    hook.on_start(ctx)
    hook.on_error(ctx, RuntimeError("boom"))

    logged = " ".join(str(c) for c in logger.info.call_args_list)
    assert "wf-hf-sync" in logged
    assert "13.00 GB" in logged


def test_unsupported_platform_is_silent_not_noisy() -> None:
    """On Windows both probes return None; one useless line per workflow helps nobody."""
    logger = MagicMock()
    hook = MemoryHook(logger=logger, peak_probe=lambda: None, current_probe=lambda: None)
    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 1)
    assert logger.info.call_count == 0


def test_on_skip_emits_nothing() -> None:
    """A skipped workflow consumed nothing; a line would be noise.

    NOTE THE ARITY: the runner dispatches ``on_skip(ctx, reason)`` — two args
    (``workflows/runner.py``: ``_dispatch(active_hooks, "on_skip", ctx, str(exc))``).
    Calling it with one arg here would shape the test to a WRONG implementation and
    pass while production emitted a TypeError traceback on every skip.
    """
    logger = MagicMock()
    hook = MemoryHook(logger=logger, peak_probe=lambda: 1 * _GB, current_probe=lambda: 1 * _GB)
    hook.on_skip(_ctx(), "No HF sync work")
    assert logger.info.call_count == 0


def test_on_skip_releases_the_start_sample() -> None:
    """A skip must not leave a retained reference in the module whose job is finding them."""
    hook = MemoryHook(logger=MagicMock(), peak_probe=lambda: 1 * _GB, current_probe=lambda: 1 * _GB)
    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_skip(ctx, "nothing to do")
    assert hook._start_peak == {}


def test_registered_hooks_match_the_lifecycle_protocol() -> None:
    """Every registered hook's signatures must match LifecycleHook.

    Structural typing gives NO compile-time check here: nothing declares MemoryHook as
    a LifecycleHook, so pyright cannot flag an arity drift. ``_dispatch`` swallows the
    resulting TypeError but logs it at ERROR **with a traceback** — which would bury the
    per-workflow memory lines this whole cycle exists to read. This test is the price of
    the Protocol seam.

    Compares ARITY, not parameter names: ``_dispatch`` calls positionally, so a hook
    using ``context`` instead of ``ctx`` violates nothing, and a gate with false
    positives is a gate that eventually gets deleted.
    """
    from ingestion.cost_hook import CostEstimateHook
    from workflows.hooks import LifecycleHook

    for hook_cls in (CostEstimateHook, MemoryHook):
        for name in ("on_start", "on_complete", "on_skip", "on_error"):
            expected = list(inspect.signature(getattr(LifecycleHook, name)).parameters)
            actual = list(inspect.signature(getattr(hook_cls, name)).parameters)
            assert len(actual) == len(expected), (
                f"{hook_cls.__name__}.{name} takes {len(actual)} params, protocol takes "
                f"{len(expected)}: {actual} vs {expected}"
            )


def test_hook_never_raises_into_the_workflow() -> None:
    """Observability must not be able to fail a pipeline.

    The probe raises ValueError — NOT OSError — deliberately: an earlier draft caught
    only OSError while its docstring promised "never", and its test raised OSError, so
    the test agreed with the code instead of the claim.
    """
    logger = MagicMock()

    def _boom() -> int | None:
        raise ValueError("malformed /proc field")

    hook = MemoryHook(logger=logger, peak_probe=_boom, current_probe=_boom)
    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 1)  # must not raise
    logger.error.assert_called()


def test_bootstrap_hooks_registers_the_memory_hook() -> None:
    """The hook is worthless unless bootstrap_hooks actually registers it."""
    from unittest.mock import patch

    with patch("workflows.register_hook") as mock_register:
        from ingestion.bootstrap import bootstrap_hooks

        bootstrap_hooks(MagicMock(), "cat", "schema")

    registered = [type(c.args[0]).__name__ for c in mock_register.call_args_list]
    assert "MemoryHook" in registered
    assert "CostEstimateHook" in registered
