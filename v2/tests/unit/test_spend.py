"""The spend model: `run_cost` over the execution context and HF-Job flavour (spec §7.3; plan Task 3)."""

from __future__ import annotations

from lakehouse.observability import run_cost


def test_spend_contexts() -> None:
    assert run_cost("local", None, 1.0) == (0.0, None)
    assert run_cost("hf_job", "cpu-basic", 2.0) == (0.02, None)
    assert run_cost("hf_job", "cpu-upgrade", 1.0) == (0.03, None)

    cost, err = run_cost("hf_job", "a100-large", 1.0)  # unknown flavour
    assert cost is None and err is not None and "unknown flavour" in err

    cost, err = run_cost("hf_job", None, 1.0)  # missing flavour
    assert cost is None and err is not None and "flavour" in err

    cost, err = run_cost(None, None, 1.0)  # missing execution_context -> never a silent 0, never local
    assert cost is None and err is not None and "execution_context" in err

    cost, err = run_cost("web_app", None, 1.0)  # an unexpected context is not local either
    assert cost is None and err is not None
