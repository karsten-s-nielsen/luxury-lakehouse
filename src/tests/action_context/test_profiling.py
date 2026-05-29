from __future__ import annotations

import time

from analytics.action_context.profiling import StepTiming, profile_callable


def test_profile_callable_returns_total_and_ranked_timings() -> None:
    def _work() -> None:
        time.sleep(0.01)
        sum(range(10000))

    total, timings = profile_callable(_work, top=5)
    assert total >= 0.0
    assert timings
    assert all(isinstance(t, StepTiming) for t in timings)
    # ranked descending by cumulative time
    assert all(timings[i].cumulative_s >= timings[i + 1].cumulative_s for i in range(len(timings) - 1))
