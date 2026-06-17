from __future__ import annotations

import pandas as pd

from analytics.action_context.ports import FrameSource
from analytics.action_context.work_unit import FrameBundle, WorkUnit


class _FakeFrames:
    def frames(self, wu: WorkUnit) -> FrameBundle:
        return FrameBundle(tier="sb360", frames=pd.DataFrame())


def test_fake_framesource_satisfies_protocol() -> None:
    fs: FrameSource = _FakeFrames()
    assert fs.frames(WorkUnit("statsbomb", "M")).tier == "sb360"
