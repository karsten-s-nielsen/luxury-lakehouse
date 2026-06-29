"""Invariant: MatchInfo.visibility is a REQUIRED field with no default (plan R3).

This is the technical lynchpin that keeps ``classify_access_tier(skillcorner, None) -> PUBLIC``
unreachable in ingestion: a pining response missing ``visibility`` must hard-error (pydantic), never
silently become ``None -> public``. The test fails if anyone later adds a convenience default.
"""

from __future__ import annotations

import pytest

from ingestion.gradientsports_common import MatchInfo as GsMatchInfo
from ingestion.skillcorner_common import MatchInfo as ScMatchInfo


@pytest.mark.parametrize("model", [ScMatchInfo, GsMatchInfo])
def test_visibility_is_required_no_default(model: type) -> None:
    field = model.model_fields["visibility"]
    assert field.is_required(), (
        f"{model.__module__}.MatchInfo.visibility must stay REQUIRED (no default) — a silent default "
        "re-opens the skillcorner+None->public leak (plan R3)"
    )
