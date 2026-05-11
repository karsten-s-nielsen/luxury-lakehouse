"""S8 — verify _SECTION_TO_PERIOD covers extra time and penalty periods."""

from __future__ import annotations


def test_section_to_period_covers_all_dfl_periods() -> None:
    from ingestion.idsse import _SECTION_TO_PERIOD

    expected = {
        "firstHalf": 1,
        "secondHalf": 2,
        "extraTimeFirstHalf": 3,
        "extraTimeSecondHalf": 4,
        "penaltyShootout": 5,
    }
    assert _SECTION_TO_PERIOD == expected
