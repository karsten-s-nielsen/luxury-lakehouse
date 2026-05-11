"""PR-LL3 S7 — type-safe NamedTuple wrapper tests for identifiers.py."""

from __future__ import annotations

import pytest

from shared.identifiers import NativeMatchId, NativePlayerId, NativeTeamId


class TestNativeMatchId:
    def test_statsbomb(self) -> None:
        mid = NativeMatchId.statsbomb(3788741)
        assert mid.provider == "statsbomb"
        assert mid.value == "3788741"

    def test_wyscout(self) -> None:
        mid = NativeMatchId.wyscout(2852835)
        assert mid.provider == "wyscout"
        assert mid.value == "2852835"

    def test_idsse(self) -> None:
        mid = NativeMatchId.idsse("J03WMX")
        assert mid.provider == "idsse"
        assert mid.value == "J03WMX"

    def test_idsse_rejects_bad_format(self) -> None:
        with pytest.raises(ValueError):
            NativeMatchId.idsse("bad-format")

    def test_metrica(self) -> None:
        mid = NativeMatchId.metrica("Sample_Game_1")
        assert mid.provider == "metrica"
        assert mid.value == "Sample_Game_1"


class TestNativePlayerId:
    def test_statsbomb(self) -> None:
        pid = NativePlayerId.statsbomb(3009)
        assert pid.provider == "statsbomb"
        assert pid.value == "3009"

    def test_wyscout(self) -> None:
        pid = NativePlayerId.wyscout(25413)
        assert pid.provider == "wyscout"
        assert pid.value == "25413"

    def test_idsse(self) -> None:
        pid = NativePlayerId.idsse("DFL-OBJ-002G1Q")
        assert pid.provider == "idsse"
        assert pid.value == "DFL-OBJ-002G1Q"

    def test_metrica(self) -> None:
        pid = NativePlayerId.metrica("Player11")
        assert pid.provider == "metrica"
        assert pid.value == "Player11"


class TestNativeTeamId:
    def test_statsbomb(self) -> None:
        tid = NativeTeamId.statsbomb(217)
        assert tid.provider == "statsbomb"
        assert tid.value == "217"

    def test_wyscout(self) -> None:
        tid = NativeTeamId.wyscout(1610)
        assert tid.provider == "wyscout"
        assert tid.value == "1610"

    def test_idsse(self) -> None:
        tid = NativeTeamId.idsse("DFL-CLU-000002")
        assert tid.provider == "idsse"
        assert tid.value == "DFL-CLU-000002"

    def test_metrica(self) -> None:
        tid = NativeTeamId.metrica("Sample_Game_1", "home")
        assert tid.provider == "metrica"
        assert tid.value == "metrica_Sample_Game_1_home"
