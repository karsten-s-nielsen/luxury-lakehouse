"""Unit tests for SkillCorner events ingestion."""

from __future__ import annotations

import io

from ingestion.skillcorner_events import parse_events_csv


class TestParseEventsCsv:
    def test_basic_parse(self) -> None:
        csv_content = (
            "event_id,event_type,event_subtype,player_id,team_id,period,"
            "time_start,time_end,x_start,y_start,x_end,y_end,"
            "game_interruption_before,game_interruption_after,end_type,start_type\n"
            "1_0,pass,short_pass,38673,4177,1,"
            "00:01.2,00:03.4,10.5,-5.2,20.1,3.4,"
            ",,successful,\n"
        )
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")

        assert len(df) == 1
        assert df["match_id"].iloc[0] == "1886347"
        assert df["event_id"].iloc[0] == "1_0"
        assert df["player_id"].iloc[0] == 38673
        assert df["team_id"].iloc[0] == 4177
        assert "_ingested_at" in df.columns

    def test_match_id_is_raw_native(self) -> None:
        """match_id must be raw native (e.g. '1886347'), not prefixed."""
        csv_content = "event_id,event_type,player_id,team_id,period\n1_0,pass,38673,4177,1\n"
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        assert df["match_id"].iloc[0] == "1886347"
        assert not df["match_id"].iloc[0].startswith("skillcorner_")

    def test_ingested_at_is_utc(self) -> None:
        csv_content = "event_id,event_type,player_id,team_id,period\n1_0,pass,38673,4177,1\n"
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        ts = df["_ingested_at"].iloc[0]
        assert ts.tzinfo is not None

    def test_all_294_columns_preserved(self) -> None:
        """Bronze-completeness: all source columns plus match_id + _ingested_at."""
        cols = [f"col_{i}" for i in range(294)]
        header = ",".join(cols)
        values = ",".join(["x"] * 294)
        csv_content = f"{header}\n{values}\n"
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        # Source columns + match_id + _ingested_at
        assert len(df.columns) == 296
