"""StatsBomb field-inventory contract (spec §12.2). The repository holds synthetic fixtures only (licence NOASSERTION,
§23.4); their field paths are those observed in the open-data sample. The live tier is the proof of completeness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse.errors import UnmappedFieldError
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.statsbomb import StatsBombParser

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "statsbomb"

# Reuse audit (spec §12.6): IRIS `providers/statsbomb.py` keeps from the events only the team identity
# (`events_identity`: match, team id and name) and from the lineups only the first position and card counts; v1
# `src/ingestion/statsbomb.py` flattens events through statsbombpy and keeps every type-specific object only as a JSON
# string (`raw_extra_json`), which §12.1 forbids. v2 keeps every field below as its own typed column.
# The raw fields of the open-data sample read on 2026-10-27 (3 matches per default season; spec §12.2).
EXPECTED_FIELDS: dict[str, frozenset[str]] = {
    "competitions": frozenset(
        {
            "competition_gender",
            "competition_id",
            "competition_international",
            "competition_name",
            "competition_youth",
            "country_name",
            "match_available",
            "match_available_360",
            "match_updated",
            "match_updated_360",
            "season_id",
            "season_name",
        }
    ),
    "matches": frozenset(
        {
            "away_score",
            "away_team.away_team_gender",
            "away_team.away_team_group",
            "away_team.away_team_id",
            "away_team.away_team_name",
            "away_team.country.id",
            "away_team.country.name",
            "away_team.managers[].country.id",
            "away_team.managers[].country.name",
            "away_team.managers[].dob",
            "away_team.managers[].id",
            "away_team.managers[].name",
            "away_team.managers[].nickname",
            "competition.competition_id",
            "competition.competition_name",
            "competition.country_name",
            "competition_stage.id",
            "competition_stage.name",
            "home_score",
            "home_team.country.id",
            "home_team.country.name",
            "home_team.home_team_gender",
            "home_team.home_team_group",
            "home_team.home_team_id",
            "home_team.home_team_name",
            "home_team.managers[].country.id",
            "home_team.managers[].country.name",
            "home_team.managers[].dob",
            "home_team.managers[].id",
            "home_team.managers[].name",
            "home_team.managers[].nickname",
            "kick_off",
            "last_updated",
            "last_updated_360",
            "match_date",
            "match_id",
            "match_status",
            "match_status_360",
            "match_week",
            "metadata.data_version",
            "metadata.shot_fidelity_version",
            "metadata.xy_fidelity_version",
            "referee.country.id",
            "referee.country.name",
            "referee.id",
            "referee.name",
            "season.season_id",
            "season.season_name",
            "stadium.country.id",
            "stadium.country.name",
            "stadium.id",
            "stadium.name",
        }
    ),
    "events": frozenset(
        {
            "50_50.outcome.id",
            "50_50.outcome.name",
            "bad_behaviour.card.id",
            "bad_behaviour.card.name",
            "ball_receipt.outcome.id",
            "ball_receipt.outcome.name",
            "ball_recovery.offensive",
            "ball_recovery.recovery_failure",
            "block.deflection",
            "block.offensive",
            "carry.end_location",
            "clearance.aerial_won",
            "clearance.body_part.id",
            "clearance.body_part.name",
            "clearance.head",
            "clearance.left_foot",
            "clearance.other",
            "clearance.right_foot",
            "counterpress",
            "dribble.no_touch",
            "dribble.nutmeg",
            "dribble.outcome.id",
            "dribble.outcome.name",
            "dribble.overrun",
            "duel.outcome.id",
            "duel.outcome.name",
            "duel.type.id",
            "duel.type.name",
            "duration",
            "foul_committed.advantage",
            "foul_committed.card.id",
            "foul_committed.card.name",
            "foul_committed.offensive",
            "foul_committed.penalty",
            "foul_committed.type.id",
            "foul_committed.type.name",
            "foul_won.advantage",
            "foul_won.defensive",
            "foul_won.penalty",
            "goalkeeper.body_part.id",
            "goalkeeper.body_part.name",
            "goalkeeper.end_location",
            "goalkeeper.outcome.id",
            "goalkeeper.outcome.name",
            "goalkeeper.position.id",
            "goalkeeper.position.name",
            "goalkeeper.shot_saved_to_post",
            "goalkeeper.success_out",  # the 291 redistributed open-data matches (pining owner tier)
            "goalkeeper.technique.id",
            "goalkeeper.technique.name",
            "goalkeeper.type.id",
            "goalkeeper.type.name",
            "id",
            "index",
            "injury_stoppage.in_chain",
            "interception.outcome.id",
            "interception.outcome.name",
            "location",
            "minute",
            "miscontrol.aerial_won",
            "off_camera",
            "out",
            "pass.aerial_won",
            "pass.angle",
            "pass.assisted_shot_id",
            "pass.backheel",
            "pass.body_part.id",
            "pass.body_part.name",
            "pass.cross",
            "pass.cut_back",
            "pass.deflected",
            "pass.end_location",
            "pass.goal_assist",
            "pass.height.id",
            "pass.height.name",
            "pass.inswinging",
            "pass.length",
            "pass.miscommunication",
            "pass.no_touch",
            "pass.outcome.id",
            "pass.outcome.name",
            "pass.outswinging",
            "pass.recipient.id",
            "pass.recipient.name",
            "pass.shot_assist",
            "pass.straight",
            "pass.switch",
            "pass.technique.id",
            "pass.technique.name",
            "pass.through_ball",
            "pass.type.id",
            "pass.type.name",
            "period",
            "play_pattern.id",
            "play_pattern.name",
            "player.id",
            "player.name",
            "position.id",
            "position.name",
            "possession",
            "possession_team.id",
            "possession_team.name",
            "related_events",
            "second",
            "shot.aerial_won",
            "shot.body_part.id",
            "shot.body_part.name",
            "shot.deflected",
            "shot.end_location",
            "shot.first_time",
            "shot.freeze_frame[].location",
            "shot.freeze_frame[].player.id",
            "shot.freeze_frame[].player.name",
            "shot.freeze_frame[].position.id",
            "shot.freeze_frame[].position.name",
            "shot.freeze_frame[].teammate",
            "shot.key_pass_id",
            "shot.one_on_one",
            "shot.open_goal",
            "shot.outcome.id",
            "shot.outcome.name",
            "shot.redirect",
            "shot.saved_to_post",
            "shot.statsbomb_xg",
            "shot.technique.id",
            "shot.technique.name",
            "shot.type.id",
            "shot.type.name",
            "substitution.outcome.id",
            "substitution.outcome.name",
            "substitution.replacement.id",
            "substitution.replacement.name",
            "tactics.formation",
            "tactics.lineup[].jersey_number",
            "tactics.lineup[].player.id",
            "tactics.lineup[].player.name",
            "tactics.lineup[].position.id",
            "tactics.lineup[].position.name",
            "team.id",
            "team.name",
            "timestamp",
            "type.id",
            "type.name",
            "under_pressure",
        }
    ),
    "lineups": frozenset(
        {
            "lineup[].cards[].card_type",
            "lineup[].cards[].period",
            "lineup[].cards[].reason",
            "lineup[].cards[].time",
            "lineup[].country.id",
            "lineup[].country.name",
            "lineup[].jersey_number",
            "lineup[].player_id",
            "lineup[].player_name",
            "lineup[].player_nickname",
            "lineup[].positions[].end_reason",
            "lineup[].positions[].from",
            "lineup[].positions[].from_period",
            "lineup[].positions[].position",
            "lineup[].positions[].position_id",
            "lineup[].positions[].start_reason",
            "lineup[].positions[].to",
            "lineup[].positions[].to_period",
            "team_id",
            "team_name",
        }
    ),
    "three_sixty": frozenset(
        {
            "event_uuid",
            "freeze_frame[].actor",
            "freeze_frame[].keeper",
            "freeze_frame[].location",
            "freeze_frame[].teammate",
            "visible_area",
        }
    ),
}


ARTIFACTS = {
    "events": "events_9100001.json",
    "lineups": "lineups_9100001.json",
    "three_sixty": "three_sixty_9100001.json",
    "matches": "matches_43_106.json",
    "competitions": "competitions.json",
}


@pytest.mark.parametrize(("artifact", "name"), sorted(ARTIFACTS.items()))
def test_field_inventory_complete(artifact: str, name: str) -> None:
    parser = StatsBombParser()
    inventory = parser.field_inventory(artifact, (FIXTURES / name).read_bytes())
    assert inventory
    assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == []


@pytest.mark.parametrize(("artifact", "name"), sorted(ARTIFACTS.items()))
def test_unmapped_field_fails_loudly(artifact: str, name: str) -> None:
    records = json.loads((FIXTURES / name).read_bytes())
    records[0]["a_new_field_from_statsbomb"] = 1
    with pytest.raises(UnmappedFieldError) as caught:
        StatsBombParser().table(artifact, json.dumps(records).encode())
    assert caught.value.field_paths == ["a_new_field_from_statsbomb"]


def test_nested_unmapped_field_named() -> None:
    events = json.loads((FIXTURES / "events_9100001.json").read_bytes())
    shot = next(e for e in events if "shot" in e)
    shot["shot"]["freeze_frame"][0]["new_flag"] = True
    with pytest.raises(UnmappedFieldError) as caught:
        StatsBombParser().table("events", json.dumps(events).encode())
    assert caught.value.field_paths == ["shot.freeze_frame[].new_flag"]


@pytest.mark.parametrize("artifact", sorted(ARTIFACTS))
def test_columns_unique(artifact: str) -> None:
    table = StatsBombParser().table(artifact, (FIXTURES / ARTIFACTS[artifact]).read_bytes())
    assert len(set(table.column_names)) == len(table.column_names)


@pytest.mark.parametrize("artifact", sorted(ARTIFACTS))
def test_expected_fields_mapped(artifact: str) -> None:
    """Every raw field of the real sample has a bronze column."""
    assert unmapped_fields(EXPECTED_FIELDS[artifact], StatsBombParser().bronze_columns(artifact)) == []


# The pining owner tier (spec §7.2), measured on 2026-10-27 with the owner token: every artifact of the 36 commercial
# matches (provenance `original`) and the 291 redistributed open-data matches. These are the fields the open-data
# sample lacks; the commercial fixtures (`commercial_*_9100001.json`, synthetic values) hold every one of them.
OWNER_EXTRA_FIELDS: dict[str, frozenset[str]] = {
    "events": frozenset(
        {
            *(f"obv_{side}_{part}" for side in ("for", "against") for part in ("after", "before", "net")),
            "obv_total_net",
            "pass.pass_cluster_id",
            "pass.pass_cluster_label",
            "pass.pass_cluster_probability",
            "pass.pass_success_probability",
            "pass.xclaim",
            "shot.gk_positioning_xg_suppression",
            "shot.gk_save_difficulty_xg",
            "shot.gk_shot_stopping_xg_suppression",
            "shot.shot_execution_xg",
            "shot.shot_execution_xg_uplift",
            "shot.shot_goal_assist",
            "shot.shot_shot_assist",
        }
    ),
    "lineups": frozenset(
        {
            *(f"events[].{k}" for k in ("outcome", "period", "player_id", "player_name", "timestamp", "type")),
            *(f"formations[].{k}" for k in ("formation", "period", "reason", "timestamp")),
            "lineup[].birth_date",
            "lineup[].player_gender",
            "lineup[].player_height",
            "lineup[].player_weight",
            "lineup[].positions[].counterpart_id",
            "lineup[].positions[].counterpart_name",
            "lineup[].stats",
            *(
                f"lineup[].stats.{k}"
                for k in ("assists", "goals", "own_goals", "penalties_missed", "penalties_saved", "penalties_scored")
            ),
        }
    ),
    "three_sixty": frozenset(
        {
            "ball_receipt_exceeds_distance",
            "ball_receipt_in_space",
            "distance_to_nearest_defender",
            "distances_from_edge_of_visible_area",
            "distances_from_edge_of_visible_area[].distance",
            "distances_from_edge_of_visible_area[].point_id",
            "line_breaking_pass",
            "num_defenders_on_goal_side_of_actor",
            "visible_player_counts",
            "visible_player_counts[].count",
            "visible_player_counts[].team_id",
        }
    ),
    # The per-match metadata object: the match fields, nulls in place of some objects, and five credentialed fields.
    "metadata": frozenset(
        {
            *EXPECTED_FIELDS["matches"],
            "home_team.country",
            "home_team.managers[].country",
            "referee.country",
            "stadium.country",
            "attendance",
            "behind_closed_doors",
            "neutral_ground",
            "collection_status",
            "play_status",
        }
    ),
}
COMMERCIAL = {
    "events": "commercial_events_9100001.json",
    "lineups": "commercial_lineups_9100001.json",
    "three_sixty": "commercial_three_sixty_9100001.json",
    "metadata": "commercial_metadata_9100001.json",
}


@pytest.mark.parametrize("artifact", sorted(COMMERCIAL))
def test_owner_tier_fields_mapped(artifact: str) -> None:
    parser = StatsBombParser()
    assert unmapped_fields(OWNER_EXTRA_FIELDS[artifact], parser.bronze_columns(artifact)) == []


@pytest.mark.parametrize("artifact", sorted(COMMERCIAL))
def test_commercial_fixture_holds_the_owner_fields(artifact: str) -> None:
    inventory = StatsBombParser().field_inventory(artifact, (FIXTURES / COMMERCIAL[artifact]).read_bytes())
    missing = OWNER_EXTRA_FIELDS[artifact] - inventory
    if artifact == "metadata":
        missing -= EXPECTED_FIELDS["matches"] - inventory  # open fields the synthetic object leaves out
    assert missing == set()
