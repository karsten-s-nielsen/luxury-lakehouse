"""DFL event-XML attribute schema — source of truth for IDSSE bronze coverage.

Every attribute that can appear on a DFL ``<Event>`` element, its first-child
element, or a nested grandchild, across all 7 IDSSE match XMLs enumerated
2026-04-21. The parser in :mod:`ingestion.idsse` uses these constants to
pre-declare every bronze column so the pandas→Arrow→Spark conversion never
drops a sparsely-populated column as ``NullType``.

Parity with ``src/tests/fixtures/idsse_dfl_event_attr_enumeration.json`` is
asserted by ``test_idsse_bronze_coverage.TestSchemaFixtureParity`` — when the
fixture is regenerated from a new DFL snapshot, update this module in lockstep.
"""

from __future__ import annotations

SCHEMA_VERSION = "dfl_2026_04_21_2level"
"""Matches the ``schema_version`` key in the JSON fixture."""

# ---------------------------------------------------------------------------
# <Event>-level attributes
# ---------------------------------------------------------------------------

EVENT_LEVEL_ATTRS: tuple[str, ...] = (
    "CalculatedFrame",
    "CalculatedTimestamp",
    "EndFrame",
    "EventId",
    "EventTime",
    "MatchId",
    "StartFrame",
    "X-Position",
    "X-PositionFromTracking",
    "X-Source-Position",
    "Y-Position",
    "Y-PositionFromTracking",
    "Y-Source-Position",
)

# ---------------------------------------------------------------------------
# First-child tag → its attribute tuple
# ---------------------------------------------------------------------------

FIRST_CHILD_ATTRS: dict[str, tuple[str, ...]] = {
    "BallClaiming": ("BallPossessionPhase", "Player", "Team", "Type"),
    "BallDeflection": ("Player", "Team", "Type"),
    "Caution": (
        "CardColor",
        "CardRating",
        "OtherReason",
        "Player",
        "Reason",
        "RefDecisionEvaluation",
        "Team",
    ),
    "CautionTeamofficial": ("CardColor", "PersonSentOff", "Team"),
    "ChanceWithoutShot": (
        "AssistAction",
        "ChanceAssist",
        "ChanceAssistType",
        "CounterAttack",
        "Player",
        "PreventionGoalkeeper",
        "SetupOrigin",
        "Sitter",
        "Situation",
        "TakerSetup",
        "Team",
    ),
    "CornerKick": (
        "DecisionTimestamp",
        "Placing",
        "PostMarking",
        "Rotation",
        "Side",
        "TargetArea",
        "Team",
    ),
    "Delete": ("Reason",),
    "FairPlay": ("BallPossessionPhase", "Player", "Team"),
    "FinalWhistle": ("BreakingOff", "FinalResult", "GameSection"),
    "Foul": (
        "CommittingPlayerAction",
        "FoulType",
        "Fouled",
        "Fouler",
        "TeamFouled",
        "TeamFouler",
    ),
    "FreeKick": ("DecisionTimestamp", "ExecutionMode", "Team"),
    "GoalDisallowed": ("Player", "Reason", "RefDecisionEvaluation", "Team"),
    "GoalKick": ("DecisionTimestamp", "Team"),
    "KickOff": ("GameSection", "TeamLeft", "TeamRight"),
    "Nutmeg": ("AffectedPlayer", "AffectedTeam", "Player", "Team"),
    "Offside": ("Player", "Team"),
    "OtherBallAction": (
        "BallPossessionPhase",
        "DefensiveClearance",
        "Player",
        "Team",
    ),
    "OtherPlayerAction": (
        "ChangeContingentExhausted",
        "ChangeOfCaptain",
        "Player",
        "PlayerBecomesGoalkeeper",
        "Team",
    ),
    "Penalty": (
        "CausingPlayer",
        "DecisionTimestamp",
        "FouledPlayer",
        "GoalkeeperBehaviour",
        "GoalkeeperMovement",
        "PlayersInBox",
        "ProspectiveTaker",
        "RefDecisionEvaluation",
        "RetakenPenalty",
        "Team",
    ),
    "PenaltyNotAwarded": (
        "CausingPlayer",
        "PlayerToBeAwarded",
        "Reason",
        "RefDecisionEvaluation",
        "Team",
    ),
    "Play": (
        "BallPossessionPhase",
        "Distance",
        "Evaluation",
        "FlatCross",
        "FromOpenPlay",
        "GoalKeeperAction",
        "Height",
        "PenaltyBox",
        "PlayAngle",
        "PlayOrigin",
        "Player",
        "Recipient",
        "Rotation",
        "SemiField",
        "Team",
    ),
    "PlayerNotSentOff": ("Player", "Reason", "RefDecisionEvaluation", "Team", "Type"),
    "PossessionLossBeforeGoal": (
        "Player",
        "PossessionLossOrigin",
        "Team",
        "TypeOfPossessionLoss",
    ),
    "RefereeBall": (),
    "Run": ("Player", "Team"),
    "ShotAtGoal": (
        "AfterFreeKick",
        "AmountOfDefenders",
        "AngleToGoal",
        "AssistAction",
        "AssistShotAtGoal",
        "AssistTypeShotAtGoal",
        "BallPossessionPhase",
        "BuildUp",
        "ChanceEvaluation",
        "CounterAttack",
        "DirectFreeKickIntention",
        "DistanceToGoal",
        "ExtendedTypeOfShot",
        "GoalDistanceGoalkeeper",
        "InsideBox",
        "PenaltyDirection",
        "PenaltyExecution",
        "Player",
        "PlayerSpeed",
        "Pressure",
        "Rotation",
        "SetupOrigin",
        "ShotAssistFouledPlayer",
        "ShotCondition",
        "ShotContribution",
        "ShotOrigin",
        "SignificanceEvaluation",
        "SitterContribution",
        "TakerBallControl",
        "TakerSetup",
        "Team",
        "TypeOfShot",
        "xG",
    ),
    "SitterPrevented": ("Player", "Reason", "RefDecisionEvaluation", "Team"),
    "SpectacularPlay": ("Player", "Team", "Type"),
    "Substitution": ("PlayerIn", "PlayerOut", "PlayingPosition", "Team"),
    "TacklingGame": (
        "BallPossessionPhase",
        "DribbleEvaluation",
        "DribblingSide",
        "DribblingType",
        "GoalKeeperInvolved",
        "Loser",
        "LoserRole",
        "LoserTeam",
        "PossessionChange",
        "Type",
        "Winner",
        "WinnerAction",
        "WinnerResult",
        "WinnerRole",
        "WinnerTeam",
    ),
    "ThrowIn": ("DecisionTimestamp", "Side", "Team"),
    "VideoAssistantAction": (
        "FinalDecision",
        "Linesman1",
        "Linesman2",
        "OpponentTeam",
        "ProofedEvent",
        "RefDecision",
        "RefDecisionEvaluation",
        "Referee",
        "RefereeinRRA",
        "TeamChallenged",
        "TimestampEndAction",
        "TimestampStartAction",
        "VideoAssistant",
    ),
}

# ---------------------------------------------------------------------------
# Nested children: {parent_first_child_tag: {nested_tag: attrs_tuple}}
# ---------------------------------------------------------------------------

_SHOT_AT_GOAL_ATTRS: tuple[str, ...] = FIRST_CHILD_ATTRS["ShotAtGoal"]
_PLAY_ATTRS: tuple[str, ...] = FIRST_CHILD_ATTRS["Play"]

NESTED_CHILD_ATTRS: dict[str, dict[str, tuple[str, ...]]] = {
    "CornerKick": {"Play": _PLAY_ATTRS},
    "FreeKick": {"Play": _PLAY_ATTRS, "ShotAtGoal": _SHOT_AT_GOAL_ATTRS},
    "GoalKick": {"Play": _PLAY_ATTRS},
    "KickOff": {"Play": _PLAY_ATTRS},
    "Penalty": {"ShotAtGoal": _SHOT_AT_GOAL_ATTRS},
    "Play": {
        "Cross": ("GoalKeeper", "GoalKeeperInterference", "Side"),
        "Pass": ("Direction", "FreeKickLayup", "OneTwo"),
    },
    "ShotAtGoal": {
        "BlockedShot": ("BlockedByOwnTeam", "GoalPrevented", "Player"),
        "OtherShot": (),
        "SavedShot": ("GoalKeeper", "SaveEvaluation", "SaveResult", "SaveType"),
        "ShotWide": ("PitchMarking", "Placing"),
        "ShotWoodWork": ("Location",),
        "SuccessfulShot": (
            "Assist",
            "AssistContribution",
            "AssistFouledPlayer",
            "AssistType",
            "CurrentResult",
            "DeflectionKeeper",
            "DeflectionPlayer",
            "Error",
            "GoalZone",
            "RefDecisionEvaluation",
            "Solo",
        ),
    },
    "ThrowIn": {
        "FairPlay": ("BallPossessionPhase", "Player", "Team"),
        "FaultExecution": ("BallPossessionPhase", "Player", "Team"),
        "Play": _PLAY_ATTRS,
    },
}
