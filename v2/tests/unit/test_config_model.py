"""Config model and install defaults (spec §8, §10.7, §10.9, §29.13)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from lakehouse.config.model import (
    MODULE_IDS,
    AllClause,
    CheckpointConfig,
    CompetitionClause,
    MaintenanceConfig,
    MatchClause,
    ModuleConfig,
    ResourceCaps,
    RetentionConfig,
    ScheduleConfig,
    Settings,
    load_defaults,
)


def _module(settings: Settings, module_id: str) -> ModuleConfig:
    return next(m for m in settings.modules if m.module_id == module_id)


def test_defaults_laptop() -> None:
    s = load_defaults("laptop")
    sb = _module(s, "statsbomb_public")
    assert sb.active
    assert sb.statsbomb_360_only is False
    assert sb.filter == [
        CompetitionClause(competition_id="9", seasons=["281"]),
        CompetitionClause(competition_id="43", seasons=["3"]),
        CompetitionClause(competition_id="43", seasons=["106"]),
    ]
    ws = _module(s, "wyscout_public")
    assert ws.active
    assert ws.filter == [CompetitionClause(competition_id="28", seasons="all")]
    for module_id in ("idsse_public", "skillcorner_public"):
        module = _module(s, module_id)
        assert module.active
        assert module.filter == [AllClause()]
    assert not _module(s, "gradientsports_public").active
    for module_id in MODULE_IDS:
        if module_id.endswith("_pining"):
            assert not _module(s, module_id).active
    assert s.auto_accept_recommended is True
    assert s.raw_archive is False
    assert s.schedule == ScheduleConfig(enabled=False, cron=None)
    assert s.retention == RetentionConfig(snapshot_days=8, version_days=7)
    assert s.caps == ResourceCaps(threads=4, memory_limit="3GB", arrow_cpus=4)
    assert s.sweep_max_fraction == 0.10
    assert s.pining_base_url == "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"


def test_defaults_hf_and_staging() -> None:
    for kind in ("hf", "staging"):
        s = load_defaults(kind)
        assert s.raw_archive is True
        assert s.auto_accept_recommended is False
        assert s.schedule.enabled is False
        assert {m.module_id for m in s.modules} == set(MODULE_IDS)


def test_checkpoint_defaults() -> None:
    """T3 spec D1/D4/D8 and decision 21: T = 5 minutes, M = 50 units, G = 2 minutes, B = 50, and the growth warnings."""
    expected = CheckpointConfig(
        minutes=5, matches=50, grace_minutes=2, move_batch=50, warn_versions=200, warn_catalog_mb=1024
    )
    assert CheckpointConfig() == expected
    for kind in ("laptop", "hf", "staging"):
        assert load_defaults(kind).checkpoint == expected


@pytest.mark.parametrize(
    "field", ["minutes", "matches", "grace_minutes", "move_batch", "warn_versions", "warn_catalog_mb"]
)
def test_checkpoint_rejects_zero(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        CheckpointConfig.model_validate({field: 0})


def test_saved_config_without_checkpoint_loads_defaults() -> None:
    """A config saved before T3 has no `checkpoint` block; it loads with the defaults."""
    data: dict[str, Any] = load_defaults("staging").model_dump(mode="json")
    del data["checkpoint"]
    assert Settings.model_validate(data).checkpoint == CheckpointConfig()


def test_maintenance_defaults() -> None:
    """T12 spec D3: run-end maintenance spends at most 300 seconds per run (rewrite and merge together)."""
    assert MaintenanceConfig() == MaintenanceConfig(merge_budget_seconds=300)
    for kind in ("laptop", "hf", "staging"):
        assert load_defaults(kind).maintenance == MaintenanceConfig()


def test_maintenance_budget_rejects_negative() -> None:
    with pytest.raises(ValidationError, match="merge_budget_seconds"):
        MaintenanceConfig.model_validate({"merge_budget_seconds": -1})
    assert MaintenanceConfig.model_validate({"merge_budget_seconds": 0}).merge_budget_seconds == 0


def test_saved_config_without_maintenance_loads_defaults() -> None:
    """A config saved before T12 has no `maintenance` block; it loads with the default."""
    data: dict[str, Any] = load_defaults("staging").model_dump(mode="json")
    del data["maintenance"]
    assert Settings.model_validate(data).maintenance == MaintenanceConfig()


def test_retention_rule() -> None:
    with pytest.raises(ValidationError, match="snapshot"):
        RetentionConfig(snapshot_days=7, version_days=7)
    RetentionConfig(snapshot_days=9, version_days=7)


def test_unknown_schema_version_rejected() -> None:
    data: dict[str, Any] = load_defaults("laptop").model_dump()
    data["schema_version"] = 2
    with pytest.raises(ValidationError):
        Settings.model_validate(data)


def test_clause_ids_validated() -> None:
    for bad in ("9'; drop table x", "a/../b", "..", "", "9 281"):
        with pytest.raises(ValidationError):
            CompetitionClause(competition_id=bad, seasons="all")
        with pytest.raises(ValidationError):
            MatchClause(match_ids=[bad])
    CompetitionClause(competition_id="DFL-COM-000002", seasons=["DFL-SEA-0001K6"])
    MatchClause(match_ids=["J03WMX", "3788741"])


def test_statsbomb_360_option_only_for_statsbomb() -> None:
    for module_id in ("statsbomb_public", "statsbomb_pining"):
        ModuleConfig(module_id=module_id, active=True, filter=[], statsbomb_360_only=True)
    for module_id in MODULE_IDS:
        if not module_id.startswith("statsbomb_"):
            with pytest.raises(ValidationError, match="StatsBomb"):
                ModuleConfig(module_id=module_id, active=True, filter=[], statsbomb_360_only=True)


def test_every_module_exactly_once() -> None:
    data: dict[str, Any] = load_defaults("laptop").model_dump()
    data["modules"] = data["modules"][:-1]
    with pytest.raises(ValidationError, match="module"):
        Settings.model_validate(data)
    data = load_defaults("laptop").model_dump()
    data["modules"].append(data["modules"][0])
    with pytest.raises(ValidationError, match="module"):
        Settings.model_validate(data)


def test_schedule_needs_a_cron_when_enabled() -> None:
    with pytest.raises(ValidationError, match="cron"):
        ScheduleConfig(enabled=True, cron=None)
    with pytest.raises(ValidationError, match="cron"):
        ScheduleConfig(enabled=True, cron="every day")
    ScheduleConfig(enabled=True, cron="0 3 * * *")
    ScheduleConfig(enabled=True, cron="@daily")


def test_caps_and_base_url_validated() -> None:
    with pytest.raises(ValidationError):
        ResourceCaps(threads=0, memory_limit="3GB", arrow_cpus=4)
    with pytest.raises(ValidationError):
        ResourceCaps(threads=4, memory_limit="lots", arrow_cpus=4)
    data: dict[str, Any] = load_defaults("laptop").model_dump()
    data["pining_base_url"] = "http://example.com/v1"
    with pytest.raises(ValidationError, match="https"):
        Settings.model_validate(data)


def test_settings_are_frozen() -> None:
    s = load_defaults("laptop")
    with pytest.raises(ValidationError):
        s.raw_archive = True  # type: ignore[misc]


def test_json_round_trip() -> None:
    s = load_defaults("staging")
    assert Settings.model_validate_json(s.model_dump_json()) == s
