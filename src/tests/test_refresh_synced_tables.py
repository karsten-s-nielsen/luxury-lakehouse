"""Tests for ingestion.refresh_synced_tables — auth and table list invariants."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from ingestion.refresh_synced_tables import (
    _check_event_log_ownership,
    _classify_pipeline_poll_response,
    _event_log_fqn,
    _fetch_table_owner,
)


@pytest.fixture(autouse=True)
def _stub_workspace_client_and_reset_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Autouse: stub WorkspaceClient + reset host cache so tests never hit real auth.

    CI has no ``DATABRICKS_HOST``/``DATABRICKS_TOKEN``, so any test that
    transitively reaches ``_get_host()`` or ``_get_auth_headers()`` would
    otherwise fail with ``default auth: cannot configure default credentials``
    the first time ``WorkspaceClient()`` is constructed. The stub returns a
    benign MagicMock with a fake host; tests that need specific behaviour
    (e.g. the ``_get_host_*`` regression tests) layer their own
    ``monkeypatch.setattr`` on top — their patch wins inside the test body
    because pytest's function-scoped monkeypatch applies setattr calls in
    order and last-write-wins.

    Also resets ``_CACHED_HOST`` so cache state from a prior test can't
    leak forward.
    """
    import ingestion.refresh_synced_tables as mod

    stub_client = MagicMock()
    stub_client.config.host = "https://test.databricks.com"
    stub_client.config.authenticate.return_value = {"Authorization": "Bearer test-token"}

    monkeypatch.setattr(mod, "WorkspaceClient", lambda: stub_client)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)


def test_get_auth_headers_uses_workspace_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must obtain headers from WorkspaceClient.config.authenticate."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer test-token-123"}

    monkeypatch.setattr(
        "ingestion.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    from ingestion.refresh_synced_tables import _get_auth_headers

    headers = _get_auth_headers()

    assert headers == {"Authorization": "Bearer test-token-123"}
    mock_ws.config.authenticate.assert_called_once()


def test_get_auth_headers_does_not_call_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must NOT shell out to the Databricks CLI."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer x"}
    monkeypatch.setattr(
        "ingestion.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("subprocess.run must not be called from _get_auth_headers")

    monkeypatch.setattr(subprocess, "run", _fail)

    from ingestion.refresh_synced_tables import _get_auth_headers

    _get_auth_headers()  # must not raise


def test_synced_tables_list_has_40_entries() -> None:
    """SYNCED_TABLES drift guard — should match the 40 tables in Terraform.

    34 baseline + 3 pre-aggregated marts added 2026-04-17
    (fct_heatmap_agg, fct_vaep_breakdown_agg, fct_gk_actions_detail) + 1
    pre-aggregated mart added 2026-04-18 (fct_funnel_stages_agg, D58) to
    eliminate the season-mode Parallel Seq Scan + LIMIT 500000 truncation
    on the Conversion Funnel page + 1 discipline-events mart added
    2026-04-19 (fct_discipline_events, for Match Summary Row 1 auto-include
    and Row 2 red-card markers) + 1 Kimball conformed match dim added
    2026-04-20 (dim_matches_synced, ADR-011 PR 1) + 1 v2 xG mart added
    2026-04-22 (fct_xg_predictions_v2, ADR-013 PR 3 — first mart applying
    the ML-inference-outputs-follow-dbt pattern) - 1 v1 xG mart removed
    2026-05-07 (fct_xg_predictions, XG1-RETIRE per ADR-023).
    """
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    assert len(SYNCED_TABLES) == 40


def test_get_host_uses_workspace_client_not_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must resolve via WorkspaceClient.config.host, not os.environ.

    Regression test for: daily Databricks job failed with KeyError('DATABRICKS_HOST')
    because the env var is not set in the job runtime even though OAuth M2M auth
    works. WorkspaceClient.config.host handles that case via runtime context.
    """
    import ingestion.refresh_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.config.host = "https://test.databricks.com"
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    assert mod._get_host() == "https://test.databricks.com"


def test_get_host_adds_https_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must add https:// prefix when WorkspaceClient returns bare host."""
    import ingestion.refresh_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.config.host = "test.databricks.com"
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    assert mod._get_host() == "https://test.databricks.com"


def test_get_host_caches_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must cache the result after first lookup (perf: 102 HTTP calls/run)."""
    import ingestion.refresh_synced_tables as mod

    call_count = [0]

    def _counted() -> MagicMock:
        call_count[0] += 1
        m = MagicMock()
        m.config.host = "https://cached.test"
        return m

    monkeypatch.setattr(mod, "WorkspaceClient", _counted)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    mod._get_host()
    mod._get_host()
    mod._get_host()

    assert call_count[0] == 1, "WorkspaceClient should be instantiated exactly once"


def test_get_pipeline_id_uses_provided_catalog_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_pipeline_id must build the URL from caller-provided catalog/schema, never module state."""
    captured: dict[str, str] = {}

    def mock_get(url: str, **_kwargs: object) -> MagicMock:
        captured["url"] = url
        resp = MagicMock()
        resp.json.return_value = {"data_synchronization_status": {"pipeline_id": "pipe-xyz"}}
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr("ingestion.refresh_synced_tables.requests.get", mock_get)
    # _get_host() reads DATABRICKS_HOST at runtime; mock it to avoid env dependence in CI
    monkeypatch.setattr("ingestion.refresh_synced_tables._get_host", lambda: "https://test.databricks.com")

    from ingestion.refresh_synced_tables import _get_pipeline_id

    pipeline_id = _get_pipeline_id(
        "fct_shots_synced",
        {"Authorization": "Bearer x"},
        catalog="alt_catalog",
        schema="alt_schema",
    )

    assert pipeline_id == "pipe-xyz"
    assert "alt_catalog.alt_schema.fct_shots_synced" in captured["url"]


def test_main_rejects_invalid_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() must exit non-zero when --catalog fails identifier validation (SQL injection guard)."""
    monkeypatch.setattr("sys.argv", ["refresh_synced_tables", "--catalog", "drop; table users--"])

    from ingestion.refresh_synced_tables import main

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2


def test_main_rejects_invalid_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() must exit non-zero when --schema fails identifier validation."""
    monkeypatch.setattr("sys.argv", ["refresh_synced_tables", "--schema", "1invalid"])

    from ingestion.refresh_synced_tables import main

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# _classify_pipeline_poll_response — silent-success bug regression tests.
#
# Prior bug: `_poll_pipeline` returned on top-level ``state == "IDLE"``, but
# Databricks pipelines report ``IDLE`` whenever they are not currently
# executing — even after a FAILED update. 33 of 34 synced tables were
# silently reporting SUCCESS in the daily job despite being broken for days.
# The classifier below must consult ``latest_updates[0].state`` before
# declaring a pipeline healthy.
# ---------------------------------------------------------------------------


def test_idle_top_state_with_completed_latest_update_returns_idle() -> None:
    """The happy path: pipeline not currently running, last update succeeded."""
    resp = {
        "state": "IDLE",
        "latest_updates": [{"state": "COMPLETED", "update_id": "abc"}],
    }
    assert _classify_pipeline_poll_response(resp) == "IDLE"


def test_idle_top_state_with_failed_latest_update_returns_failed() -> None:
    """The original silent-success bug: top state IDLE but last update FAILED."""
    resp = {
        "state": "IDLE",
        "latest_updates": [{"state": "FAILED", "update_id": "abc"}],
    }
    assert _classify_pipeline_poll_response(resp) == "FAILED"


def test_idle_top_state_with_canceled_latest_update_returns_failed() -> None:
    resp = {
        "state": "IDLE",
        "latest_updates": [{"state": "CANCELED", "update_id": "abc"}],
    }
    assert _classify_pipeline_poll_response(resp) == "FAILED"


def test_deleted_pipeline_returns_deleted_regardless_of_updates() -> None:
    resp = {
        "state": "DELETED",
        "latest_updates": [{"state": "COMPLETED", "update_id": "abc"}],
    }
    assert _classify_pipeline_poll_response(resp) == "DELETED"


def test_deleted_pipeline_with_no_updates_returns_deleted() -> None:
    resp = {"state": "DELETED", "latest_updates": []}
    assert _classify_pipeline_poll_response(resp) == "DELETED"


def test_running_top_state_returns_none_for_continue_polling() -> None:
    resp = {
        "state": "RUNNING",
        "latest_updates": [{"state": "RUNNING", "update_id": "abc"}],
    }
    assert _classify_pipeline_poll_response(resp) is None


def test_in_flight_update_states_return_none() -> None:
    for upd_state in (
        "RUNNING",
        "CREATED",
        "QUEUED",
        "WAITING_FOR_RESOURCES",
        "INITIALIZING",
        "SETTING_UP_TABLES",
        "RESETTING",
        "RESYNCING",
    ):
        resp = {"state": "IDLE", "latest_updates": [{"state": upd_state}]}
        assert _classify_pipeline_poll_response(resp) is None, f"state {upd_state} should keep polling"


def test_no_latest_updates_idle_top_state_returns_none() -> None:
    """Brand new pipeline — no updates yet. Caller keeps polling until top state transitions."""
    resp = {"state": "IDLE", "latest_updates": []}
    assert _classify_pipeline_poll_response(resp) is None


def test_no_latest_updates_key_at_all_returns_none() -> None:
    resp = {"state": "IDLE"}
    assert _classify_pipeline_poll_response(resp) is None


def test_unknown_update_state_returns_none() -> None:
    """Defensive: if Databricks adds a new update state we don't recognize, keep polling.

    Timeout will eventually surface the problem as an error.
    """
    resp = {"state": "IDLE", "latest_updates": [{"state": "SOMETHING_NEW"}]}
    assert _classify_pipeline_poll_response(resp) is None


def test_failed_top_state_with_failed_latest_update_returns_failed() -> None:
    resp = {
        "state": "FAILED",
        "latest_updates": [{"state": "FAILED"}],
    }
    assert _classify_pipeline_poll_response(resp) == "FAILED"


# ---------------------------------------------------------------------------
# Event_log ownership pre-check (Bug B 2c) — regression tests for drift
# ---------------------------------------------------------------------------


def test_event_log_fqn_converts_dashes_to_underscores() -> None:
    fqn = _event_log_fqn(
        catalog="soccer_analytics",
        schema="dev_gold",
        pipeline_id="4ea189db-aa43-4144-8825-da54cf965b7f",
    )
    assert fqn == "soccer_analytics.dev_gold.event_log_4ea189db_aa43_4144_8825_da54cf965b7f"


def test_event_log_fqn_observability_schema() -> None:
    fqn = _event_log_fqn(
        catalog="soccer_analytics",
        schema="observability",
        pipeline_id="abc-def",
    )
    assert fqn == "soccer_analytics.observability.event_log_abc_def"


def _stub_response(status_code: int, payload: object) -> MagicMock:
    """Return a MagicMock that looks like a requests.Response with .json() and .raise_for_status()."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload

    def _raise() -> None:
        if status_code >= 400 and status_code != 404:
            from requests import HTTPError

            raise HTTPError(f"HTTP {status_code}")

    resp.raise_for_status.side_effect = _raise
    return resp


def test_fetch_table_owner_returns_owner_string() -> None:
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(200, {"owner": "dbt-owners-dev"}),
    ):
        owner = _fetch_table_owner("cat.sch.event_log_x", {"Authorization": "Bearer t"})
    assert owner == "dbt-owners-dev"


def test_fetch_table_owner_returns_none_on_404() -> None:
    """A 404 means the event_log does not exist yet (brand-new pipeline) — not drift."""
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(404, {"error_code": "TABLE_NOT_FOUND"}),
    ):
        owner = _fetch_table_owner("cat.sch.event_log_x", {"Authorization": "Bearer t"})
    assert owner is None


def test_fetch_table_owner_returns_none_when_owner_field_missing() -> None:
    """Defensive: if the API stops returning the owner field, don't falsely match."""
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(200, {"name": "event_log_x"}),
    ):
        owner = _fetch_table_owner("cat.sch.event_log_x", {"Authorization": "Bearer t"})
    assert owner is None


def test_check_event_log_ownership_ok_when_owner_matches() -> None:
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(200, {"owner": "dbt-owners-dev"}),
    ):
        ok, fqn, actual = _check_event_log_ownership(
            "4ea189db-aa43-4144-8825-da54cf965b7f",
            {"Authorization": "Bearer t"},
            catalog="soccer_analytics",
            schema="dev_gold",
            expected_owner="dbt-owners-dev",
        )
    assert ok is True
    assert actual == "dbt-owners-dev"
    assert fqn.endswith("event_log_4ea189db_aa43_4144_8825_da54cf965b7f")


def test_check_event_log_ownership_drift_returns_false() -> None:
    """The real-world 2026-04 regression: event_log owned by user, not the SP/group."""
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(200, {"owner": "karstenskyt@gmail.com"}),
    ):
        ok, _fqn, actual = _check_event_log_ownership(
            "0e9352e8-3d7e-4d92-a646-bcc2d6ce075c",
            {"Authorization": "Bearer t"},
            catalog="soccer_analytics",
            schema="observability",
            expected_owner="dbt-owners-dev",
        )
    assert ok is False
    assert actual == "karstenskyt@gmail.com"


def test_check_event_log_ownership_missing_event_log_is_ok() -> None:
    """A brand-new pipeline whose event_log has not been created yet is not drift."""
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(404, {"error_code": "TABLE_NOT_FOUND"}),
    ):
        ok, _fqn, actual = _check_event_log_ownership(
            "abc-def",
            {"Authorization": "Bearer t"},
            catalog="soccer_analytics",
            schema="dev_gold",
            expected_owner="dbt-owners-dev",
        )
    assert ok is True
    assert actual is None


def test_check_event_log_ownership_prod_group() -> None:
    """expected_owner is configurable — the default 'dbt-owners-dev' is environment-specific."""
    with patch(
        "ingestion.refresh_synced_tables.requests.get",
        return_value=_stub_response(200, {"owner": "dbt-owners-prod"}),
    ):
        ok, _fqn, actual = _check_event_log_ownership(
            "4ea189db-aa43-4144-8825-da54cf965b7f",
            {"Authorization": "Bearer t"},
            catalog="soccer_analytics",
            schema="prod_gold",
            expected_owner="dbt-owners-prod",
        )
    assert ok is True
    assert actual == "dbt-owners-prod"


# ---------------------------------------------------------------------------
# wait_until_online (PR 4b G1 helper)
# ---------------------------------------------------------------------------


class TestWaitUntilOnline:
    """Tests for ingestion.refresh_synced_tables.wait_until_online (PR 4b G1)."""

    @patch("ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("ingestion.refresh_synced_tables.requests.get")
    def test_returns_on_online_state(self, mock_get: MagicMock) -> None:
        from ingestion import refresh_synced_tables as refresh

        mock_get.side_effect = [
            _stub_response(200, {"status": {"detailed_state": "SYNCED_TABLE_PENDING"}}),
            _stub_response(200, {"status": {"detailed_state": "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"}}),
        ]
        refresh.wait_until_online(
            "soccer_analytics.dev_gold.fct_action_values_synced",
            timeout_s=60,
            poll_interval_s=1,
        )
        assert mock_get.call_count == 2

    @patch("ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("ingestion.refresh_synced_tables.time.monotonic")
    @patch("ingestion.refresh_synced_tables.requests.get")
    def test_timeout_raises_with_context(
        self,
        mock_get: MagicMock,
        mock_mono: MagicMock,
    ) -> None:
        from ingestion import refresh_synced_tables as refresh

        # monotonic called: start, after-poll #1, after-poll #2, ...
        mock_mono.side_effect = [0.0, 10.0, 700.0]
        mock_get.return_value = _stub_response(200, {"status": {"detailed_state": "SYNCED_TABLE_PENDING"}})

        with pytest.raises(TimeoutError) as excinfo:
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.fct_action_values_synced",
                timeout_s=600,
                poll_interval_s=1,
            )
        msg = str(excinfo.value)
        assert "fct_action_values_synced" in msg
        assert "SYNCED_TABLE_PENDING" in msg

    @patch("ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("ingestion.refresh_synced_tables.requests.get")
    def test_terminal_failure_raises(self, mock_get: MagicMock) -> None:
        from ingestion import refresh_synced_tables as refresh

        mock_get.return_value = _stub_response(200, {"status": {"detailed_state": "SYNCED_TABLE_OFFLINE_FAILED"}})
        with pytest.raises(RuntimeError) as excinfo:
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.fct_action_values_synced",
                timeout_s=60,
                poll_interval_s=1,
            )
        assert "SYNCED_TABLE_OFFLINE_FAILED" in str(excinfo.value)

    @patch("ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("ingestion.refresh_synced_tables.requests.get")
    def test_http_404_propagates(self, mock_get: MagicMock) -> None:
        import requests as req

        from ingestion import refresh_synced_tables as refresh

        resp = _stub_response(404, {"error_code": "NOT_FOUND"})
        resp.raise_for_status.side_effect = req.HTTPError("404")  # type: ignore[attr-defined]
        mock_get.return_value = resp

        with pytest.raises(req.HTTPError):
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.does_not_exist_synced",
                timeout_s=60,
                poll_interval_s=1,
            )

    def test_invalid_table_fqn_last_segment_rejected(self) -> None:
        from ingestion import refresh_synced_tables as refresh

        with pytest.raises(ValueError, match="Invalid table_fqn"):
            refresh.wait_until_online("soccer_analytics.dev_gold.has space")


# ─────────────────────────────────────────────────────────────────────────────
# _poll_pipelines_parallel — parallel polling refactor (2026-04-26 hotfix).
#
# Sequential polling in the prior `main` was Σ-per-pipeline; one slow pipeline
# stalled all reporting. Parallel polling collapses worst-case to max-of-
# pipelines. The tests below assert the parallelism actually delivers + that
# per-pipeline exceptions don't abort the batch.
# ─────────────────────────────────────────────────────────────────────────────


class TestPollPipelinesParallel:
    def test_runs_polls_concurrently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Total wall-time must collapse to max-of-polls, not sum-of-polls."""
        import time as _time

        from ingestion.refresh_synced_tables import _poll_pipelines_parallel

        per_poll_seconds = 0.5

        def _slow_poll(pipeline_id: str, headers: dict[str, str]) -> str:
            _time.sleep(per_poll_seconds)
            return "IDLE"

        monkeypatch.setattr(
            "ingestion.refresh_synced_tables._poll_pipeline",
            _slow_poll,
        )

        triggered = [(f"table_{i}", f"pid_{i}") for i in range(5)]
        start = _time.monotonic()
        results = _poll_pipelines_parallel(triggered, {}, max_workers=10)
        elapsed = _time.monotonic() - start

        # Sequential would be 5 * 0.5 = 2.5s. Parallel with 10 workers should
        # be ~0.5s. Threshold 1.5s gives 3x margin for thread scheduling jitter.
        assert elapsed < 1.5, (
            f"parallel polling too slow: {elapsed:.2f}s "
            f"(expected <1.5s; sequential would be ~{len(triggered) * per_poll_seconds}s)"
        )
        assert len(results) == len(triggered)
        assert all(state == "IDLE" for state in results.values())

    def test_max_workers_caps_concurrency(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When max_workers < N, total time is at least Σ/max_workers."""
        import time as _time

        from ingestion.refresh_synced_tables import _poll_pipelines_parallel

        per_poll_seconds = 0.4

        def _slow_poll(pipeline_id: str, headers: dict[str, str]) -> str:
            _time.sleep(per_poll_seconds)
            return "IDLE"

        monkeypatch.setattr(
            "ingestion.refresh_synced_tables._poll_pipeline",
            _slow_poll,
        )

        triggered = [(f"table_{i}", f"pid_{i}") for i in range(8)]
        start = _time.monotonic()
        _poll_pipelines_parallel(triggered, {}, max_workers=2)
        elapsed = _time.monotonic() - start

        # 8 polls at 0.4s with max_workers=2 → ~4 batches of 0.4s = 1.6s lower bound.
        # Allow a generous upper bound so CI jitter doesn't flake the test.
        assert elapsed >= 1.0, (
            f"max_workers cap not honoured: {elapsed:.2f}s "
            f"(expected >=1.0s with max_workers=2 over 8 polls of 0.4s each)"
        )

    def test_per_pipeline_exception_does_not_abort_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A single failing poll must not abort the whole batch; surface as ERROR string."""
        from ingestion.refresh_synced_tables import _poll_pipelines_parallel

        def _selectively_failing_poll(pipeline_id: str, headers: dict[str, str]) -> str:
            if pipeline_id == "pid_bad":
                raise ConnectionError("network down")
            return "IDLE"

        monkeypatch.setattr(
            "ingestion.refresh_synced_tables._poll_pipeline",
            _selectively_failing_poll,
        )

        triggered = [
            ("table_a", "pid_a"),
            ("table_bad", "pid_bad"),
            ("table_c", "pid_c"),
        ]
        results = _poll_pipelines_parallel(triggered, {}, max_workers=10)

        assert results["table_a"] == "IDLE"
        assert results["table_c"] == "IDLE"
        assert results["table_bad"].startswith("ERROR:")
        assert "network down" in results["table_bad"]

    def test_failed_pipeline_state_is_returned_verbatim(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FAILED / TIMEOUT / DELETED states from _poll_pipeline pass through unchanged."""
        from ingestion.refresh_synced_tables import _poll_pipelines_parallel

        states_by_pid = {"pid_a": "IDLE", "pid_b": "FAILED", "pid_c": "TIMEOUT"}

        def _state_table_poll(pipeline_id: str, headers: dict[str, str]) -> str:
            return states_by_pid[pipeline_id]

        monkeypatch.setattr(
            "ingestion.refresh_synced_tables._poll_pipeline",
            _state_table_poll,
        )

        triggered = [(f"table_{k[-1]}", k) for k in states_by_pid]
        results = _poll_pipelines_parallel(triggered, {}, max_workers=10)

        assert results == {"table_a": "IDLE", "table_b": "FAILED", "table_c": "TIMEOUT"}

    def test_empty_triggered_list_returns_empty_dict(self) -> None:
        """Edge case: nothing triggered → empty result, no exceptions."""
        from ingestion.refresh_synced_tables import _poll_pipelines_parallel

        results = _poll_pipelines_parallel([], {}, max_workers=10)
        assert results == {}
