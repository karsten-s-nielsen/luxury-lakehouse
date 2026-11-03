"""The hf_space tier: the Ops Space's capabilities with only its scoped HF token (spec §26 items 1, 3, 15, 16; plan
Task 33 step 3; decisions in spec §29.18).

The reader test pairs with the hf_job writer through the barrier and runs first, so the populated test lake is there
for the pages and by-value reads that follow. The reader watches `ops_barrier` move across the writer's paced commits,
accepts only consistent versions (a torn read is counted, never passed), and asserts against the writer's reported
final sequence and row count. The schedule test drives the owner's throwaway suspended scheduled job. The Space token
is scoped to the lake bucket only, so a read of the restricted bucket is denied: 401 or 403, or 404 when a
fine-grained token is not granted an existing private bucket (the Hub hides it) — all three count as a denial (item 16).
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any, cast

import duckdb
import pytest
from hfkit import (
    BARRIER_ARMED,
    BARRIER_READER_READY,
    BARRIER_WRITER_DONE,
    READER_WAIT_S,
    RUN,
    armed_for,
    clear_marker,
    done_for,
)
from hfrunkit import BARRIER_BUILD_TABLE, PUBLIC_MATCH, TRACKING_TABLE
from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import HfHubHTTPError

from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.errors import HttpStatusError, IntegrityError
from lakehouse.ids.keys import match_key
from lakehouse.runs.history import read_table
from lakehouse.schedule.hf_jobs import JOBS_TOKEN_ENV, SCHEDULED_JOB_ID_ENV, HfJobsSchedule
from lakehouse.services.account import AccountService
from lakehouse.services.settings import SettingsService
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.roots import HF_TOKEN_ENV, default_bootstrap_file, resolve_roots
from lakehouse.ui.shell import PAGE_MODULES, anon_viewable, render_for
from lakehouse.web.app import SIGNIN_PATH, create_app
from lakehouse.web.auth_local import SESSION_USER
from lakehouse.web.sessions import ValidLogins
from lakehouse.wiring import build_services

pytestmark = [pytest.mark.hf, pytest.mark.hf_space]

_TEST_SCHED_ENV = "LAKEHOUSE_HF_TEST_SCHEDULED_JOB_ID"
_LAKE_PAGES = frozenset({"monitoring", "runs", "review", "modules"})  # §19.5: ≤ 500 ms cached
# Data sources loads its editor on demand (audit D1), so navigate is a lake read; Settings reads the schedule backend.
_EXTERNAL_PAGES = frozenset({"settings"})  # network / HF-jobs API; §19.4 page-load bound: ≤ 3 s
_SIGNIN_NOTICE_MARK = "Sign in to view this page."


def test_reader_sees_commits_and_reads_outputs_by_value(space: HfStorage) -> None:
    """item 3 reader half + item 15 reader half. Watches `ops_barrier` move while the writer commits, accepting only
    rows of a published version (`(k, sum(range(k)))`), and asserts against the writer's reported final sequence and
    count — never on poll timing. Then reads the dbt table, the run-history row and the tracking table by value
    through hf:// with only the Space token. Runs first so the populated lake is there for the tests that follow."""
    # Campaign nonce (impl review B HFB-IMPL-01, 2026-11-02): the reader mints it and writes it as READER_READY; the
    # writer echoes it in BARRIER_ARMED and in BARRIER_WRITER_DONE (f"{nonce}:<final_seq>:<count>", or f"{nonce}:error"
    # on failure). The reader acts only on markers that carry THIS nonce, so a reused bucket's leftover markers from an
    # earlier campaign are ignored, never read as this run's data. `space.put` overwrites any stale READER_READY.
    nonce = uuid.uuid4().hex
    clear_marker(space, BARRIER_ARMED)
    clear_marker(space, BARRIER_WRITER_DONE)
    space.put("lake", BARRIER_READER_READY, nonce.encode())
    reader = CatalogReader(space)
    seen: list[tuple[int, int, int]] = []  # (CURRENT seq, row count, row sum) of each successful barrier read
    loud: list[str] = []  # torn / mid-publish reads, counted and reported — never silently passed
    deadline = time.monotonic() + READER_WAIT_S  # one shared budget for the arm wait + the sampling (<= READER_WAIT_S)
    # Wait until the writer drops+recreates ops_barrier empty and arms THIS campaign; a writer error (f"{nonce}:error")
    # releases the reader at once rather than after the full budget.
    while not armed_for(space, nonce):
        if done_for(space, nonce) == "error":
            pytest.fail("the writer reported an error before arming; read the hf_job logs")
        if time.monotonic() >= deadline:
            pytest.fail(f"the reader timed out after {READER_WAIT_S / 60:.0f} min waiting for the writer to arm")
        time.sleep(2.0)
    try:
        while True:
            done = done_for(space, nonce) is not None
            try:
                seq = read_current(space).seq
                with reader.session() as conn:
                    row = conn.execute(
                        f"SELECT count(*), coalesce(sum(i), 0) FROM lake.main.{BARRIER_BUILD_TABLE}"  # noqa: S608 - constant name
                    ).fetchone()
                if row is not None and row[0] > 0:
                    seen.append((seq, int(row[0]), int(row[1])))
            except FileNotFoundError:
                pass  # no CURRENT yet: the writer has not committed
            except (IntegrityError, duckdb.IOException, duckdb.CatalogException) as exc:
                # T33-21 (torn-read policy, recorded + accepted in ingestion §29.19): a mid-publish read is EXPECTED
                # while the writer commits. Count it by type; never accept its rows as a published version (the
                # off_version check below); fail ONLY when NO consistent read was made (the `assert seen` below).
                loud.append(f"{type(exc).__name__}: {exc}")  # a version mid-publish, or ops_barrier not created yet
            if done:
                break
            if time.monotonic() >= deadline:
                pytest.fail(f"the reader timed out after {READER_WAIT_S / 60:.0f} min waiting for the writer")
            time.sleep(2.0)
        payload = done_for(space, nonce)
        if payload == "error":
            pytest.fail("the writer reported an error; read the hf_job logs")
        assert payload is not None, "writer-done for this campaign's nonce never appeared"
        final_seq, count = (int(part) for part in payload.split(":"))
        valid = {(k, sum(range(k))) for k in range(1, count + 1)}
        assert seen, f"the reader made no consistent read of ops_barrier; loud failures ({len(loud)}): {loud[:5]}"
        off_version = [(c, s) for (_, c, s) in seen if (c, s) not in valid]
        assert off_version == [], f"the reader saw rows of no published version: {off_version[:3]}"
        assert (count, sum(range(count))) in [(c, s) for (_, c, s) in seen], "the reader never saw the final rows"
        distinct_seqs = {seq for (seq, _, _) in seen}
        assert len(distinct_seqs) >= 2, f"the reader saw {len(distinct_seqs)} barrier sequence(s); it needs >= 2"
        assert max(distinct_seqs) <= final_seq, f"reader saw seq {max(distinct_seqs)} beyond writer {final_seq}"
        assert reader.attach_count >= 2, "the reader did not re-attach as CURRENT moved"
        with reader.session() as conn:
            players = conn.execute(
                "SELECT count(*) FROM lake.main.dim_players WHERE provider = 'skillcorner'"
            ).fetchone()
            ok_runs = [row for row in read_table(conn, "ops_runs") if row["status"] == "ok"]
            tracking = conn.execute(  # the public match only: the Space token never reads private bronze (§9)
                f"SELECT count(*), coalesce(sum(x_mm), 0) FROM lake.main.{TRACKING_TABLE} WHERE match_key = ?",  # noqa: S608 - constant name
                [match_key("skillcorner", PUBLIC_MATCH)],
            ).fetchone()
    finally:
        reader.close()
    assert players is not None and players[0] > 0, "dim_players (a dbt table) read by value for the scenario provider"
    assert ok_runs, "an ok run-history row read by value"
    assert tracking is not None and tracking[0] > 0 and tracking[1] != 0, "the tracking table read by value"


def test_config_write_through_api_and_read_through_hf(space: HfStorage) -> None:
    """item 1 Space half: the Space writes a config-side object through `HfApi` (no gateway) and reads it back through
    `hf://`. A throwaway key under `config/_live-<run>/`, never the live `config/settings.json`."""
    key = f"config/_live-{RUN}/probe.json"
    space.put("lake", key, b'{"probe": true}')
    assert space.get("lake", key) == b'{"probe": true}'
    assert space.read_uri("lake", key).startswith("hf://buckets/")


def test_space_token_cannot_read_the_restricted_bucket(space: HfStorage) -> None:
    """item 16: the Space token is scoped to the lake bucket only, so a read of the restricted bucket is denied. A
    fine-grained token that is not granted the restricted bucket gets HTTP 404 (the Hub hides an existing private
    bucket the token cannot see); a broad or org-wide token gets 401 or 403. Any of the three is a denial; only a
    success (the list returns) fails this test, because it means the token can read restricted. The restricted
    bucket's existence and reachability are proven by the hf_job tier, which writes the private match's bronze to it,
    so a 404 here is a scope denial, not a wrong bucket name (§29.6)."""
    with pytest.raises(HfHubHTTPError) as caught:
        list(space.list("restricted", "data/"))
    status = caught.value.response.status_code
    assert status in (401, 403, 404), f"expected a denial (401/403/404), got HTTP {status}"


def test_space_token_cannot_touch_the_control_repo(space: HfStorage) -> None:
    """T6 spec D1: only the Job's token writes the lease ledger; the Space token can neither read nor write the
    control repository (`<lake bucket>-control`). A private repository the token cannot see reports 401, 403 or 404."""
    repo = f"{space.roots.lake.hf_bucket}-control"
    api: Any = HfApi(token=os.environ[HF_TOKEN_ENV])
    with pytest.raises(HfHubHTTPError) as read:
        api.repo_info(repo, repo_type="dataset")
    assert read.value.response.status_code in (401, 403, 404), f"read: HTTP {read.value.response.status_code}"
    operation = CommitOperationAdd(path_in_repo="lease.json", path_or_fileobj=b"{}")
    with pytest.raises(HfHubHTTPError) as write:
        api.create_commit(repo, operations=[operation], commit_message="must be refused", repo_type="dataset")
    assert write.value.response.status_code in (401, 403, 404), f"write: HTTP {write.value.response.status_code}"


def test_every_page_renders_for_both_scopes_and_lake_pages_are_cached_fast(space: HfStorage) -> None:
    """item 16 + item 3 / §19.4 + §19.5. Every Ops page renders for an operator (full content) and for a public caller
    (the sign-in notice, unless the page is anon-viewable). Each route's first render meets the §19.4 page-load bound
    (≤ 3 s). "Cold" here (review V2UI-IMPL-28): the TRUE cold start is the first route's first render — the Space's
    object cache is empty then; later routes' first renders are partly warm (shared catalog version), so their ≤ 3 s
    check is weaker than a cold-start check — only the first route measures a true cold start. The six lake-read pages
    meet the §19.5 500 ms
    cached-interaction budget on the warm second render (authoritative here — HF's object cache warms the re-render; the
    offline test gives `review` a 1.0 s local carve-out, §29.19); the two external pages (`filters`, `schedule`) meet
    the 3 s bound on both renders (§29.18). A Flask test client confirms the sign-in guard redirects an anonymous
    caller."""
    env = {**os.environ, SCHEDULED_JOB_ID_ENV: os.environ.get(_TEST_SCHED_ENV, "")}
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=default_bootstrap_file(env))
    services = build_services(space, resolved, env=env)
    for route in PAGE_MODULES:
        assert route in _LAKE_PAGES or route in _EXTERNAL_PAGES, f"unclassified page {route}"
        start = time.monotonic()
        operator = render_for(route, services, "operator")  # the COLD first render of this route (§19.4 page load)
        cold_ms = (time.monotonic() - start) * 1000
        assert operator.strip(), f"the {route} page rendered empty for an operator"
        assert cold_ms <= 3000, f"the {route} cold first render took {cold_ms:.0f} ms (budget 3000 ms, §19.4)"
        public = render_for(route, services, "public")
        if anon_viewable(route):
            assert public.strip() and _SIGNIN_NOTICE_MARK not in public, f"{route} is anon-viewable but was hidden"
        else:
            assert _SIGNIN_NOTICE_MARK in public, f"the operator-only page {route} leaked content to a public caller"
    for route in PAGE_MODULES:
        render_for(route, services, "operator")  # warms the HF object cache for this version
        start = time.monotonic()
        render_for(route, services, "operator")
        elapsed_ms = (time.monotonic() - start) * 1000
        budget = 500 if route in _LAKE_PAGES else 3000
        assert elapsed_ms <= budget, f"the {route} second render took {elapsed_ms:.0f} ms (budget {budget} ms)"

    settings = SettingsService(space, resolved).load()[0]
    app = create_app(settings, mode="local", accounts=AccountService(space), env={"LAKEHOUSE_SESSION_SECRET": "x" * 32})
    client = app.test_client()
    response = client.get("/ops/ping")
    location = response.headers.get("Location", "")
    assert response.status_code == 302 and SIGNIN_PATH in location, "the login guard did not redirect an anon caller"
    # item 16 other half (T33-12): a SIGNED-IN operator is let through the guard. Mint a valid login into the session
    # directly (the sign-in flow is covered elsewhere), then the guarded /ops/ping returns 200.
    valid = cast("ValidLogins", app.config["LAKEHOUSE_VALID_LOGINS"])
    with client.session_transaction() as sess:
        sess[SESSION_USER] = "ops"
        valid.mint(sess)
    assert client.get("/ops/ping").status_code == 200, "the guard did not let a signed-in operator through"


def _wait_until_running(api: Any, job_id: str, *, timeout_s: float) -> None:
    """Poll a job's stage until it is RUNNING, or fail loudly at the timeout. Uses the same `inspect_job(...).status
    .stage` that `HfJobsSchedule.status` reads (confirmed mocked in test_schedule_render)."""
    deadline = time.monotonic() + timeout_s
    while True:
        stage = api.inspect_job(job_id=job_id).status.stage
        if stage is not None and "RUN" in str(stage).upper():
            return
        if time.monotonic() >= deadline:
            pytest.fail(f"the triggered run did not reach RUNNING within {timeout_s / 60:.0f} min (stage={stage!r})")
        time.sleep(5.0)


def test_schedule_controls_the_throwaway_job(space: HfStorage) -> None:
    """item 15 schedule half + item 16 inspect: `reschedule`, `suspend`, `resume` and `trigger` on the owner's
    throwaway suspended scheduled job; a reschedule keeps the job's secret; the `--no-concurrency` refusal of a second
    trigger while a run is active (HTTP 409 -> `HttpStatusError`); and the inspect response lists the secret name but
    never its value. The Space token never creates a job and never sends a secret (§18, §29.5). `space` keeps this
    test in the hf_space tier."""
    _ = space
    job_id = os.environ.get(_TEST_SCHED_ENV, "")
    assert job_id, f"{_TEST_SCHED_ENV} must name the owner's throwaway suspended scheduled job"
    # the job-control token (two-token design), falling back to HF_TOKEN — mirrors HfJobsSchedule.from_env
    jobs_token = os.environ.get(JOBS_TOKEN_ENV) or os.environ[HF_TOKEN_ENV]
    api: Any = HfApi(token=jobs_token)  # Any: the live HF-jobs API shapes are exercised only here
    schedule = HfJobsSchedule(api, job_id)

    schedule.reschedule("0 4 * * *")
    assert schedule.status().cron == "0 4 * * *", "reschedule did not change the cron"
    inspected = repr(api.inspect_scheduled_job(scheduled_job_id=job_id))
    assert "LAKEHOUSE_TEST_SECRET" in inspected, "reschedule dropped the job's secret name"
    assert "dummy" not in inspected, "the scheduled job's inspect response leaked a secret value"
    schedule.suspend()
    assert schedule.status().enabled is False, "suspend did not disable the schedule"
    schedule.resume()
    assert schedule.status().enabled is True, "resume did not enable the schedule"

    run_id = schedule.trigger()  # starts the throwaway job's run (a 10-minute sleep), returns the new job id
    assert run_id, "trigger returned no job id"
    try:
        _wait_until_running(api, run_id, timeout_s=600)
        with pytest.raises(HttpStatusError) as caught:
            schedule.trigger()  # a second trigger while the first run is active is refused under --no-concurrency
        assert caught.value.status == 409, f"expected HTTP 409, got {caught.value.status}"
    finally:
        try:
            api.cancel_job(job_id=run_id)  # HfApi 2.3.0
        except Exception as exc:  # noqa: BLE001 - cleanup only; reported (never silent), the 10-min run self-ends
            print(f"warning: could not cancel the triggered run {run_id}: {type(exc).__name__}: {exc}", flush=True)
        schedule.suspend()  # leave the throwaway job suspended, as the owner created it
