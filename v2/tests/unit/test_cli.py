"""The CLI (plan Task 26): every subcommand calls the services layer or the catalog functions, writes one JSON line
and exits 0 (ok), 1 (failed), 2 (usage, config or gating) or 3 (skipped: a run holds the lease).

The catalog functions and the run are fakes that record their call; the config, the decisions and the account go
through the real services on a lake that `lakehouse init` made. The two re-parse tests use a real local lake."""

from __future__ import annotations

import dataclasses
import tomllib
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from clikit import Cli, FakeBackend, Recorder
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, fake_table, save_settings, with_entry

from lakehouse import cli
from lakehouse.catalog.cleanup import CleanupPreview, CleanupReport
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.restore import RestoreRecord
from lakehouse.catalog.verify import VerifyReport
from lakehouse.catalog.working import VersionInfo
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError, LeaseMarkerError
from lakehouse.ids.keys import match_key
from lakehouse.resolve.decisions import DecisionStore, MustLink
from lakehouse.resolve.model import RecordRef
from lakehouse.runs.run import RunCounts, RunError, RunResult, reparse_matches, run_ingestion
from lakehouse.services.account import AccountService
from lakehouse.services.settings import SettingsService
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, default_bootstrap_file, resolve_roots

PASSWORD = "correct horse battery staple"  # noqa: S105 - a test value


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    return {
        "LAKEHOUSE_CONFIG_DIR": str(tmp_path / "config"),
        "LAKEHOUSE_CACHE_DIR": str(tmp_path / "cache"),
        "USERNAME": "ops",
    }


def _init(env: Mapping[str, str], lake: Path) -> None:
    code, [line] = Cli(env)("init", "--lake", str(lake))
    assert (code, line["status"]) == (0, "ok")


def _storage(lake: Path) -> LocalStorage:
    resolved = resolve_roots(cli_lake=str(lake), cli_restricted=None, env={}, bootstrap_file=lake / "none.json")
    return LocalStorage(resolved.roots)


def _settings(lake: Path) -> SettingsService:
    resolved = resolve_roots(cli_lake=str(lake), cli_restricted=None, env={}, bootstrap_file=lake / "none.json")
    return SettingsService(LocalStorage(resolved.roots), resolved)


# --- init and the roots ---------------------------------------------------------------------------------------------


def test_init_writes_bootstrap(tmp_path: Path, env: dict[str, str]) -> None:
    lake = tmp_path / "My Lake"
    code, [line] = Cli(env)("init", "--lake", str(lake))
    assert (code, line["command"], line["status"], line["config_version"]) == (0, "init", "ok", 1)
    restricted = tmp_path / "My Lake-restricted"
    assert lake.is_dir() and restricted.is_dir()
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=default_bootstrap_file(env))
    assert (resolved.source, resolved.roots.lake.local_path, resolved.roots.restricted.local_path) == (
        "bootstrap",
        lake,
        restricted,
    )
    assert _settings(lake).load() == (load_defaults("laptop"), 1)
    code, [again] = Cli(env)("init", "--lake", str(lake), "--profile", "staging")
    assert (code, again["config_version"], again["config"]) == (0, 1, "kept")  # an existing config stays
    assert _settings(lake).load()[0] == load_defaults("laptop")


def test_init_profile_and_usage(tmp_path: Path, env: dict[str, str]) -> None:
    code, _ = Cli(env)("init", "--lake", str(tmp_path / "s"), "--profile", "staging")
    assert code == 0 and _settings(tmp_path / "s").load()[0] == load_defaults("staging")
    assert Cli(env)("init", "--lake", str(tmp_path / "h"), "--profile", "hf")[0] == 2  # HF buckets are the owner's
    assert Cli(env)("init")[0] == 2
    assert not (tmp_path / "h").exists()


def test_global_root_flags_override_env(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "a")
    _init(env, tmp_path / "b")  # the bootstrap file now names b
    seen: list[Path | None] = []

    def verify(storage: Storage) -> VerifyReport:
        seen.append(storage.roots.lake.local_path)
        return VerifyReport(1, 0, 1, [])

    c = Cli({**env, "LAKEHOUSE_LAKE_ROOT": str(tmp_path / "a")}, verify=verify)
    assert c("verify")[0] == 0  # the environment beats the bootstrap file
    assert c("--lake", str(tmp_path / "b"), "verify")[0] == 0  # the flag beats the environment
    assert seen == [tmp_path / "a", tmp_path / "b"]
    code, [line] = c("--restricted", str(tmp_path / "x"), "verify")
    assert (code, line["status"]) == (2, "error") and "--lake" in line["message"]
    code, [line] = Cli(env)("--lake", str(tmp_path / "missing"), "verify")
    assert code == 2 and "does not exist" in line["message"]


# --- run, verify, restore, relocate, sweep ---------------------------------------------------------------------------


def test_run_builds_the_run_from_the_saved_config(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    run = Recorder(RunResult("run-1", "ok", RunCounts(added=2), []))
    adapters = Recorder({"idsse_public": "an adapter"})
    parsers = {"idsse": "a parser"}
    code, [line] = Cli(env, run=run, adapters=adapters, parsers=lambda: parsers)("run", "--trigger", "schedule")
    assert code == 0
    assert (line["command"], line["status"], line["run_id"], line["counts"]["added"]) == ("run", "ok", "run-1", 2)
    [call] = run.calls
    assert (call["trigger"], call["settings"], call["adapters"], call["parsers"]) == (
        "schedule",
        load_defaults("laptop"),
        {"idsse_public": "an adapter"},
        parsers,
    )
    assert call["storage"].roots.lake.local_path == tmp_path / "lake"
    assert adapters.calls[0]["args"] == (load_defaults("laptop"),)
    assert Cli(env, run=run, adapters=adapters, parsers=dict)("run")[0] == 0
    assert run.calls[-1]["trigger"] == "cli"


@pytest.mark.parametrize(("status", "exit_code"), [("failed", 1), ("skipped", 3)])
def test_run_exit_codes(tmp_path: Path, env: dict[str, str], status: str, exit_code: int) -> None:
    _init(env, tmp_path / "lake")
    errors = [RunError("idsse_public", "J03WPY", "unmapped_field", "a field")] if status == "failed" else []
    run = Recorder(RunResult("run-2", status, RunCounts(failed=len(errors)), errors))  # type: ignore[arg-type]
    code, [line] = Cli(env, run=run, adapters=Recorder({}), parsers=dict)("run")
    assert (code, line["status"]) == (exit_code, status)
    assert [e["kind"] for e in line["errors"]] == [e.kind for e in errors]


def test_cli_run_exits_1_on_failed_marker_store(tmp_path: Path, env: dict[str, str]) -> None:
    """A failed run-anchor or marker store (T3 spec D5) reaches the generic `LakehouseError` branch: exit 1."""
    _init(env, tmp_path / "lake")
    message = "lease generation 1 could not store its run anchor or acquisition marker: OSError: disk full"

    def failing(**kwargs: Any) -> RunResult:
        raise LeaseMarkerError(message)

    code, [line] = Cli(env, run=failing, adapters=Recorder({}), parsers=dict)("run")
    assert (code, line["command"], line["status"], line["message"]) == (1, "run", "failed", message)


def test_verify_reports_problems(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    clean = Cli(env, verify=Recorder(VerifyReport(3, 40, 3, [])))
    code, [line] = clean("verify")
    assert (code, line["versions"], line["files"], line["current_seq"], line["problems"]) == (0, 3, 40, 3, [])
    broken = Cli(env, verify=Recorder(VerifyReport(3, 40, 3, ["version 2: the file lake:data/x is missing"])))
    code, [line] = broken("verify")
    assert (code, line["status"], line["problems"]) == (1, "failed", ["version 2: the file lake:data/x is missing"])


def test_restore(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    restore = Recorder(RestoreRecord("ops", NOW, 2, 4, [3], 1))
    code, [line] = Cli(env, restore=restore)("restore", "--to", "2")
    assert (code, line["restored_seq"], line["new_seq"], line["dropped_seqs"]) == (0, 2, 4, [3])
    [call] = restore.calls
    assert (call["target_seq"], call["operator"], call["settings"]) == (2, "ops", load_defaults("laptop"))
    assert Cli(env, restore=restore)("--operator", "karsten", "restore", "--to", "1")[0] == 0
    assert restore.calls[-1]["operator"] == "karsten"
    assert Cli(env, restore=restore)("restore")[0] == 2


def _version(seq: int) -> VersionInfo:
    key = CanonicalKey("lake", f"catalog/versions/{seq:012d}.ducklake")
    return VersionInfo(seq=seq, kind="run_end", key=key, sha256="0" * 64, size=1, published_at=NOW)


def test_relocate_moves_the_bootstrap(tmp_path: Path, env: dict[str, str]) -> None:
    old, new = tmp_path / "old", tmp_path / "new"
    _init(env, old)
    old.rename(new)
    (tmp_path / "old-restricted").rename(tmp_path / "new-restricted")
    relocate = Recorder(_version(7))
    code, [line] = Cli(env, relocate=relocate)("relocate", "--from", str(old), "--to", str(new))
    assert (code, line["seq"], line["bootstrap"]) == (0, 7, "updated")
    [call] = relocate.calls
    assert (call["old_roots"].lake.local_path, call["old_roots"].restricted.local_path) == (
        old,
        tmp_path / "old-restricted",
    )
    assert (call["new_roots"].lake.local_path, call["new_roots"].restricted.local_path) == (
        new,
        tmp_path / "new-restricted",
    )
    assert call["args"][0].roots == call["new_roots"]  # the storage is opened on the new roots
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=default_bootstrap_file(env))
    assert resolved.roots.lake.local_path == new
    assert Cli(env, relocate=relocate)("--lake", str(new), "relocate", "--from", str(old), "--to", str(new))[0] == 2


def test_reparse_targets(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    reparse = Recorder(RunResult("run-3", "ok", RunCounts(changed=1), []))
    c = Cli(env, reparse=reparse, parsers=dict)
    assert c("reparse", "--match", "idsse:J03WPY")[0] == 0
    assert c("reparse", "--all")[0] == 0
    assert [call["target"] for call in reparse.calls] == [("idsse", "J03WPY"), "all"]
    for bad in (["reparse"], ["reparse", "--match", "J03WPY"], ["reparse", "--match", "nobody:1"]):
        assert c(*bad)[0] == 2
    assert len(reparse.calls) == 2


def test_sweep_needs_allow_large(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    orphan = CanonicalKey("lake", "data/bronze/x/900-v1-a.parquet")
    sweep = Recorder(
        CleanupReport(expired_snapshots=0, pruned_versions=[], ducklake_deleted=[], swept=[orphan], moved_not_found=[])
    )
    code, [line] = Cli(env, sweep=sweep)("sweep")
    assert code == 2 and "--allow-large" in line["message"] and sweep.calls == []
    code, [line] = Cli(env, sweep=sweep)("sweep", "--allow-large")
    assert (code, line["swept"]) == (0, ["lake:data/bronze/x/900-v1-a.parquet"])
    assert sweep.calls[0]["settings"] == load_defaults("laptop")
    assert sweep.calls[0]["expected"] is None


def _preview() -> CleanupPreview:
    return CleanupPreview(
        pruned_versions=[3],
        ducklake_listed=[CanonicalKey("lake", "data/ops/ops_runs/a.parquet")],
        doomed=[CanonicalKey("lake", "data/bronze/x/900-v1-a.parquet"), CanonicalKey("restricted", "data/r/b.parquet")],
        exempted=1,
        counted=1,
        objects_listed=40,
        max_fraction=0.1,
    )


def test_cli_sweep_dry_run_writes_the_list(tmp_path: Path, env: dict[str, str]) -> None:
    """`sweep --dry-run` (T12 plan Task 5 Step 3): no `--allow-large`, no sweep; it prints the counts and every key
    and writes the keys to the list file, one per line, for `--expect`."""
    _init(env, tmp_path / "lake")
    preview, sweep = Recorder(_preview()), Recorder(None)
    listed = tmp_path / "doomed.txt"
    code, [line] = Cli(env, sweep=sweep, sweep_preview=preview)("sweep", "--dry-run", "--list-file", str(listed))
    keys = [str(key) for key in _preview().keys()]
    assert (code, line["command"], line["status"], line["dry_run"]) == (0, "sweep", "ok", True)
    assert (line["doomed"], line["exempted"], line["counted"], line["objects_listed"]) == (2, 1, 1, 40)
    assert (line["pruned_versions"], line["ducklake_listed"], line["over_guard"]) == ([3], 1, False)
    assert (line["keys"], line["list_file"]) == (keys, str(listed))
    assert listed.read_bytes() == ("\n".join(keys) + "\n").encode()
    assert sweep.calls == [] and preview.calls[0]["settings"] == load_defaults("laptop")
    code, [line] = Cli(env, sweep=sweep, sweep_preview=Recorder(_preview()))("sweep", "--dry-run")
    assert (code, line["list_file"], line["keys"]) == (0, None, keys)  # the list file is optional


def test_cli_sweep_dry_run_flags_the_guard(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    over = dataclasses.replace(_preview(), counted=5)  # 5 > 0.1 x 40
    code, [line] = Cli(env, sweep_preview=Recorder(over))("sweep", "--dry-run")
    assert (code, line["over_guard"]) == (0, True)


def test_cli_sweep_expect_passes_the_list(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    listed = tmp_path / "doomed.txt"
    listed.write_bytes(b"lake:data/bronze/x/900-v1-a.parquet\n\nrestricted:data/r/b.parquet\n")
    sweep = Recorder(CleanupReport(0, [], [], [], []))
    code, _ = Cli(env, sweep=sweep)("sweep", "--allow-large", "--expect", str(listed))
    assert code == 0
    assert sweep.calls[0]["expected"] == frozenset(
        {CanonicalKey("lake", "data/bronze/x/900-v1-a.parquet"), CanonicalKey("restricted", "data/r/b.parquet")}
    )


def test_cli_sweep_unexpected_key_fails(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    listed = tmp_path / "doomed.txt"
    listed.write_bytes(b"lake:data/bronze/x/900-v1-a.parquet\n")

    def refuses(*args: Any, **kwargs: Any) -> CleanupReport:
        raise GuardStopError("the cleanup would delete 1 objects outside the expected list (lake:x); nothing deleted")

    code, [line] = Cli(env, sweep=refuses)("sweep", "--allow-large", "--expect", str(listed))
    assert (code, line["status"]) == (1, "failed") and "outside the expected list" in line["message"]


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["sweep", "--expect", "x.txt"], "--expect"),
        (["sweep", "--dry-run", "--allow-large"], "--dry-run"),
        (["sweep", "--dry-run", "--expect", "x.txt"], "--dry-run"),
        (["sweep", "--allow-large", "--list-file", "x.txt"], "--list-file"),
        (["sweep", "--allow-large", "--expect", "missing.txt"], "missing.txt"),
        (["sweep", "--allow-large", "--expect", "bad.txt"], "invalid canonical key"),
        (["sweep", "--allow-large", "--expect", "empty.txt"], "no key"),
    ],
)
def test_cli_sweep_flag_errors(tmp_path: Path, env: dict[str, str], argv: list[str], message: str) -> None:
    _init(env, tmp_path / "lake")
    (tmp_path / "bad.txt").write_bytes(b"data/bronze/no-root.parquet\n")
    (tmp_path / "empty.txt").write_bytes(b"\n")
    argv = [str(tmp_path / arg) if arg.endswith(".txt") else arg for arg in argv]
    sweep, preview = Recorder(None), Recorder(None)
    code, [line] = Cli(env, sweep=sweep, sweep_preview=preview)(*argv)
    assert (code, line["status"]) == (2, "error") and message in line["message"]
    assert sweep.calls == preview.calls == []


def test_hf_catalog_commands_need_gateway(env: dict[str, str]) -> None:
    hf = {
        **env,
        "LAKEHOUSE_LAKE_ROOT": "hf://buckets/ns/lake",
        "LAKEHOUSE_RESTRICTED_ROOT": "hf://buckets/ns/lake-restricted",
        "HF_TOKEN": "hf_not_a_real_token",
    }
    opened: list[ResolvedRoots] = []

    def open_storage(resolved: ResolvedRoots, env: Mapping[str, str]) -> Storage:
        opened.append(resolved)
        raise AssertionError("the command opened the lake")

    restore, relocate, sweep, preview = Recorder(None), Recorder(None), Recorder(None), Recorder(None)
    c = Cli(hf, open_storage=open_storage, restore=restore, relocate=relocate, sweep=sweep, sweep_preview=preview)
    commands = (
        ["restore", "--to", "2"],
        [
            "relocate",
            "--from",
            "hf://buckets/ns/old",
            "--from-restricted",
            "hf://buckets/ns/old-restricted",
            "--to",
            "hf://buckets/ns/lake",
            "--to-restricted",
            "hf://buckets/ns/lake-restricted",
        ],
        ["sweep", "--allow-large"],
        ["sweep", "--dry-run"],  # it holds the lease and opens a working catalog like the sweep
    )
    for argv in commands:
        code, [line] = c(*argv)
        assert (code, line["status"]) == (2, "error"), argv
        assert "HF Job" in line["message"] and "HF_S3_ACCESS_KEY_ID" in line["message"], argv
    assert opened == [] and restore.calls == relocate.calls == sweep.calls == preview.calls == []


# --- schedule, config, decisions, account ----------------------------------------------------------------------------


def test_schedule_install_uninstall_status(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    backend = FakeBackend()
    passed = {"LAKEHOUSE_X": "1", "UV_CACHE_DIR": "u", "UV_PYTHON_INSTALL_DIR": "p", "HF_HOME": "h", "TMPDIR": "t"}
    c = Cli({**env, **passed, "PATH": "/bin", "OTHER": "o"}, schedule_backend=lambda: backend)
    code, [line] = c("schedule", "install")
    assert code == 2 and "off" in line["message"] and backend.calls == []
    assert c("config", "set", "schedule.cron", "0 6 * * *")[0] == 0
    assert c("config", "set", "schedule.enabled", "true")[0] == 0
    assert c("schedule", "install", "--system")[0] == 2  # --system needs --run-as
    code, [line] = c("schedule", "install", "--system", "--run-as", "ops")
    assert (code, line["installed"], line["cron"]) == (0, True, "0 6 * * *")
    [(kind, (cron, system, task_env, run_as))] = backend.calls
    assert (kind, cron, system, run_as) == ("install", "0 6 * * *", True, "ops")
    assert task_env == {
        **passed,
        "LAKEHOUSE_CONFIG_DIR": env["LAKEHOUSE_CONFIG_DIR"],
        "LAKEHOUSE_CACHE_DIR": env["LAKEHOUSE_CACHE_DIR"],
        "LAKEHOUSE_LAKE_ROOT": str(tmp_path / "lake"),
        "LAKEHOUSE_RESTRICTED_ROOT": str(tmp_path / "lake-restricted"),
    }
    code, [line] = c("schedule", "status")
    assert (code, line["backend"], line["next_run"]) == (0, "systemd", "2026-10-28T06:00:00+00:00")
    assert c("schedule", "uninstall")[0] == 0 and backend.calls[-1] == ("uninstall", None)
    # status and uninstall never open the lake: a lost or disconnected lake can still lose its task
    nowhere = Cli({**env, "LAKEHOUSE_CONFIG_DIR": str(tmp_path / "no-bootstrap")}, schedule_backend=lambda: backend)
    assert nowhere("schedule", "status")[0] == 0
    assert nowhere("schedule", "uninstall")[0] == 0 and backend.calls[-1] == ("uninstall", None)
    assert nowhere("schedule", "install")[0] == 2


def test_config_show_and_set(tmp_path: Path, env: dict[str, str]) -> None:
    _init(env, tmp_path / "lake")
    c = Cli(env)
    code, [line] = c("config", "show")
    assert (code, line["version"], line["settings"]) == (0, 1, load_defaults("laptop").model_dump(mode="json"))
    assert c("config", "set", "raw_archive", "true") == (0, [{"command": "config", "status": "ok", "version": 2}])
    assert c("config", "set", "modules.gradientsports_public.active", "true")[0] == 0
    saved, version = _settings(tmp_path / "lake").load()
    assert (version, saved.raw_archive, saved.module("gradientsports_public").active) == (3, True, True)
    for bad in (
        ["config", "set", "retention.snapshot_days", "3"],  # not longer than the version retention
        ["config", "set", "nothing.here", "1"],
        ["config", "set", "modules.nobody.active", "true"],
        ["config", "set", "raw_archive"],
        ["config", "set", "--defaults", "staging", "raw_archive", "true"],
    ):
        assert c(*bad)[0] == 2, bad
    assert c("config", "set", "--defaults", "staging")[0] == 0
    assert _settings(tmp_path / "lake").load() == (load_defaults("staging"), 4)
    assert _settings(tmp_path / "lake").history()[0].saved_by == "ops"


def test_config_set_checkpoint_minutes(tmp_path: Path, env: dict[str, str]) -> None:
    """`lakehouse config set checkpoint.<field> <n>` sets the T3 checkpoint block (spec D1); 0 is refused."""
    _init(env, tmp_path / "lake")
    c = Cli(env)
    assert c("config", "set", "checkpoint.minutes", "10")[0] == 0
    saved, _ = _settings(tmp_path / "lake").load()
    assert saved.checkpoint.minutes == 10
    assert c("config", "set", "checkpoint.grace_minutes", "0")[0] == 2


def test_decisions_export_import(tmp_path: Path, env: dict[str, str]) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _init(env, a)
    _init(env, b)
    decision = MustLink(
        entity="team",
        a=RecordRef("statsbomb", "1"),
        b=RecordRef("wyscout", "10"),
        decided_by="ops",
        decided_at=NOW,
        reason="same team",
    )
    DecisionStore(_storage(a)).add(decision)
    file = tmp_path / "decisions.json"
    assert Cli(env)("--lake", str(a), "decisions", "export", str(file)) == (
        0,
        [{"command": "decisions", "status": "ok", "exported": 1, "file": str(file)}],
    )
    assert Cli(env)("decisions", "import", str(file))[1][0]["imported"] == 1  # into b, from the bootstrap file
    assert Cli(env)("decisions", "import", str(file))[1][0]["imported"] == 0
    assert DecisionStore(_storage(b)).load().must_links == [decision]


def test_account_create(tmp_path: Path, env: dict[str, str]) -> None:
    lake = tmp_path / "lake"
    _init(env, lake)
    c = Cli(env, stdin=PASSWORD + "\n")
    code, [line] = c("account", "create", "ops", "--password-stdin")
    assert (code, line["username"]) == (0, "ops") and PASSWORD not in c.out.getvalue()
    assert AccountService(_storage(lake)).verify("ops", PASSWORD)
    code, [line] = Cli(env, stdin=PASSWORD + "\n")("account", "create", "other", "--password-stdin")
    assert code == 2 and "--replace" in line["message"]
    prompted = Cli(env, read_password=lambda: PASSWORD + " 2")
    assert prompted("account", "create", "other", "--replace")[0] == 0
    assert AccountService(_storage(lake)).verify("other", PASSWORD + " 2")
    assert Cli(env, stdin="short\n")("account", "create", "ops", "--password-stdin", "--replace")[0] == 2
    hf = {**env, "LAKEHOUSE_LAKE_ROOT": "hf://buckets/ns/l", "LAKEHOUSE_RESTRICTED_ROOT": "hf://buckets/ns/r"}
    code, [line] = Cli(hf, stdin=PASSWORD + "\n")("account", "create", "ops", "--password-stdin")
    assert code == 2 and "Hugging Face" in line["message"]


def test_console_script_and_module_entry() -> None:
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["scripts"]["lakehouse"] == "lakehouse.cli:main"
    assert callable(cli.main)


# --- the re-parse from the archive, on a real lake -------------------------------------------------------------------


def _first_run(kit: LakeKit, adapters: list[FakeAdapter], parsers: Mapping[str, FakeParser]) -> None:
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers=parsers,  # type: ignore[arg-type]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )
    assert result.status == "ok"


def _bronze(kit: LakeKit, provider: str) -> list[tuple[Any, ...]]:
    with CatalogReader(kit.storage).session() as conn:
        return conn.execute(
            f'SELECT match_key, artifact, line, _source_hash FROM lake.main."{fake_table(provider)}" ORDER BY ALL'  # noqa: S608 - a checked bronze table name
        ).fetchall()


def _no_adapter(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("the re-parse built an adapter")


def _reparse_cli(kit: LakeKit, env: Mapping[str, str], parsers: Mapping[str, FakeParser]) -> Callable[..., Any]:
    reparse = partial(reparse_matches, dbt=None, now=lambda: NOW)
    c = Cli(env, adapters=_no_adapter, parsers=lambda: parsers, reparse=reparse)
    return partial(c, "--lake", str(kit.roots.lake.local_path))


def test_reparse_from_archive_offline(kit: LakeKit, env: dict[str, str]) -> None:
    save_settings(kit.storage, [active("idsse_public"), active("skillcorner_public")], raw_archive=True)
    idsse = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )
    skillcorner = FakeAdapter(
        "skillcorner_public",
        "skillcorner",
        "public",
        [entry("skillcorner_public", "1899585")],
        {("1899585", "events"): b"s"},
    )
    _first_run(kit, [idsse, skillcorner], {"idsse": FakeParser("idsse"), "skillcorner": FakeParser("skillcorner")})
    before = {p: _bronze(kit, p) for p in ("idsse", "skillcorner")}
    fetched = [list(idsse.fetched), list(skillcorner.fetched)]
    newer = {"idsse": FakeParser("idsse", version="2"), "skillcorner": FakeParser("skillcorner")}
    code, [line] = _reparse_cli(kit, env, newer)("reparse", "--all")
    assert (code, line["status"], line["counts"]["changed"], line["counts"]["unchanged"]) == (0, "ok", 2, 1)
    assert sorted(newer["idsse"].parsed) == ["J03WMX", "J03WPY"] and newer["skillcorner"].parsed == []
    assert [idsse.fetched, skillcorner.fetched] == fetched  # no source call
    assert {p: _bronze(kit, p) for p in ("idsse", "skillcorner")} == before


def test_reparse_after_private_move(kit: LakeKit, env: dict[str, str]) -> None:
    save_settings(kit.storage, [active("gradientsports_pining")], raw_archive=True)
    adapter = FakeAdapter(
        "gradientsports_pining",
        "gradientsports",
        "pining",
        [entry("gradientsports_pining", "3812", competition=("38", "2022"))],
        {("3812", "events"): b"line one\nline two"},
    )
    parsers = {"gradientsports": FakeParser("gradientsports")}
    _first_run(kit, [adapter], parsers)
    with_entry(adapter, "3812", visibility="private")
    _first_run(kit, [adapter], parsers)  # the move to the restricted root
    before = _bronze(kit, "gradientsports")
    again = {"gradientsports": FakeParser("gradientsports")}
    code, [line] = _reparse_cli(kit, env, again)("reparse", "--match", "gradientsports:3812")
    assert (code, line["counts"]["changed"]) == (0, 1) and again["gradientsports"].parsed == ["3812"]
    assert _bronze(kit, "gradientsports") == before
    prefix = f"{match_key('gradientsports', '3812')}-"

    def names(root: str) -> list[str]:
        return [i.key.rsplit("/", 1)[-1] for i in kit.storage.list(root, "data/bronze/")]  # type: ignore[arg-type]

    assert not [n for n in names("lake") if n.startswith(prefix)]
    assert [n for n in names("restricted") if n.startswith(prefix)]  # the bronze stays in the restricted root
