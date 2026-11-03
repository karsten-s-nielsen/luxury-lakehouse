"""The `lakehouse` command (spec §10, §13.2, §17, §18, §19; plan Task 26).

Every subcommand calls the services layer or the catalog and run functions, never a store directly. Each one writes
one JSON line with its result to stdout (the logs are JSON lines too) and exits with:

- 0: done;
- 1: failed (a failed run, a `verify` problem, a guard stop, an integrity error);
- 2: usage, config or gating (a missing root, an invalid value, a command that HF allows only in a Job);
- 3: skipped (a run holds the lease: try again after it).

The roots resolve as a pair from `--lake` / `--restricted`, then `LAKEHOUSE_LAKE_ROOT` / `LAKEHOUSE_RESTRICTED_ROOT`,
then the bootstrap file that `lakehouse init` writes (§10.5). On HF, `restore`, `relocate` and `sweep --allow-large`
write catalog-control objects, and `sweep --dry-run` holds the lease and opens a working catalog like the sweep, so
they run only inside an HF Job that holds the gateway key (§29.9); without it they stop before the lake is opened.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import shutil
import socket
import sys
import tempfile
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path
from typing import Any, Final, TextIO, cast, get_args

from lakehouse.catalog.cleanup import CleanupPreview, CleanupReport, sweep_now, sweep_preview
from lakehouse.catalog.publish import roots_record
from lakehouse.catalog.relocate import relocate as relocate_lake
from lakehouse.catalog.restore import RestoreRecord
from lakehouse.catalog.restore import restore as restore_version
from lakehouse.catalog.verify import VerifyReport, verify_lake
from lakehouse.catalog.working import VersionInfo
from lakehouse.config.model import InstallKind, Settings, load_defaults
from lakehouse.errors import ConfigError, ConflictError, LakehouseError, MissingRootError, RootMismatchError
from lakehouse.ids.native import Provider
from lakehouse.limits import raise_open_file_limit, require_open_file_minimum
from lakehouse.parsers.base import ProviderParser
from lakehouse.runs.run import ReparseTarget, RunResult, Trigger, exit_code, new_run_id, reparse_matches, run_ingestion
from lakehouse.schedule.base import ScheduleBackend, backend_for_host
from lakehouse.services.account import AccountService
from lakehouse.services.review import ReviewService
from lakehouse.services.schedule import ScheduleService
from lakehouse.services.settings import SettingsService
from lakehouse.sources.base import ChannelAdapter
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV, resolve_encryption_key
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.roots import (
    LAKE_ROOT_ENV,
    RESTRICTED_ROOT_ENV,
    S3_ACCESS_KEY_ID_ENV,
    S3_SECRET_ACCESS_KEY_ENV,
    ResolvedRoots,
    default_bootstrap_file,
    init_lake,
    normalise_root,
    open_storage,
    resolve_roots,
    write_bootstrap,
)
from lakehouse.wiring import build_adapters, default_parsers

__all__ = ["Deps", "main"]

_PROVIDERS: Final[frozenset[str]] = frozenset(get_args(Provider))
_INIT_PROFILES: Final = ("laptop", "staging")  # `init` creates local roots only; the owner creates HF buckets
# The non-secret variables a scheduled task gets from the environment that installs it (§18), besides `LAKEHOUSE_*`.
_TASK_ENV: Final = frozenset({"UV_CACHE_DIR", "UV_PYTHON_INSTALL_DIR", "HF_HOME", "TMPDIR"})
# the commands that open a catalog: they stop below the open-file minimum (T12 spec D1)
_CATALOG_COMMANDS: Final = frozenset({"init", "run", "reparse", "verify", "restore", "relocate", "sweep", "decisions"})


class UsageError(ValueError):
    """A command that cannot run as given: exit code 2, with the message."""


def _prompt_password() -> str:
    first = getpass.getpass("Operator password: ")
    if getpass.getpass("Repeat the password: ") != first:
        raise UsageError("the two passwords differ")
    return first


@dataclass(frozen=True)
class Deps:
    """What the CLI calls: the real functions by default, fakes in the tests."""

    env: Mapping[str, str]
    stdout: TextIO
    stdin: TextIO
    open_storage: Callable[[ResolvedRoots, Mapping[str, str]], Storage] = open_storage
    init_lake: Callable[..., Storage] = init_lake
    run: Callable[..., RunResult] = run_ingestion
    reparse: Callable[..., RunResult] = reparse_matches
    restore: Callable[..., RestoreRecord] = restore_version
    relocate: Callable[..., VersionInfo] = relocate_lake
    sweep: Callable[..., CleanupReport] = sweep_now
    sweep_preview: Callable[..., CleanupPreview] = sweep_preview
    verify: Callable[[Storage], VerifyReport] = verify_lake
    adapters: Callable[..., Mapping[str, ChannelAdapter]] = build_adapters
    parsers: Callable[[], Mapping[Provider, ProviderParser]] = default_parsers
    schedule_backend: Callable[[], ScheduleBackend] = backend_for_host
    read_password: Callable[[], str] = _prompt_password


class _Context:
    """One command's view of the roots, the lake and the operator; the lake opens only when a command reads it."""

    def __init__(self, deps: Deps, args: argparse.Namespace) -> None:
        self.deps = deps
        self.env = deps.env
        self.args = args
        self.bootstrap_file = default_bootstrap_file(deps.env)

    def resolve(self, lake: str | None, restricted: str | None) -> ResolvedRoots:
        return resolve_roots(cli_lake=lake, cli_restricted=restricted, env=self.env, bootstrap_file=self.bootstrap_file)

    @cached_property
    def resolved(self) -> ResolvedRoots:
        return self.resolve(self.args.lake, self.args.restricted)

    @cached_property
    def storage(self) -> Storage:
        return self.deps.open_storage(self.resolved, self.env)

    @cached_property
    def operator(self) -> str:
        chosen: str | None = self.args.operator
        return chosen or self.env.get("USERNAME") or self.env.get("USER") or self.env.get("LOGNAME") or "cli"

    def settings_service(self) -> SettingsService:
        return SettingsService(self.storage, self.resolved)

    def saved_settings(self) -> Settings:
        return self.settings_service().load()[0]

    def emit(self, payload: Mapping[str, Any]) -> None:
        self.deps.stdout.write(json.dumps(payload, ensure_ascii=False, default=_json_value) + "\n")
        self.deps.stdout.flush()


def _json_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path | CanonicalKey):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not JSON")


def _require_gateway(ctx: _Context, command: str, *resolved: ResolvedRoots) -> None:
    """On HF, the catalog-control writers run only with the gateway key, which only an HF Job holds (§29.9)."""
    on_hf = any(r.roots.lake.hf_bucket is not None for r in resolved)
    if on_hf and not (ctx.env.get(S3_ACCESS_KEY_ID_ENV) and ctx.env.get(S3_SECRET_ACCESS_KEY_ENV)):
        raise UsageError(
            f"{command} writes catalog-control objects: on HF it runs only inside an HF Job that the owner launches "
            f"with the gateway key (the Job secrets {S3_ACCESS_KEY_ID_ENV} and {S3_SECRET_ACCESS_KEY_ENV}); "
            "nothing was written"
        )


def _work_dir(run_id: str) -> Path:
    """A private work folder (mode 0700) for one run; never the current folder, which a scheduled task may not own."""
    return Path(tempfile.mkdtemp(prefix=f"lakehouse-{run_id}-"))


def _holder(kind: str) -> str:
    return f"{kind}@{socket.gethostname()}:{os.getpid()}"


def _run_payload(command: str, result: RunResult) -> dict[str, Any]:
    return {
        "command": command,
        "status": result.status,
        "run_id": result.run_id,
        "counts": asdict(result.counts),
        "errors": [asdict(error) for error in result.errors],
    }


# --- the commands ----------------------------------------------------------------------------------------------------


def _init(ctx: _Context) -> int:
    args = ctx.args
    lake: str | None = args.init_lake or args.lake
    if not lake:
        raise UsageError("init needs the lake folder: lakehouse init --lake <path>")
    resolved = ctx.resolve(lake, args.init_restricted or args.restricted)
    key = resolve_encryption_key(resolved.roots, ctx.deps.env)  # also validates a set-but-bad key before creating
    storage = ctx.deps.init_lake(resolved, bootstrap_file=ctx.bootstrap_file)
    service = SettingsService(storage, resolved)
    try:
        version, config = service.load()[1], "kept"
    except FileNotFoundError:
        defaults = load_defaults(args.profile)
        version, config = service.save(defaults, expected_version=0, operator=ctx.operator), "saved"
    record = roots_record(resolved.roots)
    result: dict[str, Any] = {
        "command": "init",
        "status": "ok",
        "lake": record["lake"],
        "restricted": record["restricted"],
        "bootstrap_file": ctx.bootstrap_file,
        "profile": args.profile,
        "config": config,
        "config_version": version,
        "encrypted": key is not None,
    }
    if resolved.roots.lake.local_path is not None and key is None:
        result["warning"] = {
            "kind": "local_lake_unencrypted",
            "message": (
                f"this local lake is NOT encrypted at rest; set {ENCRYPTION_KEY_ENV} and re-create the lake to "
                "encrypt it"
            ),
        }
    ctx.emit(result)
    return 0


@contextmanager
def observe_run(storage: Any, run_id: str, env: Mapping[str, str]) -> Generator[None, None, None]:
    """Install a run's observability for the duration of one run (plan Task 10): secret masking, the OTel export
    provider that writes finished spans to the run's observability object, and the log collector that writes its
    masked log copy. ONLY the run entrypoints call this — never the web app, so the Space has no path to write the
    object (OBS-B32). The Space installs the `off`/read path only (it never runs ingestion, §17.1)."""
    import logging as _logging

    from lakehouse.logging import install_secret_masking, remove_secret_masking
    from lakehouse.observability import ObservabilityWriter, collect_logs
    from lakehouse.telemetry import install_providers
    from lakehouse.telemetry_export import LakeSpanExporter

    install_secret_masking(env)
    install_providers("export", exporter=LakeSpanExporter(ObservabilityWriter(storage, run_id)))
    handler = collect_logs(storage, run_id)
    root = _logging.getLogger("lakehouse")
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        install_providers("off")
        remove_secret_masking()


def _run(ctx: _Context) -> int:
    trigger: Trigger = ctx.args.trigger
    settings = ctx.saved_settings()
    adapters = ctx.deps.adapters(settings, env=ctx.env)
    run_id = new_run_id(datetime.now(UTC))
    work = _work_dir(run_id)
    try:
        with observe_run(ctx.storage, run_id, ctx.env):
            result = ctx.deps.run(
                storage=ctx.storage,
                trigger=trigger,
                adapters=adapters,
                parsers=ctx.deps.parsers(),
                run_id=run_id,
                holder_id=_holder(trigger),
                work_dir=work,
                env=ctx.env,
                settings=settings,
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)  # a scratch folder: the run has published or kept nothing in it
    ctx.emit(_run_payload("run", result))
    return exit_code(result)


def _reparse_target(text: str | None) -> ReparseTarget:
    if text is None:
        return "all"
    provider, _, native = text.partition(":")
    if provider not in _PROVIDERS or not native:
        raise UsageError(f"--match takes <provider>:<native match id>, with a provider of {sorted(_PROVIDERS)}")
    return (provider, native)  # type: ignore[return-value] - checked against the Provider literal above


def _reparse(ctx: _Context) -> int:
    target = _reparse_target(ctx.args.match)
    run_id = new_run_id(datetime.now(UTC))
    work = _work_dir(run_id)
    try:
        with observe_run(ctx.storage, run_id, ctx.env):
            result = ctx.deps.reparse(
                storage=ctx.storage,
                parsers=ctx.deps.parsers(),
                target=target,
                run_id=run_id,
                holder_id=_holder("reparse"),
                work_dir=work,
                env=ctx.env,
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)  # a scratch folder, as for a run
    ctx.emit(_run_payload("reparse", result))
    return exit_code(result)


def _verify(ctx: _Context) -> int:
    report = ctx.deps.verify(ctx.storage)
    status = "failed" if report.problems else "ok"
    ctx.emit({"command": "verify", "status": status, **asdict(report)})
    return 1 if report.problems else 0


def _restore(ctx: _Context) -> int:
    _require_gateway(ctx, "restore", ctx.resolved)
    record = ctx.deps.restore(
        ctx.storage,
        target_seq=ctx.args.to,
        operator=ctx.operator,
        settings=ctx.saved_settings(),
        env=ctx.env,
    )
    ctx.emit({"command": "restore", "status": "ok", **asdict(record)})
    return 0


def _relocate(ctx: _Context) -> int:
    args = ctx.args
    if args.lake or args.restricted:
        raise UsageError("relocate names its roots with --from and --to: leave out --lake and --restricted")
    old = ctx.resolve(args.source, args.source_restricted)
    new = ctx.resolve(args.target, args.target_restricted)
    _require_gateway(ctx, "relocate", old, new)
    storage = ctx.deps.open_storage(new, ctx.env)
    settings = SettingsService(storage, new).load()[0]
    info = ctx.deps.relocate(storage, old_roots=old.roots, new_roots=new.roots, settings=settings, env=ctx.env)
    bootstrap = "unchanged"
    try:
        stored = ctx.resolve(None, None) if not ctx.env.get(LAKE_ROOT_ENV) else None
    except MissingRootError:
        stored = None
    if stored is not None and _same(roots_record(stored.roots), roots_record(old.roots)):
        record = roots_record(new.roots)
        write_bootstrap(ctx.bootstrap_file, lake=record["lake"], restricted=record["restricted"])
        bootstrap = "updated"
    record = roots_record(new.roots)
    ctx.emit(
        {
            "command": "relocate",
            "status": "ok",
            "seq": info.seq,
            "lake": record["lake"],
            "restricted": record["restricted"],
            "bootstrap": bootstrap,
        }
    )
    return 0


def _same(left: Mapping[str, str], right: Mapping[str, str]) -> bool:
    return all(normalise_root(left[root]) == normalise_root(right[root]) for root in ("lake", "restricted"))


def _read_key_list(path: Path) -> frozenset[CanonicalKey]:
    """The `--expect` list: one canonical key per line, as `sweep --dry-run --list-file` writes it."""
    lines = [line.strip() for line in path.read_bytes().decode("utf-8").splitlines()]
    keys = frozenset(CanonicalKey.parse(line) for line in lines if line)
    if not keys:
        raise UsageError(f"the --expect list {path} has no key: write it with sweep --dry-run --list-file")
    return keys


def _sweep_preview(ctx: _Context) -> int:
    """`sweep --dry-run` (T12 plan Task 5 Step 3): what `sweep --allow-large` would delete; nothing changes."""
    args = ctx.args
    if args.allow_large or args.expect is not None:
        raise UsageError("sweep --dry-run changes nothing: leave out --allow-large and --expect")
    _require_gateway(ctx, "sweep --dry-run", ctx.resolved)
    preview = ctx.deps.sweep_preview(ctx.storage, settings=ctx.saved_settings(), env=ctx.env)
    keys = [str(key) for key in preview.keys()]
    if args.list_file is not None:
        Path(args.list_file).write_bytes("".join(f"{key}\n" for key in keys).encode("utf-8"))
    ctx.emit(
        {
            "command": "sweep",
            "status": "ok",
            "dry_run": True,
            "doomed": len(preview.doomed),
            "exempted": preview.exempted,
            "counted": preview.counted,
            "objects_listed": preview.objects_listed,
            "over_guard": preview.counted > preview.max_fraction * preview.objects_listed,
            "pruned_versions": preview.pruned_versions,
            "ducklake_listed": len(preview.ducklake_listed),
            "keys": keys,
            "list_file": args.list_file,
        }
    )
    return 0


def _sweep(ctx: _Context) -> int:
    args = ctx.args
    if args.dry_run:
        return _sweep_preview(ctx)
    if args.expect is not None and not args.allow_large:
        raise UsageError("--expect goes with sweep --allow-large: it bounds what the lifted guard may delete")
    if not args.allow_large:
        raise UsageError(
            "sweep runs only with --allow-large (or --dry-run, which deletes nothing): a run's own cleanup already "
            "sweeps under the 10 % guard; check the list of the stopped sweep first"
        )
    if args.list_file is not None:
        raise UsageError("--list-file goes with sweep --dry-run; sweep --allow-large reads a list with --expect")
    expected = _read_key_list(Path(args.expect)) if args.expect is not None else None
    _require_gateway(ctx, "sweep --allow-large", ctx.resolved)
    report = ctx.deps.sweep(ctx.storage, settings=ctx.saved_settings(), env=ctx.env, expected=expected)
    ctx.emit(
        {
            "command": "sweep",
            "status": "ok",
            "swept": [str(key) for key in report.swept],
            "pruned_versions": report.pruned_versions,
            "ducklake_deleted": len(report.ducklake_deleted),
            "expired_snapshots": report.expired_snapshots,
        }
    )
    return 0


def _task_env(ctx: _Context) -> dict[str, str]:
    """The task's variables: the allowlisted ones of this environment, and the roots of this lake (so the task runs on
    the lake whose config holds the schedule, whichever source named it here)."""
    # The encryption key is a secret (§20): it never goes into a task definition. The operator supplies it to the
    # scheduled process through platform ops-config (owner decision 2), not here.
    chosen = {
        key: value
        for key, value in ctx.env.items()
        if (key.startswith("LAKEHOUSE_") or key in _TASK_ENV) and key != ENCRYPTION_KEY_ENV
    }
    record = roots_record(ctx.resolved.roots)
    chosen[LAKE_ROOT_ENV] = record["lake"]
    chosen[RESTRICTED_ROOT_ENV] = record["restricted"]
    return chosen


def _schedule(ctx: _Context) -> int:
    args = ctx.args
    if args.system and not args.run_as:
        raise UsageError("--system needs --run-as <user>: a system-level task runs as the operator, never as root")
    action: str = args.action
    install = action == "install"
    service = ScheduleService(
        ctx.settings_service() if install else None,
        ctx.deps.schedule_backend(),
        env=_task_env(ctx) if install else {},
        system=args.system,
        run_as=args.run_as,
    )
    status = service.install() if install else service.uninstall() if action == "uninstall" else service.get()
    ctx.emit({"command": "schedule", "status": "ok", "action": action, **asdict(status)})
    return 0


def _set_value(settings: Settings, key: str, text: str) -> Settings:
    """`config set <key> <value>`: a dotted key (`modules.<module id>.<field>` for a module), a JSON value or text."""
    data = settings.model_dump(mode="json")
    parts = key.split(".")
    node: dict[str, Any] = data
    if parts[0] == "modules":
        if len(parts) != 3:
            raise UsageError("a module setting is modules.<module id>.<field>, for example modules.idsse_public.active")
        found: dict[str, Any] | None = next((m for m in data["modules"] if m["module_id"] == parts[1]), None)
        if found is None:
            raise UsageError(f"no module {parts[1]!r}")
        node, parts = found, parts[2:]
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            raise UsageError(f"no setting {key!r}")
        node = cast(dict[str, Any], child)
    if parts[-1] not in node:
        raise UsageError(f"no setting {key!r}")
    try:
        value: Any = json.loads(text)
    except json.JSONDecodeError:
        value = text  # plain text, for example a cron expression
    node[parts[-1]] = value
    return Settings.model_validate(data)


def _config(ctx: _Context) -> int:
    args = ctx.args
    service = ctx.settings_service()
    settings, version = service.load()
    if args.action == "show":
        ctx.emit(
            {"command": "config", "status": "ok", "version": version, "settings": settings.model_dump(mode="json")}
        )
        return 0
    defaults: InstallKind | None = args.defaults
    if defaults is not None:
        if args.key is not None:
            raise UsageError("config set takes --defaults <kind> or <key> <value>, not both")
        draft = load_defaults(defaults)
    elif args.key is None or args.value is None:
        raise UsageError("config set takes <key> <value>, or --defaults <kind>")
    else:
        draft = _set_value(settings, args.key, args.value)
    saved = service.save(draft, expected_version=version, operator=ctx.operator)
    ctx.emit({"command": "config", "status": "ok", "version": saved})
    return 0


def _decisions(ctx: _Context) -> int:
    review = ReviewService(ctx.storage)
    path = Path(ctx.args.file)
    if ctx.args.action == "export":
        ctx.emit({"command": "decisions", "status": "ok", "exported": review.export(path), "file": ctx.args.file})
    else:
        ctx.emit({"command": "decisions", "status": "ok", "imported": review.import_(path), "file": ctx.args.file})
    return 0


def _account(ctx: _Context) -> int:
    args = ctx.args
    if ctx.resolved.roots.lake.hf_bucket is not None:
        raise UsageError("the HF Space signs in with Hugging Face: a local operator account is for a local install")
    password = ctx.deps.stdin.readline().rstrip("\r\n") if args.password_stdin else ctx.deps.read_password()
    try:
        AccountService(ctx.storage).create(args.username, password, replace=args.replace)
    except ConflictError as exc:
        raise UsageError(str(exc)) from exc
    ctx.emit({"command": "account", "status": "ok", "username": args.username})
    return 0


# --- the parser and the entry ----------------------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse", description="The v2 lakehouse: ingestion, catalog and schedule.")
    parser.add_argument("--lake", help="the lake root: a folder, or hf://buckets/<ns>/<name>")
    parser.add_argument("--restricted", help="the restricted root (default: the folder beside the lake root)")
    parser.add_argument("--operator", help="the name recorded for a save or a restore (default: the login name)")
    commands = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    init = commands.add_parser("init", help="create the local roots, write the bootstrap file, save the defaults")
    init.add_argument("--lake", dest="init_lake", help="the lake folder to create")
    init.add_argument("--restricted", dest="init_restricted", help="the restricted folder to create")
    init.add_argument("--profile", choices=_INIT_PROFILES, default="laptop", help="the install defaults to save")
    init.set_defaults(handler=_init)

    run = commands.add_parser("run", help="run the ingestion once")
    run.add_argument("--trigger", choices=get_args(Trigger), default="cli")
    run.set_defaults(handler=_run)

    restore = commands.add_parser("restore", help="point CURRENT at a copy of a kept catalog version")
    restore.add_argument("--to", type=int, required=True, help="the catalog version to restore")
    restore.set_defaults(handler=_restore)

    commands.add_parser("verify", help="check every kept catalog version in full").set_defaults(handler=_verify)

    relocate = commands.add_parser("relocate", help="rewrite the catalog's file paths after the lake moved")
    relocate.add_argument("--from", dest="source", required=True, help="the old lake root")
    relocate.add_argument("--from-restricted", dest="source_restricted", help="the old restricted root")
    relocate.add_argument("--to", dest="target", required=True, help="the new lake root")
    relocate.add_argument("--to-restricted", dest="target_restricted", help="the new restricted root")
    relocate.set_defaults(handler=_relocate)

    reparse = commands.add_parser("reparse", help="parse again from the raw archive, offline")
    which = reparse.add_mutually_exclusive_group(required=True)
    which.add_argument("--match", help="one match: <provider>:<native match id>")
    which.add_argument("--all", action="store_true", help="every match whose parser version changed")
    reparse.set_defaults(handler=_reparse)

    sweep = commands.add_parser("sweep", help="sweep orphans with the large-sweep guard lifted")
    sweep.add_argument("--allow-large", action="store_true", help="lift the guard after checking the list")
    sweep.add_argument("--dry-run", action="store_true", help="list what the sweep would delete; delete nothing")
    sweep.add_argument("--list-file", help="with --dry-run: write the keys to delete to this file, one per line")
    sweep.add_argument("--expect", help="with --allow-large: stop before any delete on a key not in this list file")
    sweep.set_defaults(handler=_sweep)

    schedule = commands.add_parser("schedule", help="install, uninstall or show the native OS task")
    schedule.add_argument("action", choices=("install", "uninstall", "status"))
    schedule.add_argument("--system", action="store_true", help="a system-level task (an always-on machine)")
    schedule.add_argument("--run-as", help="the operator account a system-level task runs as (user or user:group)")
    schedule.set_defaults(handler=_schedule)

    config = commands.add_parser("config", help="show or change the saved config")
    config_actions = config.add_subparsers(dest="action", required=True, metavar="<action>")
    config_actions.add_parser("show", help="the saved config and its version")
    config_set = config_actions.add_parser("set", help="change one value, or save the defaults of an install kind")
    config_set.add_argument("key", nargs="?", help="a dotted key, for example schedule.cron")
    config_set.add_argument("value", nargs="?", help="a JSON value, or plain text")
    config_set.add_argument("--defaults", choices=get_args(InstallKind), help="save these install defaults")
    config.set_defaults(handler=_config)

    decisions = commands.add_parser("decisions", help="export or import the review decisions")
    decisions_actions = decisions.add_subparsers(dest="action", required=True, metavar="<action>")
    decisions_actions.add_parser("export", help="write the decisions to a JSON file").add_argument("file")
    decisions_actions.add_parser("import", help="add the decisions of an exported file").add_argument("file")
    decisions.set_defaults(handler=_decisions)

    account = commands.add_parser("account", help="the local operator account")
    account_actions = account.add_subparsers(dest="action", required=True, metavar="<action>")
    create = account_actions.add_parser("create", help="create (or with --replace, replace) the operator account")
    create.add_argument("username")
    create.add_argument("--password-stdin", action="store_true", help="read the password from the first stdin line")
    create.add_argument("--replace", action="store_true", help="replace an existing account")
    account.set_defaults(handler=_account)
    return parser


def main(argv: Sequence[str] | None = None, *, deps: Deps | None = None) -> int:
    limit = raise_open_file_limit()  # first, at every entry point (T12 spec D1)
    chosen = deps if deps is not None else Deps(env=os.environ, stdout=sys.stdout, stdin=sys.stdin)
    try:
        args = _parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    except SystemExit as exc:  # argparse wrote the usage and the error to stderr
        return 0 if exc.code in (0, None) else 2
    ctx = _Context(chosen, args)
    handler: Callable[[_Context], int] = args.handler
    try:
        if args.command in _CATALOG_COMMANDS:
            require_open_file_minimum(limit)
        return handler(ctx)
    except ConflictError as exc:  # a run holds the lease
        return _failure(ctx, "skipped", exc, 3)
    except (ValueError, ConfigError, MissingRootError, RootMismatchError, FileNotFoundError, PermissionError) as exc:
        return _failure(ctx, "error", exc, 2)
    except LakehouseError as exc:  # a guard stop, an integrity error, a lost lease, a stale save or catalog
        return _failure(ctx, "failed", exc, 1)


def _failure(ctx: _Context, status: str, exc: BaseException, code: int) -> int:
    ctx.emit({"command": ctx.args.command, "status": status, "message": str(exc)})
    return code


if __name__ == "__main__":
    sys.exit(main())
