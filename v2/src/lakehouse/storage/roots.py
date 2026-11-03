"""Storage root resolution, the bootstrap file and opening a storage (spec §10.5; plan Task 8).

The roots cannot live in the settings, because the settings live in the lake root. So the two roots resolve as a
pair from one source: the CLI flags `--lake` / `--restricted`, then the environment variables `LAKEHOUSE_LAKE_ROOT`
/ `LAKEHOUSE_RESTRICTED_ROOT`, then the bootstrap file that `lakehouse init` writes. The restricted root comes only
from the source that named the lake root, so a lake never pairs with the restricted root of another lake.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from pydantic import SecretStr

from lakehouse.errors import ConfigError, MissingRootError
from lakehouse.storage.base import Storage
from lakehouse.storage.hf import HfStorage, S3Credentials
from lakehouse.storage.keys import ROOTS, RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

__all__ = [
    "BOOTSTRAP_FILE_NAME",
    "ResolvedRoots",
    "RootSource",
    "default_bootstrap_file",
    "init_lake",
    "normalise_root",
    "open_storage",
    "resolve_roots",
    "write_bootstrap",
]

RootSource = Literal["cli", "env", "bootstrap"]

BOOTSTRAP_FILE_NAME: Final = "bootstrap.json"
LAKE_ROOT_ENV: Final = "LAKEHOUSE_LAKE_ROOT"
RESTRICTED_ROOT_ENV: Final = "LAKEHOUSE_RESTRICTED_ROOT"
LAKE_S3_PATH_ENV: Final = "LAKEHOUSE_LAKE_S3_PATH"
RESTRICTED_S3_PATH_ENV: Final = "LAKEHOUSE_RESTRICTED_S3_PATH"
CONTROL_REPO_ENV: Final = "LAKEHOUSE_CONTROL_REPO"
CONTROL_REPO_SUFFIX: Final = "-control"
CONFIG_DIR_ENV: Final = "LAKEHOUSE_CONFIG_DIR"
HF_TOKEN_ENV: Final = "HF_TOKEN"  # noqa: S105 - the name of an environment variable, not a secret
S3_ACCESS_KEY_ID_ENV: Final = "HF_S3_ACCESS_KEY_ID"
S3_SECRET_ACCESS_KEY_ENV: Final = "HF_S3_SECRET_ACCESS_KEY"  # noqa: S105 - an environment variable name
_BOOTSTRAP_SCHEMA_VERSION: Final = 1
_HF_ROOT: Final = re.compile(r"^hf://buckets/([A-Za-z0-9][A-Za-z0-9._-]*)/([A-Za-z0-9][A-Za-z0-9._-]*)$")
_NO_ROOT_MESSAGE: Final = "no lake root configured: run lakehouse init --lake <path>"


@dataclass(frozen=True)
class ResolvedRoots:
    roots: RootMap
    source: RootSource
    control_repo: str | None = None  # the HF lease ledger repository of a writer (T6 spec D1)


def default_bootstrap_file(env: Mapping[str, str]) -> Path:
    """`bootstrap.json` in the user config folder, or in `LAKEHOUSE_CONFIG_DIR` when that is set."""
    override = env.get(CONFIG_DIR_ENV)
    if override:
        return Path(override) / BOOTSTRAP_FILE_NAME
    if sys.platform == "win32":
        appdata = env.get("APPDATA")
        base = Path(appdata) if appdata else Path.home() / "AppData" / "Roaming"
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        xdg = env.get("XDG_CONFIG_HOME")
        base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "lakehouse" / BOOTSTRAP_FILE_NAME


def write_bootstrap(path: Path, *, lake: str, restricted: str | None) -> None:
    """Write the bootstrap file atomically, as UTF-8 JSON (non-ASCII paths kept as they are)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": _BOOTSTRAP_SCHEMA_VERSION, "lake": lake, "restricted": restricted}
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".bootstrap-")
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=False, indent=2)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _read_bootstrap(path: Path) -> tuple[str, str | None] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"bootstrap file {path} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"bootstrap file {path} must hold a JSON object")
    lake = payload.get("lake")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - JSON
    restricted = payload.get("restricted")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - JSON
    if payload.get("schema_version") != _BOOTSTRAP_SCHEMA_VERSION:  # pyright: ignore[reportUnknownMemberType] - JSON
        raise ValueError(f"bootstrap file {path} has an unknown schema_version")
    if not isinstance(lake, str) or not lake:
        raise ValueError(f"bootstrap file {path} names no lake root")
    if restricted is not None and not isinstance(restricted, str):
        raise ValueError(f"bootstrap file {path} has an invalid restricted root")
    return lake, restricted or None


def _is_hf(value: str) -> bool:
    return value.startswith("hf://")


def _hf_bucket(value: str) -> str:
    match = _HF_ROOT.match(value.rstrip("/"))
    if match is None:
        raise ValueError(f"invalid HF root {value!r}: expected hf://buckets/<namespace>/<bucket>")
    return f"{match.group(1)}/{match.group(2)}"


def _local_path(value: str) -> Path:
    return Path(value).expanduser().absolute()


def _sibling_restricted(lake: Path) -> Path:
    if not lake.name:
        raise ValueError(f"lake root {lake} is a drive or file-system root: name a folder")
    return lake.with_name(lake.name + "-restricted")


def _s3_paths(env: Mapping[str, str]) -> tuple[str | None, str | None]:
    """The gateway S3 paths: required on HF when gateway credentials are present (the Job), else ignored."""
    if not (env.get(S3_ACCESS_KEY_ID_ENV) or env.get(S3_SECRET_ACCESS_KEY_ENV)):
        return None, None
    missing = [name for name in (LAKE_S3_PATH_ENV, RESTRICTED_S3_PATH_ENV) if not env.get(name)]
    if missing:
        raise MissingRootError(f"gateway credentials are set, so {' and '.join(missing)} must be set too")
    lake, restricted = env[LAKE_S3_PATH_ENV], env[RESTRICTED_S3_PATH_ENV]
    for value in (lake, restricted):
        if not value.startswith("s3://"):
            raise ValueError(f"invalid gateway S3 path {value!r}: it must start with s3://")
    return lake, restricted


def _check_s3_path(variable: str, s3_path: str | None, hf_bucket: str) -> None:
    """The gateway names an object `s3://<namespace>/<name>/<key>` (the namespace is the gateway bucket), so an S3
    path must be exactly the one its `hf://buckets/<namespace>/<name>` root implies. Any other value writes elsewhere
    and fails only after the run starts (audit T4: the daily job ran with `s3://lake`)."""
    if s3_path is None:
        return
    expected = f"s3://{hf_bucket}"
    if s3_path.rstrip("/") != expected:
        raise ConfigError(f"{variable} is {s3_path!r}, but its hf:// root implies {expected!r}: set it to {expected}")


def _control_repo(env: Mapping[str, str], lake_bucket: str) -> str | None:
    """The lease ledger repository of an HF writer (gateway credentials present, the Job): required, and exactly
    `<lake bucket>-control` (T6 spec D1). A reader without gateway credentials needs none."""
    if not (env.get(S3_ACCESS_KEY_ID_ENV) or env.get(S3_SECRET_ACCESS_KEY_ENV)):
        return None
    value = env.get(CONTROL_REPO_ENV)
    if not value:
        raise MissingRootError(f"gateway credentials are set, so {CONTROL_REPO_ENV} must name the lease ledger too")
    expected = lake_bucket + CONTROL_REPO_SUFFIX
    if value != expected:
        raise ConfigError(f"{CONTROL_REPO_ENV} is {value!r}, but the lake {lake_bucket} implies {expected!r}")
    return value


def resolve_roots(
    *, cli_lake: str | None, cli_restricted: str | None, env: Mapping[str, str], bootstrap_file: Path
) -> ResolvedRoots:
    """Resolve the lake and restricted roots as a pair from the first source that names a lake root."""
    source: RootSource
    if cli_lake:
        source, lake, restricted = "cli", cli_lake, cli_restricted or None
    elif cli_restricted:
        raise ValueError("--restricted needs --lake: the restricted root resolves only with its lake root")
    elif env.get(LAKE_ROOT_ENV):
        source, lake, restricted = "env", env[LAKE_ROOT_ENV], env.get(RESTRICTED_ROOT_ENV) or None
    elif env.get(RESTRICTED_ROOT_ENV):
        raise ValueError(
            f"{RESTRICTED_ROOT_ENV} needs {LAKE_ROOT_ENV}: the restricted root resolves only with its lake"
        )
    else:
        stored = _read_bootstrap(bootstrap_file)
        if stored is None:
            raise MissingRootError(_NO_ROOT_MESSAGE)
        source, (lake, restricted) = "bootstrap", stored
    if restricted is not None and _is_hf(lake) != _is_hf(restricted):
        raise ValueError("the lake and restricted roots must be the same kind: both local paths or both hf:// buckets")
    if _is_hf(lake):
        if restricted is None:
            raise MissingRootError(
                f"an HF lake needs its restricted root named explicitly (--restricted or {RESTRICTED_ROOT_ENV})"
            )
        lake_s3, restricted_s3 = _s3_paths(env)
        lake_bucket, restricted_bucket = _hf_bucket(lake), _hf_bucket(restricted)
        _check_s3_path(LAKE_S3_PATH_ENV, lake_s3, lake_bucket)
        _check_s3_path(RESTRICTED_S3_PATH_ENV, restricted_s3, restricted_bucket)
        roots = RootMap(
            lake=StorageRootInfo(local_path=None, hf_bucket=lake_bucket, s3_bucket_path=lake_s3),
            restricted=StorageRootInfo(local_path=None, hf_bucket=restricted_bucket, s3_bucket_path=restricted_s3),
        )
        return ResolvedRoots(roots=roots, source=source, control_repo=_control_repo(env, lake_bucket))
    lake_path = _local_path(lake)
    restricted_path = _local_path(restricted) if restricted is not None else _sibling_restricted(lake_path)
    roots = RootMap(
        lake=StorageRootInfo(local_path=lake_path, hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=restricted_path, hf_bucket=None, s3_bucket_path=None),
    )
    return ResolvedRoots(roots=roots, source=source)


def _case_insensitive(path: Path) -> bool:
    """Windows paths always compare without case. On macOS, probe the volumes that `path` crosses."""
    if sys.platform == "win32":
        return True
    if sys.platform != "darwin":
        return False
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    text = str(probe)
    swapped = text.swapcase()
    if swapped == text:
        return True  # no cased letters to probe; APFS and HFS+ default to case-insensitive
    return os.path.exists(swapped) and os.path.samefile(text, swapped)


def normalise_root(value: str) -> str:
    """The form in which two root values compare: resolved, `/` separators, no trailing separator, case-folded
    where the file system ignores case. An `hf://` URI loses a trailing `/` only."""
    if _is_hf(value):
        return value.rstrip("/")
    resolved = Path(value).expanduser().resolve()
    text = resolved.as_posix()
    if len(text) > 1:
        text = text.rstrip("/")
    return text.casefold() if _case_insensitive(resolved) else text


def _credentials(env: Mapping[str, str]) -> S3Credentials | None:
    key_id, secret = env.get(S3_ACCESS_KEY_ID_ENV), env.get(S3_SECRET_ACCESS_KEY_ENV)
    if not key_id and not secret:
        return None
    if not key_id or not secret:
        raise ValueError(f"set both {S3_ACCESS_KEY_ID_ENV} and {S3_SECRET_ACCESS_KEY_ENV}, or neither")
    return S3Credentials(access_key_id=SecretStr(key_id), secret_access_key=SecretStr(secret))


def open_storage(resolved: ResolvedRoots, env: Mapping[str, str]) -> Storage:
    """Open the storage of resolved roots. A missing local root raises `MissingRootError` and creates nothing."""
    roots = resolved.roots
    if roots.lake.hf_bucket is not None:
        token = env.get(HF_TOKEN_ENV)
        if not token:
            raise ValueError(f"an HF lake needs {HF_TOKEN_ENV}")
        storage = HfStorage(
            roots, hf_token=SecretStr(token), s3_key=_credentials(env), control_repo=resolved.control_repo
        )
        ledger = storage.hub_ledger
        if ledger is not None:
            ledger.check()  # the control repository exists and this token sees it, before any work (T6 spec D1)
        return storage
    for root in ROOTS:
        path = roots.info(root).local_path
        if path is None or not path.is_dir():
            raise MissingRootError(
                f"the {root} root {path} does not exist: reconnect its drive, or run lakehouse init --lake <path>"
            )
    return LocalStorage(roots)


def init_lake(resolved: ResolvedRoots, *, bootstrap_file: Path) -> Storage:
    """Create both local roots and write the bootstrap file. The only function that creates roots."""
    roots = resolved.roots
    lake, restricted = roots.lake.local_path, roots.restricted.local_path
    if lake is None or restricted is None:
        raise ValueError("lakehouse init creates local roots only; the owner creates HF buckets (spec §10.5)")
    lake.mkdir(parents=True, exist_ok=True)
    restricted.mkdir(parents=True, exist_ok=True)
    write_bootstrap(bootstrap_file, lake=str(lake), restricted=str(restricted))
    return LocalStorage(roots)
