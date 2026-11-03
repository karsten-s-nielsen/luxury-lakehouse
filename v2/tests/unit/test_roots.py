"""Storage root resolution (spec §10.5; plan Task 8): the two roots resolve as a pair from one source."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from lakehouse.errors import ConfigError, MissingRootError
from lakehouse.storage.roots import (
    ResolvedRoots,
    default_bootstrap_file,
    init_lake,
    normalise_root,
    resolve_roots,
    write_bootstrap,
)


def _bootstrap(tmp_path: Path, lake: str, restricted: str | None) -> Path:
    path = tmp_path / "cfg" / "bootstrap.json"
    write_bootstrap(path, lake=lake, restricted=restricted)
    return path


def test_roots_precedence(tmp_path: Path) -> None:
    boot = _bootstrap(tmp_path, str(tmp_path / "A"), str(tmp_path / "A-r"))
    env = {"LAKEHOUSE_LAKE_ROOT": str(tmp_path / "E"), "LAKEHOUSE_RESTRICTED_ROOT": str(tmp_path / "E-r")}
    cli = resolve_roots(
        cli_lake=str(tmp_path / "C"), cli_restricted=str(tmp_path / "C-r"), env=env, bootstrap_file=boot
    )
    assert cli.source == "cli"
    assert cli.roots.lake.local_path == tmp_path / "C"
    assert cli.roots.restricted.local_path == tmp_path / "C-r"
    from_env = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=boot)
    assert from_env.source == "env"
    assert from_env.roots.lake.local_path == tmp_path / "E"
    from_boot = resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=boot)
    assert from_boot.source == "bootstrap"
    assert from_boot.roots.restricted.local_path == tmp_path / "A-r"


def test_roots_resolve_as_pair(tmp_path: Path) -> None:
    boot = _bootstrap(tmp_path, str(tmp_path / "A"), str(tmp_path / "A-restricted"))
    lake_b = tmp_path / "LakeB"
    resolved = resolve_roots(cli_lake=str(lake_b), cli_restricted=None, env={}, bootstrap_file=boot)
    assert resolved.roots.lake.local_path == lake_b
    assert resolved.roots.restricted.local_path == tmp_path / "LakeB-restricted"
    env = {"LAKEHOUSE_LAKE_ROOT": str(lake_b)}
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=boot)
    assert resolved.roots.restricted.local_path == tmp_path / "LakeB-restricted"


def test_restricted_without_lake_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="restricted"):
        resolve_roots(cli_lake=None, cli_restricted=str(tmp_path / "R"), env={}, bootstrap_file=tmp_path / "none.json")
    with pytest.raises(ValueError, match="restricted"):
        resolve_roots(
            cli_lake=None,
            cli_restricted=None,
            env={"LAKEHOUSE_RESTRICTED_ROOT": str(tmp_path / "R")},
            bootstrap_file=tmp_path / "none.json",
        )


def test_restricted_defaults_to_sibling_locally(tmp_path: Path) -> None:
    resolved = resolve_roots(cli_lake=str(tmp_path / "L"), cli_restricted=None, env={}, bootstrap_file=tmp_path / "x")
    assert resolved.roots.restricted.local_path == tmp_path / "L-restricted"


def test_hf_requires_explicit_restricted(tmp_path: Path) -> None:
    with pytest.raises(MissingRootError, match="restricted"):
        resolve_roots(cli_lake="hf://buckets/ns/lake", cli_restricted=None, env={}, bootstrap_file=tmp_path / "x")
    resolved = resolve_roots(
        cli_lake="hf://buckets/ns/lake",
        cli_restricted="hf://buckets/ns/restricted",
        env={},
        bootstrap_file=tmp_path / "x",
    )
    assert resolved.roots.lake.hf_bucket == "ns/lake"
    assert resolved.roots.restricted.hf_bucket == "ns/restricted"
    assert resolved.roots.lake.local_path is None


def test_mixed_root_kinds_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="same kind"):
        resolve_roots(
            cli_lake=str(tmp_path / "L"), cli_restricted="hf://buckets/ns/r", env={}, bootstrap_file=tmp_path / "x"
        )


def test_job_requires_s3_paths(tmp_path: Path) -> None:
    env = {
        "LAKEHOUSE_LAKE_ROOT": "hf://buckets/ns/lake",
        "LAKEHOUSE_RESTRICTED_ROOT": "hf://buckets/ns/restricted",
        "HF_S3_ACCESS_KEY_ID": "AKIAEXAMPLEKEY",
        "HF_S3_SECRET_ACCESS_KEY": "example-secret-value",
        "LAKEHOUSE_CONTROL_REPO": "ns/lake-control",  # T6: a writer also names its lease ledger
    }
    with pytest.raises(MissingRootError, match="LAKEHOUSE_LAKE_S3_PATH"):
        resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    # the gateway form is s3://<namespace>/<name> (the namespace is the gateway bucket; load test, 2026-10-31)
    env |= {"LAKEHOUSE_LAKE_S3_PATH": "s3://ns/lake", "LAKEHOUSE_RESTRICTED_S3_PATH": "s3://ns/restricted"}
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    assert resolved.roots.lake.s3_bucket_path == "s3://ns/lake"
    assert resolved.roots.restricted.s3_bucket_path == "s3://ns/restricted"


_JOB_ENV = {
    "LAKEHOUSE_LAKE_ROOT": "hf://buckets/ns/lake",
    "LAKEHOUSE_RESTRICTED_ROOT": "hf://buckets/ns/restricted",
    "HF_S3_ACCESS_KEY_ID": "AKIAEXAMPLEKEY",
    "HF_S3_SECRET_ACCESS_KEY": "example-secret-value",
}


@pytest.mark.parametrize(
    ("variable", "value", "expected"),
    [
        ("LAKEHOUSE_LAKE_S3_PATH", "s3://lake", "s3://ns/lake"),  # the daily job's value (audit T4, 2026-10-31)
        ("LAKEHOUSE_LAKE_S3_PATH", "s3://ns--lake", "s3://ns/lake"),
        ("LAKEHOUSE_LAKE_S3_PATH", "s3://ns/restricted", "s3://ns/lake"),
        ("LAKEHOUSE_RESTRICTED_S3_PATH", "s3://ns/lake", "s3://ns/restricted"),
        ("LAKEHOUSE_RESTRICTED_S3_PATH", "s3://other/restricted", "s3://ns/restricted"),
    ],
)
def test_s3_path_must_match_its_hf_root(tmp_path: Path, variable: str, value: str, expected: str) -> None:
    """Audit T4: an S3 path that names another object than its `hf://` root fails before any work starts, with a
    `ConfigError` that names both values."""
    env = _JOB_ENV | {"LAKEHOUSE_LAKE_S3_PATH": "s3://ns/lake", "LAKEHOUSE_RESTRICTED_S3_PATH": "s3://ns/restricted"}
    env[variable] = value
    with pytest.raises(ConfigError) as raised:
        resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    message = str(raised.value)
    assert variable in message and value in message and expected in message
    assert "example-secret-value" not in message


_CONTROL_ENV = _JOB_ENV | {
    "LAKEHOUSE_LAKE_S3_PATH": "s3://ns/lake",
    "LAKEHOUSE_RESTRICTED_S3_PATH": "s3://ns/restricted",
}


def test_control_repo_required_with_gateway_credentials(tmp_path: Path) -> None:
    """T6 spec D1: an HF writer (gateway credentials present) needs its lease ledger repository."""
    with pytest.raises(MissingRootError, match="LAKEHOUSE_CONTROL_REPO"):
        resolve_roots(cli_lake=None, cli_restricted=None, env=_CONTROL_ENV, bootstrap_file=tmp_path / "x")
    env = _CONTROL_ENV | {"LAKEHOUSE_CONTROL_REPO": "ns/lake-control"}
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    assert resolved.control_repo == "ns/lake-control"


def test_control_repo_must_match_the_lake(tmp_path: Path) -> None:
    env = _CONTROL_ENV | {"LAKEHOUSE_CONTROL_REPO": "ns/other-control"}
    with pytest.raises(ConfigError) as raised:
        resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    assert "ns/other-control" in str(raised.value) and "ns/lake-control" in str(raised.value)


def test_local_lake_has_no_control_repo(tmp_path: Path) -> None:
    resolved = resolve_roots(cli_lake=str(tmp_path / "L"), cli_restricted=None, env={}, bootstrap_file=tmp_path / "x")
    assert resolved.control_repo is None
    reader = {k: v for k, v in _CONTROL_ENV.items() if not k.startswith("HF_S3_")}  # the Space: no gateway key
    as_reader = resolve_roots(cli_lake=None, cli_restricted=None, env=reader, bootstrap_file=tmp_path / "x")
    assert as_reader.control_repo is None


def test_s3_path_trailing_slash_accepted(tmp_path: Path) -> None:
    env = _JOB_ENV | {
        "LAKEHOUSE_LAKE_S3_PATH": "s3://ns/lake/",
        "LAKEHOUSE_RESTRICTED_S3_PATH": "s3://ns/restricted/",
        "LAKEHOUSE_CONTROL_REPO": "ns/lake-control",
    }
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    assert resolved.roots.lake.s3_bucket_path == "s3://ns/lake/"


def test_no_root_configured_fails(tmp_path: Path) -> None:
    with pytest.raises(MissingRootError, match="lakehouse init --lake"):
        resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=tmp_path / "absent.json")


def test_bootstrap_unicode_path_roundtrip(tmp_path: Path) -> None:
    lake = tmp_path / "My Lake Lukáš"
    boot = _bootstrap(tmp_path, str(lake), None)
    assert json.loads(boot.read_text(encoding="utf-8"))["lake"] == str(lake)
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=boot)
    assert resolved.roots.lake.local_path == lake
    assert resolved.roots.restricted.local_path == tmp_path / "My Lake Lukáš-restricted"


def test_config_dir_override(tmp_path: Path) -> None:
    env = {"LAKEHOUSE_CONFIG_DIR": str(tmp_path / "config-home")}
    boot = default_bootstrap_file(env)
    assert boot == tmp_path / "config-home" / "bootstrap.json"
    default = default_bootstrap_file({})
    assert default.name == "bootstrap.json"
    assert "lakehouse" in default.parts
    assert default != boot
    created = resolve_roots(cli_lake=str(tmp_path / "L"), cli_restricted=None, env=env, bootstrap_file=boot)
    init_lake(created, bootstrap_file=boot)
    assert sorted(p.name for p in (tmp_path / "config-home").iterdir()) == ["bootstrap.json"]
    read_back = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=default_bootstrap_file(env))
    assert read_back.source == "bootstrap"
    assert read_back.roots == created.roots


def test_init_refuses_hf_roots(tmp_path: Path) -> None:
    resolved = resolve_roots(
        cli_lake="hf://buckets/ns/lake", cli_restricted="hf://buckets/ns/r", env={}, bootstrap_file=tmp_path / "x"
    )
    with pytest.raises(ValueError, match="local roots only"):
        init_lake(ResolvedRoots(roots=resolved.roots, source="cli"), bootstrap_file=tmp_path / "b.json")
    assert not (tmp_path / "b.json").exists()


def test_bootstrap_corrupt_fails_loudly(tmp_path: Path) -> None:
    boot = tmp_path / "bootstrap.json"
    boot.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError, match="not valid JSON"):
        resolve_roots(cli_lake=None, cli_restricted=None, env={}, bootstrap_file=boot)


def test_invalid_hf_root_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hf://buckets/<namespace>/<bucket>"):
        resolve_roots(cli_lake="hf://buckets/ns", cli_restricted="hf://buckets/ns/r", env={}, bootstrap_file=tmp_path)


def test_normalise_root(tmp_path: Path) -> None:
    if sys.platform == "win32":
        assert normalise_root(r"C:\Lake") == normalise_root("c:/lake/") == normalise_root("C:\\Lake\\")
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks need extra rights on this host")
    assert normalise_root(str(link)) == normalise_root(str(target))
    assert normalise_root(str(tmp_path / "a")) != normalise_root(str(tmp_path / "b"))
    assert normalise_root("hf://buckets/ns/lake/") == "hf://buckets/ns/lake"
    assert "\\" not in normalise_root(str(target))
    assert not normalise_root(str(target)).endswith("/")
