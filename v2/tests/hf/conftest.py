"""Shared fixtures and secret masking for the live HF tests (spec §20; plan Task 33).

These tests run only inside an owner-launched HF Job (Gate G-B). `job` and `space` are the two storage channels of
§10.5: `job` holds the S3 gateway key (data files, raw files and the catalog-control objects) and `space` reads
through `HfApi` with only the HF token, as the Ops Space does. An autouse fixture installs the in-process secret
masking (spec §20), beside the `python -m lakehouse.mask` wrapper that masks the whole process output at the OS level.

Every test object lives under a per-run prefix and is removed at session end. The suite runs against a throwaway test
bucket pair, never the production lake (owner decision, 2026-10-29): see `hfkit.require_test_lake`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from hfkit import JOB_PREFIXES, SPACE_PREFIXES, purge, require_test_lake
from pydantic import SecretStr

from lakehouse.logging import install_secret_masking, remove_secret_masking
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.roots import HF_TOKEN_ENV, default_bootstrap_file, open_storage, resolve_roots


@pytest.fixture(scope="session", autouse=True)
def _mask_secrets() -> Iterator[None]:
    """Mask every secret value in log records and in stdout/stderr for the whole session (spec §20)."""
    install_secret_masking(os.environ)
    try:
        yield
    finally:
        remove_secret_masking()


@pytest.fixture(scope="session")
def job() -> Iterator[HfStorage]:
    """The Job's storage: the gateway key plus the HF token. Requested only by the hf_job tests."""
    resolved = resolve_roots(
        cli_lake=None, cli_restricted=None, env=os.environ, bootstrap_file=default_bootstrap_file(os.environ)
    )
    storage = open_storage(resolved, os.environ)
    assert isinstance(storage, HfStorage), "the live HF tests need HF roots"
    assert "gateway=True" in repr(storage), "the hf_job tests need the gateway credentials of the Job"
    try:
        yield storage
    finally:
        purge(storage, JOB_PREFIXES)


@pytest.fixture(scope="session")
def space() -> Iterator[HfStorage]:
    """The Space's storage: the HF token only, every read through `HfApi` (no gateway), as on the Ops Space."""
    resolved = resolve_roots(
        cli_lake=None, cli_restricted=None, env=os.environ, bootstrap_file=default_bootstrap_file(os.environ)
    )
    storage = HfStorage(resolved.roots, hf_token=SecretStr(os.environ[HF_TOKEN_ENV]), s3_key=None)
    try:
        yield storage
    finally:
        purge(storage, SPACE_PREFIXES, roots=("lake",))


@pytest.fixture
def test_lake(job: HfStorage) -> HfStorage:
    """The `job` storage, only after proving it is a throwaway test lake (guards every committing/sweep test)."""
    require_test_lake(job)
    return job
