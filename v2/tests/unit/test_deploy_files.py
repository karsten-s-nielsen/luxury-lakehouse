"""The Space and Job images (plan Task 29): both Dockerfiles pin the base image by digest, install the uv-locked set
with `--no-deps`, run as a non-root user, and hold no secret — no secret value and no `ENV` line that names a secret.
The Space README front matter turns HF OAuth on. The images are not built or pushed here (Gate G-B, Task 33)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from lakehouse.mask import SECRET_ENV_VARS

_DEPLOY = Path(__file__).parents[2] / "deploy"
_SPACE_DOCKERFILE = _DEPLOY / "space" / "Dockerfile"
_JOB_DOCKERFILE = _DEPLOY / "job" / "Dockerfile"
_SPACE_README = _DEPLOY / "space" / "README.md"
_JOB_README = _DEPLOY / "job" / "README.md"
_DIGEST = re.compile(r"^FROM\s+\S+@sha256:[0-9a-f]{64}\b", re.MULTILINE)
_USER = re.compile(r"^USER\s+(?!root\b|0\b)\S+", re.MULTILINE)
_ENV = re.compile(r"^ENV\s+([A-Z0-9_]+)", re.MULTILINE)


@pytest.mark.parametrize("path", [_SPACE_DOCKERFILE, _JOB_DOCKERFILE], ids=["space", "job"])
def test_dockerfile_pins_digest_no_deps_non_root(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert _DIGEST.search(text), "the base image must be pinned by @sha256 digest"
    assert "python:3.12-slim" in text, "the base image is python:3.12-slim"
    installs = [line for line in text.splitlines() if "uv pip install" in line]
    assert installs, "the image installs with uv pip install"
    assert all("--no-deps" in line for line in installs), "every uv pip install must use --no-deps"
    assert _USER.search(text), "the image must run as a non-root USER"


@pytest.mark.parametrize("path", [_SPACE_DOCKERFILE, _JOB_DOCKERFILE], ids=["space", "job"])
def test_dockerfile_holds_no_secret(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    env_names = set(_ENV.findall(text))
    leaked = env_names & set(SECRET_ENV_VARS)
    assert not leaked, f"a Dockerfile must not set a secret via ENV: {sorted(leaked)}"
    # no literal token value (an HF user-token shape: hf_ then 20+ alphanumerics)
    assert not re.search(r"\bhf_[A-Za-z0-9]{20,}\b", text), "no HF token value in the image"


def test_space_runs_web_and_job_runs_masked_run() -> None:
    space = _SPACE_DOCKERFILE.read_text(encoding="utf-8")
    job = _JOB_DOCKERFILE.read_text(encoding="utf-8")
    assert "lakehouse.web" in space, "the Space runs the web app"
    # the exec-form CMD, normalised from the JSON array to a plain command line
    job_cmd = job.replace('"', "").replace(", ", " ")
    assert "lakehouse.mask" in job_cmd, "the Job masks its output"
    assert "lakehouse run --trigger schedule" in job_cmd, "the Job runs the scheduled run"


def test_space_readme_front_matter() -> None:
    text = _SPACE_README.read_text(encoding="utf-8")
    assert text.startswith("---\n"), "the README starts with YAML front matter"
    front = text.split("---\n", 2)[1]
    assert "sdk: docker" in front
    assert "hf_oauth: true" in front
    assert re.search(r"^app_port:\s*\d+", front, re.MULTILINE), "the front matter sets app_port"


def test_job_readme_front_matter() -> None:
    """The Job image is uploaded as the `karstenskyt/jobs` Docker Space with `delete_patterns=["**"]`, so a stage
    without this README prunes the Space's front matter and HF reports CONFIG_ERROR (load test, 2026-10-31). The
    README therefore lives in the repo and rides every Job-image deploy."""
    text = _JOB_README.read_text(encoding="utf-8")
    assert text.startswith("---\n"), "the README starts with YAML front matter"
    front = text.split("---\n", 2)[1]
    assert "sdk: docker" in front
    assert "hf_oauth" not in front, "the Job Space serves no sign-in"
