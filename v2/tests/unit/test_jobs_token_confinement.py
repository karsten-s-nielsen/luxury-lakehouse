"""The job-control token is confined to the schedule seam (two-token design, §29.9).

A scheduled job under a personal namespace can only be managed by a classic `read`/`write`-role token, because HF's
fine-grained Jobs permission is organisation-only and HF OAuth has no jobs scope. Such a token also reads every repo
the owner can read, including the restricted bucket, so it must NEVER reach the lake data path. The Space therefore
holds it as a SEPARATE secret `LAKEHOUSE_HF_JOBS_TOKEN`, read in exactly one place — `HfJobsSchedule.from_env`. Every
storage and page read uses the lake-only `HF_TOKEN`. This test fails if any other module reads the jobs-token env, so
a future change cannot quietly wire the broad token into a bucket read."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Final

from lakehouse.schedule.hf_jobs import JOBS_TOKEN_ENV

SRC: Final = Path(__file__).resolve().parents[2] / "src" / "lakehouse"
_SEAM: Final = "schedule/hf_jobs.py"  # the one module allowed to read the jobs token


def _reads_jobs_token(source: str) -> bool:
    """Whether the module READS the jobs-token env — the constant `JOBS_TOKEN_ENV`, or the literal used as an env
    lookup key (`env.get("…")`, `os.environ["…"]`). A module that only NAMES the secret in a user-facing string (a
    help note telling the operator which secret to set) does not read it, so it is not a confinement breach."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "JOBS_TOKEN_ENV":
            return True  # the shared constant — the right way to read it, allowed only in the seam
        if isinstance(node, ast.Call):  # env.get("LAKEHOUSE_HF_JOBS_TOKEN") / os.getenv("…")
            if any(isinstance(a, ast.Constant) and a.value == JOBS_TOKEN_ENV for a in node.args):
                return True
        if isinstance(node, ast.Subscript):  # env["LAKEHOUSE_HF_JOBS_TOKEN"]
            key = node.slice
            if isinstance(key, ast.Constant) and key.value == JOBS_TOKEN_ENV:
                return True
    return False


def test_jobs_token_env_is_read_only_in_the_schedule_seam() -> None:
    assert JOBS_TOKEN_ENV == "LAKEHOUSE_HF_JOBS_TOKEN"  # noqa: S105 - the literal the scan looks for, not a secret
    offenders = [
        path.relative_to(SRC).as_posix()
        for path in sorted(SRC.rglob("*.py"))
        if path.relative_to(SRC).as_posix() != _SEAM and _reads_jobs_token(path.read_text(encoding="utf-8"))
    ]
    assert offenders == [], (
        f"{JOBS_TOKEN_ENV} read outside {_SEAM} — the token must not reach the lake path: {offenders}"
    )


def test_storage_path_reads_the_lake_token_not_the_jobs_token() -> None:
    """The HF storage builder reads `HF_TOKEN` (the lake-only token), never the jobs token. Guards the other half of
    the separation: the data path's credential is the confined one, so a page render can never use the broad token."""
    roots = (SRC / "storage" / "roots.py").read_text(encoding="utf-8")
    assert "env.get(HF_TOKEN_ENV)" in roots  # the HF lake reads HF_TOKEN
    assert JOBS_TOKEN_ENV not in roots  # and never the jobs token
