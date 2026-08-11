#!/usr/bin/env python3
"""GH Actions helper: post a PR comment summarizing dbt failures (PR 4a).

Reads dbt's target/run_results.json (uploaded to UC Volume by the shim),
parses failing models + tests, posts a summary comment to the PR.

Exit code: 0 on success (regardless of whether failures were found —
this helper posts OR skips; the trigger helper owns the merge-block signal).
1 only on an unexpected error that prevents any attempt to post.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

import requests

from ingestion.databricks_auth import workspace_client

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_MAX_ERROR_LINES = 15


@dataclasses.dataclass(frozen=True)
class Failure:
    """Structured view of a single failing dbt node.

    `failures_count` is populated from the `failures` field of a dbt test result
    (number of rows returned by a data-test query). It is None for model errors.
    """

    unique_id: str
    status: str
    error_excerpt: str
    failures_count: int | None


def _workspace_client() -> WorkspaceClient:
    """Construct a Databricks WorkspaceClient from ambient environment.

    Resolution order is the SDK default: DATABRICKS_HOST + DATABRICKS_TOKEN env
    vars, then ~/.databrickscfg, then OIDC. Imported lazily so unit tests can
    patch this function without requiring the databricks-sdk to be installed.
    """

    return workspace_client()


def fetch_run_results(volume_path: str) -> dict[str, Any]:
    """Download run_results.json from UC Volume and parse as JSON."""
    ws = _workspace_client()
    logger.info("Fetching run_results from %s", volume_path)
    resp = ws.files.download(volume_path)
    # The SDK returns a streaming body for small files; both bytes and a
    # file-like object with .read() are valid depending on SDK version.
    raw = resp.contents
    if raw is None:
        raise RuntimeError(f"Empty download body from UC Volume path: {volume_path}")
    contents: bytes | str = raw.read() if hasattr(raw, "read") else raw  # type: ignore[assignment]
    return json.loads(contents)


def parse_failures(run_results: dict[str, Any]) -> list[Failure]:
    """Extract failing models and tests from a run_results.json dict.

    Skips `success` results. Keeps `error` (model compile/runtime error) and
    `fail` (data test failure). Long messages are truncated to the first
    `_MAX_ERROR_LINES - 1` lines with a trailing truncation marker line, so
    the rendered excerpt never exceeds `_MAX_ERROR_LINES` lines total.
    """
    failures: list[Failure] = []
    for r in run_results.get("results", []):
        status = r.get("status", "")
        if status not in ("error", "fail"):
            continue
        message = r.get("message", "")
        lines = message.splitlines()
        if len(lines) > _MAX_ERROR_LINES:
            kept = _MAX_ERROR_LINES - 1
            dropped = len(lines) - kept
            excerpt = "\n".join(lines[:kept]) + f"\n... (truncated — {dropped} more lines)"
        else:
            excerpt = message
        failures.append(
            Failure(
                unique_id=r.get("unique_id", ""),
                status=status,
                error_excerpt=excerpt,
                failures_count=r.get("failures"),
            )
        )
    return failures


def _display_name(unique_id: str) -> str:
    """Extract a human-readable name from a dbt `unique_id`.

    dbt unique_ids have two canonical shapes:
      - `model.<project>.<name>` — e.g. `model.luxury_lakehouse.fct_foo`
      - `test.<project>.<name>.<hash>` — e.g. `test.luxury_lakehouse.not_null_bar.123`

    For `test.` IDs the trailing hash segment is stripped so PR comments show
    the test name, not an opaque hash.
    """
    if not unique_id:
        return unique_id
    parts = unique_id.split(".")
    if len(parts) >= 4 and parts[0] == "test":
        return parts[-2]
    return parts[-1]


def format_comment(*, failures: list[Failure], run_page_url: str) -> str:
    """Build the PR comment body in GitHub-flavoured Markdown."""
    lines = ["### ❌ dbt-live-ci failed", ""]
    lines.append("**Failing models/tests:**")
    for f in failures:
        name = _display_name(f.unique_id)
        if f.failures_count is not None:
            lines.append(f"- `{name}` — {f.failures_count} failing rows")
        else:
            lines.append(f"- `{name}` — {f.status}")
    lines.append("")
    lines.append("**Error excerpt (first failure):**")
    lines.append("")
    lines.append("```")
    if failures:
        lines.append(failures[0].error_excerpt)
    lines.append("```")
    lines.append("")
    lines.append(f"[Databricks run log →]({run_page_url})")
    return "\n".join(lines)


def post_comment_to_pr(
    *,
    repo: str,
    pr_number: int,
    comment_body: str,
    github_token: str,
) -> None:
    """POST a comment via GH API.

    Fork-scope 403 (token has read-only access on PRs from forks) is logged
    and swallowed. Any other HTTPError is logged at WARNING but NOT re-raised —
    the merge-block signal is owned by the trigger helper's exit code, and we
    do not want a downstream commenting failure to mask the real dbt failure.
    """
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github+json",
            },
            json={"body": comment_body},
            timeout=(10, 30),
            verify=True,
        )
        resp.raise_for_status()
        logger.info("Posted PR comment (status=%d)", resp.status_code)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        body = exc.response.text if exc.response is not None else ""
        if status == 403 and "not accessible" in body.lower():
            logger.warning("PR comment 403 — fork-PR scope limitation. Skipping comment.")
            return
        logger.warning("PR comment POST failed: status=%s body=%s", status, body[:200])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Post dbt-failure PR comment.")
    parser.add_argument("--repo", required=True, help='GH repo, e.g. "owner/repo"')
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--run-page-url", required=True)
    parser.add_argument("--run-output-volume-path", required=True)
    parser.add_argument("--github-token", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    try:
        results = fetch_run_results(args.run_output_volume_path)
    except Exception as exc:  # noqa: BLE001 — best-effort fallback: fetch may fail for many reasons (SDK error, missing file, JSON decode). We post a generic comment and exit 0; the trigger helper owns the merge-block signal.
        logger.warning("Could not fetch run_results.json: %s. Posting generic failure comment.", exc)
        generic = (
            "### ❌ dbt-live-ci failed\n\n"
            "dbt failed before producing `run_results.json`. "
            f"[Databricks run log →]({args.run_page_url})"
        )
        post_comment_to_pr(
            repo=args.repo,
            pr_number=args.pr_number,
            comment_body=generic,
            github_token=args.github_token,
        )
        return 0

    failures = parse_failures(results)
    if not failures:
        logger.info("No failures in run_results — skipping comment.")
        return 0

    body = format_comment(failures=failures, run_page_url=args.run_page_url)
    post_comment_to_pr(
        repo=args.repo,
        pr_number=args.pr_number,
        comment_body=body,
        github_token=args.github_token,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
