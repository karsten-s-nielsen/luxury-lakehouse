"""Verify a Hugging Face token has the exact scope a lakehouse credential needs.

Hugging Face has no API to create an access token, and service accounts and resource groups need a paid org tier, so a
token is made by hand in the web UI. The UI wording changes and is easy to get wrong (a grant on your user account
instead of the bucket, a bucket added with an empty permission, Read where Write is needed). This script does not read
the UI: it exercises the token against the live buckets for the role it must fill, and names the exact defect when the
token is wrong. Run it after you create or edit a token; fix what it reports; run it again. The token value is read
from an environment variable and is never printed.

Roles:
  space  the Ops Space / hf_space test token: Write on the lake bucket only; denied the restricted bucket and the
         control repository.
  job    the Job / hf_job test token: Write on the lake and restricted buckets and the control repository.
  jobs   the Ops Space job-control token (`LAKEHOUSE_HF_JOBS_TOKEN`): can read/manage the scheduled job. A scheduled
         job under a PERSONAL namespace needs a classic write-role token (HF fine-grained Jobs is org-only). Pass
         `--job-id`; the lake arguments are not used. This is the credential the Settings schedule reads.

Usage (from the v2 directory, in the venv):
  HF_TOKEN=<value> uv run python scripts/check_hf_token.py space \\
      --lake luxury-lakehouse/lake-test --restricted luxury-lakehouse/restricted-test \\
      --control luxury-lakehouse/lake-test-control
  # read the value from another variable (for example the one in lakehouse.env):
  uv run python scripts/check_hf_token.py space --token-env HF_LAKE_TEST --lake luxury-lakehouse/lake-test \\
      --restricted luxury-lakehouse/restricted-test --control luxury-lakehouse/lake-test-control
  # the job-control token against the scheduled job:
  uv run python scripts/check_hf_token.py jobs --token-env LAKEHOUSE_HF_JOBS_TOKEN --job-id 6ae602100e0338d1bb7d2ed8

Exit code 0 means the token matches the role; 1 means it does not; 2 means the script could not run (no token).
"""

from __future__ import annotations

import argparse
import itertools
import sys
import uuid
from os import environ
from typing import Any, Final

from huggingface_hub import HfApi
from pydantic import SecretStr

from lakehouse.storage.hf import HfStorage
from lakehouse.storage.roots import default_bootstrap_file, resolve_roots


def _token_meta(api: Any) -> tuple[dict[str, Any], list[str]]:
    """Return the token's `accessToken` metadata and human summary lines (no secret value)."""
    try:
        who: dict[str, Any] = api.whoami()
    except Exception as exc:  # noqa: BLE001 - a diagnostic: report any whoami failure as a line
        return {}, [f"whoami failed: {type(exc).__name__}: {exc}"]
    token: dict[str, Any] = who.get("auth", {}).get("accessToken", {})
    lines = [f"token name={token.get('displayName')!r} role={token.get('role')!r}"]
    fine = token.get("fineGrained")
    if not fine:
        # a classic role token: correct for the jobs role (personal jobs need it), wrong for space/job (the
        # per-role verdict below says so) — so the note states the fact, not a one-size prescription
        lines.append("  classic role token: inherits full org access (required for the jobs role; not for space/job)")
        return token, lines
    scoped = fine.get("scoped", [])
    if not scoped:
        lines.append("  fine-grained, but no scoped grants")
    for item in scoped:
        entity = item.get("entity", {})
        perms = item.get("permissions", [])
        name = entity.get("name") or entity.get("_id")
        lines.append(f"  grant on {entity.get('type')} {name!r}: {perms or 'EMPTY (grants nothing)'}")
    return token, lines


_DENIAL: Final = frozenset({401, 403, 404})  # an authorization / not-found denial; a 5xx/timeout is inconclusive


def _status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    return int(code) if isinstance(code, int) else None


def _probe(fn: Any) -> tuple[str, str]:
    """Run `fn` and classify the outcome: 'readable' (it succeeded), 'denied' (HTTP 401/403/404), or 'error'
    (anything else — a 5xx, a timeout or a network failure — which is INCONCLUSIVE, never a pass and never a denial)."""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - a diagnostic: classify every outcome by its HTTP status
        code = _status(exc)
        if code in _DENIAL:
            return "denied", f"HTTP {code}"
        return "error", f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
    return "readable", "ok"


def _read_bucket(storage: HfStorage, root: str) -> tuple[str, str]:
    return _probe(lambda: list(itertools.islice(storage.list(root, ""), 0, 1)))  # type: ignore[arg-type]


def _write_bucket(storage: HfStorage, root: str) -> tuple[str, str]:
    key = f"config/_live-check-{uuid.uuid4().hex}/probe.json"

    def _do() -> None:
        storage.put(root, key, b"{}")  # type: ignore[arg-type]
        storage.delete(root, key, missing_ok=True)  # type: ignore[arg-type]

    return _probe(_do)


def _read_control(api: Any, repo: str) -> tuple[str, str]:
    return _probe(lambda: api.repo_info(repo, repo_type="dataset"))


def _read_scheduled_job(api: Any, job_id: str) -> tuple[str, str]:
    """The decisive job-control probe: can the token read the scheduled job? A 403 means it cannot manage the job
    (the status read and every control — reschedule, suspend, resume, trigger — would fail the same way)."""
    return _probe(lambda: api.inspect_scheduled_job(scheduled_job_id=job_id))


def _evaluate(role: str, results: dict[str, tuple[str, str]], *, fine_grained: bool) -> list[str]:
    """Judge the probe statuses against the role (only 'readable'/'denied' are judged; the caller treats an 'error'
    status as inconclusive and exits 2). Returns the failing lines; empty means the token matches the role."""
    status = {name: s for name, (s, _) in results.items()}
    fails: list[str] = []
    if role == "space":
        if not fine_grained:
            fails.append("not a fine-grained token; a role token inherits full org access. Use a fine-grained token.")
        if status["lake_read"] != "readable":
            fails.append(
                "cannot read the lake bucket. Grant Read/Write on the BUCKET resource itself (not your user "
                "account, not an empty permission)."
            )
        elif status.get("lake_write") not in ("readable", "skipped"):
            fails.append("cannot write the lake bucket; the lake grant is Read — set it to Write.")
        if status["restricted_read"] == "readable":
            fails.append("reads the restricted bucket; it must be denied. Remove restricted from the token scope.")
        if status["control_read"] == "readable":
            fails.append("reads the control repository; it must be denied. Remove it from the token scope.")
    elif role == "jobs":  # the job-control token: it must read/manage the scheduled job
        if status["job_read"] != "readable":
            fails.append(
                "cannot read the scheduled job. A personal-namespace job needs a classic write-role token "
                "(HF fine-grained Jobs is organisation-only); a lake-only fine-grained token gets 403 here."
            )
    else:  # job: Write on all three
        if status["lake_write"] != "readable":
            fails.append("cannot write the lake bucket; a Job token needs Write on it.")
        if status["restricted_write"] != "readable":
            fails.append("cannot write the restricted bucket; a Job token needs Write on it.")
        if status["control_read"] != "readable":
            fails.append(
                "cannot read the control repository; a Job token needs it (its Write is exercised by the real "
                "lease acquisition, not write-probed here to avoid racing the ledger)."
            )
    return fails


def _verdict(role: str, results: dict[str, tuple[str, str]], *, fine_grained: bool) -> tuple[int, list[str]]:
    """The exit code + report lines for the probe results: 2 if any probe was inconclusive (a non-denial error, so the
    run proves nothing), 1 if the token fails the role, 0 on a match. Pure, so the inconclusive -> exit 2 branch is
    unit-testable (B HFB-IMPL-07)."""
    inconclusive = [name for name, (state, _) in results.items() if state == "error"]
    if inconclusive:
        return 2, [
            f"a probe hit a non-denial error (5xx / timeout / network): {inconclusive}",
            "a denial must be HTTP 401/403/404; re-run",
        ]
    fails = _evaluate(role, results, fine_grained=fine_grained)
    return (1, fails) if fails else (0, [])


def _report(out: Any, code: int, lines: list[str], *, read_only: bool) -> int:
    """Print the verdict and return the exit code (shared by every role)."""
    if code == 2:
        out("RESULT: INCONCLUSIVE")
        for line in lines:
            out(f"  {line}")
        return 2
    if code == 1:
        out("RESULT: FAIL")
        for line in lines:
            out(f"  - {line}")
        return 1
    out("RESULT: PASS - the token matches the role")
    if read_only:
        out("  note: --read-only, so Write on the lake bucket was not checked")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify an HF token's scope for a lakehouse role.")
    parser.add_argument("role", choices=("space", "job", "jobs"))
    parser.add_argument("--token-env", default="HF_TOKEN", help="the environment variable that holds the token")
    parser.add_argument("--lake", help="the lake bucket, <namespace>/<name> (space/job roles)")
    parser.add_argument("--restricted", help="the restricted bucket, <namespace>/<name> (space/job roles)")
    parser.add_argument("--control", help="the control dataset repo, <namespace>/<name>-control (space/job roles)")
    parser.add_argument("--job-id", help="the scheduled job id (jobs role)")
    parser.add_argument(
        "--read-only",
        action="store_true",
        dest="read_only",
        help="skip the lake write probe, so a production token can be checked without writing to the lake",
    )
    args = parser.parse_args()

    environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    token = environ.get(args.token_env)
    if not token:
        print(f"FAIL: the environment variable {args.token_env} is not set")
        return 2

    api: Any = HfApi(token=token)
    out = print
    out(f"Checking {args.token_env} for role '{args.role}'")
    token_meta, summary = _token_meta(api)
    for line in summary:
        out(line)
    fine_grained = token_meta.get("role") == "fineGrained"

    # The jobs role probes the scheduled job, not the buckets, so it needs only --job-id.
    if args.role == "jobs":
        if not args.job_id:
            print("FAIL: the jobs role needs --job-id (the scheduled job id)")
            return 2
        results: dict[str, tuple[str, str]] = {"job_read": _read_scheduled_job(api, args.job_id)}
        for name, (state, detail) in results.items():
            out(f"  {name}: {state} ({detail})")
        code, lines = _verdict(args.role, results, fine_grained=fine_grained)
        return _report(out, code, lines, read_only=False)

    missing = [f"--{n}" for n in ("lake", "restricted", "control") if not getattr(args, n)]
    if missing:
        print(f"FAIL: the {args.role} role needs {', '.join(missing)}")
        return 2
    roots_env = {
        "LAKEHOUSE_LAKE_ROOT": f"hf://buckets/{args.lake}",
        "LAKEHOUSE_RESTRICTED_ROOT": f"hf://buckets/{args.restricted}",
        "HF_TOKEN": token,
    }
    resolved = resolve_roots(
        cli_lake=None, cli_restricted=None, env=roots_env, bootstrap_file=default_bootstrap_file(environ)
    )
    storage = HfStorage(resolved.roots, hf_token=SecretStr(token), s3_key=None)

    expected_control = f"{args.lake}-control"  # the control repo is always <lake bucket>-control (T6 D1)
    if args.control != expected_control:
        out(f"RESULT: FAIL - --control {args.control!r} is not the lake's control repo; expected {expected_control!r}")
        return 1

    if args.role == "space":
        results = {
            "lake_read": _read_bucket(storage, "lake"),
            "lake_write": ("skipped", "--read-only") if args.read_only else _write_bucket(storage, "lake"),
            "restricted_read": _read_bucket(storage, "restricted"),
            "control_read": _read_control(api, args.control),
        }
    else:  # job: write-probe the two buckets (safe put+delete); READ the control repo only — a write probe there
        # would commit to its `main` and could race a live lease acquisition (HFB-IMPL-13).
        results = {
            "lake_write": _write_bucket(storage, "lake"),
            "restricted_write": _write_bucket(storage, "restricted"),
            "control_read": _read_control(api, args.control),
        }
    for name, (state, detail) in results.items():
        out(f"  {name}: {state} ({detail})")

    code, lines = _verdict(args.role, results, fine_grained=fine_grained)
    return _report(out, code, lines, read_only=args.read_only)


if __name__ == "__main__":
    sys.exit(main())
