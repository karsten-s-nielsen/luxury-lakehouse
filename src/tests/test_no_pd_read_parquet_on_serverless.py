"""R-GATE (ADR-087 amendment follow-up 3): ban ``pd.read_parquet`` in serverless-reachable ``src/`` code.

Databricks serverless pins **pyarrow 8.0.0** (base runtime, NOT uv.lock's 19.0.1), and pandas'
``read_parquet`` REFUSES to read under pyarrow < 10.0.1 — so any ``pd.read_parquet`` on a serverless path
raises ``Unable to find a usable engine`` at runtime (R6, run 755550720578960: the preflight canary died
reading its own fixture). The fix is the engine-agnostic ``pyarrow.parquet.read_table(path).to_pandas()``
(reads on pyarrow 8 and 19). This gate makes "no pandas parquet engine on a serverless path"
NON-REGRESSABLE — a hand-audit already leaked (4-vs-5 sites + a missed module); a mechanical default-deny
scan does not. Mirrors ``test_topandas_boundedness.py`` / the ``resolve_hf_token`` AST gate.

Default-DENY: every ``pd.read_parquet(`` / ``pandas.read_parquet(`` CALL under ``src/`` (excluding
``src/tests/``) must be in ``_ALLOWLIST`` with a reason, or this test fails. A new serverless read must use
the engine-agnostic primitive; a genuinely non-serverless read (classified by compute) is added here with
its justification.
"""

from __future__ import annotations

import re
from pathlib import Path

# Call sites that are NOT serverless-reachable, each justified (R-AUDIT, 2026-10-10). A relative POSIX
# path -> the reason it is safe to keep ``pd.read_parquet``. Anything else is a hard failure.
_ALLOWLIST: dict[str, str] = {
    "src/analytics/action_context/tracking_frame_spill.py": (
        "read_built_frames() is TEST-ONLY (called only by src/tests/tracking_marts/test_build_spill.py); "
        "the prod Stage-2 spill read-back is Spark (spark.read.parquet, tracking_marts_dispatch.py:285), "
        "never this pandas reader."
    ),
}

# Match the CALL (trailing paren), not docstring/comment prose that merely names the function.
_PATTERN = re.compile(r"\b(?:pd|pandas)\.read_parquet\s*\(")

_SRC = Path("src")
_TESTS = Path("src") / "tests"


def _serverless_src_files() -> list[Path]:
    return [p for p in _SRC.rglob("*.py") if _TESTS not in p.parents]


def test_no_pd_read_parquet_outside_allowlist() -> None:
    """Every ``pd.read_parquet`` call in serverless-reachable ``src/`` is engine-agnostic or allowlisted."""
    violations: list[str] = []
    for path in _serverless_src_files():
        rel = path.as_posix()
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _PATTERN.search(line) and rel not in _ALLOWLIST:
                violations.append(f"{rel}:{lineno}: {line.strip()}")
    assert not violations, (
        "pd.read_parquet on a serverless-reachable path (fails on serverless pyarrow 8.0.0). Use "
        "pyarrow.parquet.read_table(path).to_pandas(), or add the file to _ALLOWLIST with a "
        "compute-classification reason:\n" + "\n".join(violations)
    )


def test_allowlist_entries_still_exist_and_still_read_parquet() -> None:
    """Non-vacuity: every allowlisted file exists AND still contains a ``pd.read_parquet`` call — so a
    stale allowlist entry (file moved, or the read already migrated) is caught instead of silently masking."""
    for rel in _ALLOWLIST:
        p = Path(rel)
        assert p.is_file(), f"allowlisted file missing: {rel} (remove the stale _ALLOWLIST entry)"
        assert _PATTERN.search(p.read_text(encoding="utf-8")), (
            f"allowlisted file {rel} no longer calls pd.read_parquet — remove the stale _ALLOWLIST entry"
        )
