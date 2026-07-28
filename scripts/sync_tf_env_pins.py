"""Sync main.tf serverless-env ``==`` pins to uv.lock (ADR-046). Human-invoked; never a
CI autofix — the parity sentinel remains the gate.

    python scripts/sync_tf_env_pins.py           # apply (rewrite main.tf in place)
    python scripts/sync_tf_env_pins.py --check    # exit 1 if any pin is out of sync (no write)

Surgical: rewrites ONLY the version substring inside each env block's
``dependencies = [...]`` span, and only the code portion of each line (trailing/full-line
comments are split off and preserved), keeping extras, comments, concat() wrappers,
ordering, and formatting intact.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Enable `python scripts/sync_tf_env_pins.py`: when run directly the repo root is not on
# sys.path (only scripts/ is), so `import scripts` fails. Under pytest, pythonpath=["."]
# already provides the repo root, making this insert a harmless duplicate.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._tf_env_pins import (
    CI_DBT_SUBMIT_SCRIPT,
    CI_DBT_UVX_WORKFLOWS,
    Drift,
    iter_dep_block_spans,
    normalize,
    parse_lock_versions,
    parse_sdk_extra_pin,
    resolve_desired_version,
    rewrite_ci_dbt_text,
)

_REPO = Path(__file__).resolve().parents[1]
_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"
_LOCK = _REPO / "uv.lock"
_PYPROJECT = _REPO / "pyproject.toml"

_PIN_RE = re.compile(r'"([A-Za-z0-9._-]+)(\[[^\]]*\])?==([\w.\-]+)"')


def _strip_trailing_comment(line: str) -> tuple[str, str]:
    """Split ``line`` into ``(code, comment)`` at the first ``#`` or ``//`` — protecting BOTH
    full-line AND trailing inline comments (M4). The TF env pins here are simple ``pkg==ver``
    strings; a URL-style dep (``@ https://…``) would split at its ``//`` yet reassemble
    losslessly (``line[:cut] + line[cut:] == line``), merely skipping its rewrite."""
    marks = [i for i in (line.find("#"), line.find("//")) if i != -1]
    if not marks:
        return line, ""
    cut = min(marks)
    return line[:cut], line[cut:]


def _rewrite_block(block: str, lock: dict[str, set[str]], sdk_pin: str, changes: list[Drift]) -> str:
    def _sub(m: re.Match[str]) -> str:  # defined once per block, not per line
        raw, extras, old = m.group(1), m.group(2) or "", m.group(3)
        desired = resolve_desired_version(normalize(raw), lock=lock, sdk_extra_pin=sdk_pin)
        if desired is None or desired == old:
            return m.group(0)
        changes.append(Drift("", normalize(raw), old, desired))
        return f'"{raw}{extras}=={desired}"'

    out: list[str] = []
    for line in block.splitlines(keepends=True):
        code, comment = _strip_trailing_comment(line)
        out.append(_PIN_RE.sub(_sub, code) + comment)  # only the code portion is eligible
    return "".join(out)


def rewrite_tf_text(tf_text: str, *, lock: dict[str, set[str]], sdk_extra_pin: str) -> tuple[str, list[Drift]]:
    """Return (new_text, changes) — version substrings synced, confined to dep-list spans."""
    spans = [span for _env, span in iter_dep_block_spans(tf_text) if span is not None]
    changes: list[Drift] = []
    out = tf_text
    for start, end in sorted(spans, reverse=True):  # right-to-left keeps earlier offsets valid
        out = out[:start] + _rewrite_block(out[start:end], lock, sdk_extra_pin, changes) + out[end:]
    return out, changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync main.tf env pins to uv.lock (ADR-046)")
    parser.add_argument("--check", action="store_true", help="exit 1 if out of sync; do not write")
    args = parser.parse_args()

    tf_text = _TF.read_text(encoding="utf-8")
    lock = parse_lock_versions(_LOCK.read_text(encoding="utf-8"))
    sdk_pin = parse_sdk_extra_pin(_PYPROJECT.read_text(encoding="utf-8"))

    new_text, changes = rewrite_tf_text(tf_text, lock=lock, sdk_extra_pin=sdk_pin)

    # CI dbt pins ride the same lockstep (ADR-046, 2026-07-27): the dbt-live-ci job's
    # declared serverless deps and the `uvx --from` runner invocations.
    repo = _TF.parents[3]
    ci_edits: list[tuple[Path, str]] = []
    for rel in (*CI_DBT_UVX_WORKFLOWS, CI_DBT_SUBMIT_SCRIPT):
        path = repo / rel
        text = path.read_text(encoding="utf-8")
        rewritten, drifts = rewrite_ci_dbt_text(rel, text, lock)
        if drifts:
            changes.extend(drifts)
            ci_edits.append((path, rewritten))

    if not changes:
        print("All TF env pins and CI dbt pins already in sync with uv.lock.")
        return 0
    for c in changes:
        print(f"  [{c.env_key}] {c.pkg}: {c.current} -> {c.desired}")
    if args.check:
        print(f"{len(changes)} pin(s) out of sync (--check).", file=sys.stderr)
        return 1
    if new_text != tf_text:
        _TF.write_text(new_text, encoding="utf-8")
    for path, rewritten in ci_edits:
        path.write_text(rewritten, encoding="utf-8")
    print(f"{len(changes)} pin(s) updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
