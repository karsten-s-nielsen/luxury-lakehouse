"""Sync bronze ``sources.yml`` column inventories against the checked-in DESCRIBE snapshots.

    uv run python scripts/sync_bronze_sources_yml.py            # apply
    uv run python scripts/sync_bronze_sources_yml.py --check    # report drift, exit 1, write nothing

``--check`` is what CI runs (``src/tests/test_bronze_sources_parity.py``); a human runs the
fixer. Both call the same pure core in ``scripts/_bronze_sources_sync.py``, so the checker
cannot drift from the fixer — the property ``scripts/sync_tf_env_pins.py`` establishes for the
Terraform env pins.

The generator owns column INVENTORY only. Existing descriptions are never rewritten, and every
edit is insert-only: verify with ``git diff --stat``, which must show **0 deletions**.

Refreshing a snapshot (after ingestion adds a column) is a separate, deliberate act — capture
``DESCRIBE TABLE`` live and commit the fixture. This tool never talks to Databricks.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

# Running this file directly puts scripts/ on sys.path, not the repo root, so `import scripts`
# fails. Under pytest, pythonpath=["."] already covers it. Same two lines as
# scripts/sync_tf_env_pins.py:21-23, which hit this first.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from scripts._bronze_sources_sync import apply_missing_columns, plan_missing_columns
from scripts._bronze_table_inventory import contract_tables, sources_yml_path

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_FIXTURES = _REPO_ROOT / "src" / "tests" / "fixtures"

# Providers whose bronze schema is captured as a live DESCRIBE snapshot. Metrica is deliberately
# absent: it is CSV/EPTS files rather than Delta tables, so its coverage test reads a header
# enumeration instead (`test_metrica_bronze_coverage.py`). Recorded here rather than filtered
# silently — an unexplained omission is how the gaps this tool exists to close accumulate.
SNAPSHOTTED_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "skillcorner",
    "gradientsports",
)


def snapshot_path(provider: str) -> pathlib.Path:
    return _FIXTURES / f"{provider}_bronze_schema_snapshot.json"


def load_snapshot(provider: str) -> dict:
    return json.loads(snapshot_path(provider).read_text(encoding="utf-8"))


def drift(provider: str) -> dict[str, list[tuple[str, str]]]:
    """{table: [(name, type), …]} the snapshot has and sources.yml lacks."""
    snapshot = load_snapshot(provider)
    text = sources_yml_path(provider).read_text(encoding="utf-8")
    contract = contract_tables(provider)
    out: dict[str, list[tuple[str, str]]] = {}
    for table in snapshot["tables"]:
        if table not in contract:
            msg = (
                f"{provider}: snapshot table {table!r} is not documented in sources.yml. "
                "Classify it in scripts/_bronze_table_inventory.py (contract or non-contract) "
                "before syncing."
            )
            raise KeyError(msg)
        missing = plan_missing_columns(snapshot, text, table)
        if missing:
            out[table] = missing
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="report drift and exit 1; write nothing")
    parser.add_argument("--provider", choices=SNAPSHOTTED_PROVIDERS, help="limit to one provider")
    args = parser.parse_args()

    providers = (args.provider,) if args.provider else SNAPSHOTTED_PROVIDERS
    total = 0
    for provider in providers:
        pending = drift(provider)
        total += sum(len(v) for v in pending.values())
        if not pending:
            print(f"{provider}: up to date")
            continue
        for table, cols in pending.items():
            print(f"{provider}.{table}: {len(cols)} undocumented column(s)")
            if args.check:
                print(f"    {[c[0] for c in cols]}")
        if not args.check:
            path = sources_yml_path(provider)
            path.write_text(
                apply_missing_columns(path.read_text(encoding="utf-8"), load_snapshot(provider)), encoding="utf-8"
            )
            print(f"{provider}: wrote {path}")

    if args.check and total:
        print(f"\nDRIFT: {total} undocumented column(s). Run without --check to fix.", file=sys.stderr)
        return 1
    if not args.check and total:
        print(f"\nSynced {total} column(s). Verify `git diff` shows 0 deletions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
