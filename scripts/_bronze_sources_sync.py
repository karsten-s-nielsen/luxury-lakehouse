"""Pure core for the bronze ``sources.yml`` column-inventory sync.

No I/O, no SDK — text in, text out. ``sync_bronze_sources_yml.py`` and the CI parity gate both
import this, so the fixer and the checker are the same code. Same shape as
``scripts/_tf_env_pins.py``, and for the same reason: a sync tool whose checker is a second
implementation drifts from its fixer, and the drift is invisible until it matters.

WHAT IT OWNS, AND WHAT IT MUST NOT TOUCH
----------------------------------------
It owns the column **inventory**: which columns appear under which table. It appends the ones a
live ``DESCRIBE`` snapshot has and ``sources.yml`` lacks, using the repo's established
auto-documented wording.

It never rewrites an existing line. Hand-authored descriptions accumulated across five providers
are the reason: a generator that regenerated prose would erase them, and no test would notice
because tests read the parsed data model, not the text.

WHY TEXT SPLICING, NOT YAML ROUND-TRIP
---------------------------------------
``yaml.safe_load`` + ``safe_dump`` preserves the data model and discards everything else —
comments, quote style, folded ``>`` blocks, blank lines. Applied to
``_gradientsports__sources.yml`` on 2026-08-09 it produced a **+1004/-79** diff where the 79
deletions were pre-existing entries silently re-serialised. Splicing text keeps the diff purely
additive, which is also what makes review possible at 300+ columns.

INDENTATION (dbt sources)
-------------------------
``    tables:`` (4) -> ``      - name: <table>`` (6) -> ``        columns:`` (8)
-> ``          - name: <col>`` (10) -> ``            description: >`` (12) -> body (14).

Table boundaries are found at **exactly** indent 6. An earlier draft auto-detected the indent
and matched the ``  - name: <source>`` entry at indent 2, so every insertion landed at EOF —
under the wrong table, while reporting success. The constants below are deliberate.
"""

from __future__ import annotations

import yaml

TABLE_INDENT = 6
COL_INDENT = 10

_AUTO_DESCRIPTION = (
    "{type_} — auto-documented from DESCRIBE TABLE; refine description when a staging model first reads this col."
)


def needs_yaml_quoting(name: str) -> bool:
    """True when a bare column name would not round-trip as the string it is.

    ``50_50`` parses as the integer 5050; ``yes``/``no``/``on``/``off`` parse as booleans. The
    existing statsbomb entry is written ``- name: "50_50"`` for exactly this reason, and says so
    in its own comment. Emitting such a name unquoted lands a key of the wrong TYPE, and the
    coverage gate then reports the column missing forever.
    """
    try:
        return yaml.safe_load(name) != name
    except yaml.YAMLError:
        return True


def render_column_block(name: str, type_: str, *, col_indent: int = COL_INDENT) -> str:
    """One column entry in the house folded-block style used by all six providers."""
    pad = " " * col_indent
    rendered = f'"{name}"' if needs_yaml_quoting(name) else name
    return f"{pad}- name: {rendered}\n{pad}{'  '}description: >\n{pad}{'    '}{_AUTO_DESCRIPTION.format(type_=type_)}\n"


def _table_bounds(lines: list[str], table: str) -> tuple[int, int]:
    """[start, end) line indices of ``table``'s block, located at exactly TABLE_INDENT."""
    marker = f"{' ' * TABLE_INDENT}- name: {table}"
    start = next((i for i, ln in enumerate(lines) if ln.rstrip("\n") == marker), None)
    if start is None:
        msg = f"table {table!r} not found in sources.yml at indent {TABLE_INDENT}"
        raise KeyError(msg)
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith(f"{' ' * TABLE_INDENT}- name: ")),
        len(lines),
    )
    return start, end


def _documented_columns(block: str, *, col_indent: int = COL_INDENT) -> set[str]:
    """Column names already present, with YAML quoting removed.

    ``- name: "50_50"`` must compare equal to the snapshot's ``50_50``. Reading the raw text
    without unquoting reported that column perpetually undocumented — a false positive found
    2026-08-10 by running --check against the whole repo.
    """
    prefix = f"{' ' * col_indent}- name: "
    out = set()
    for ln in block.splitlines():
        if ln.startswith(prefix):
            out.add(ln[len(prefix) :].strip().strip("\"'"))
    return out


def plan_missing_columns(snapshot: dict, sources_yml_text: str, table: str) -> list[tuple[str, str]]:
    """(name, type) pairs the snapshot has and ``sources.yml`` lacks, in snapshot order.

    Snapshot order — not sorted — so the appended block mirrors ``DESCRIBE TABLE``, which is how
    a reader diffs the two by eye.
    """
    lines = sources_yml_text.splitlines(keepends=True)
    start, end = _table_bounds(lines, table)
    documented = _documented_columns("".join(lines[start:end]))
    return [(c["name"], c["type"]) for c in snapshot["tables"][table] if c["name"] not in documented]


def apply_missing_columns(sources_yml_text: str, snapshot: dict) -> str:
    """Append every missing column to its own table's block. INSERT-ONLY.

    Raises ``KeyError`` if the snapshot names a table absent from ``sources.yml`` — that is a
    classification gap belonging to ``scripts/_bronze_table_inventory.py``, and silently
    skipping it would recreate the invisible-drift the inventory exists to end.
    """
    text = sources_yml_text
    for table in snapshot["tables"]:
        missing = plan_missing_columns(snapshot, text, table)
        if not missing:
            continue
        lines = text.splitlines(keepends=True)
        start, end = _table_bounds(lines, table)
        # Insert after the block's last content line, before any trailing blank lines, so the
        # new entries stay inside this table rather than drifting into the next one's header.
        insert_at = end
        while insert_at > start and not lines[insert_at - 1].strip():
            insert_at -= 1
        block = "".join(render_column_block(name, type_) for name, type_ in missing)
        lines.insert(insert_at, block)
        text = "".join(lines)
    return text
