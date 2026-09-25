"""Anti-bloat gate for the always-loaded AGENTS.md instruction surface.

Enforces the class-1 byte budget + per-invariant migration-safety oracle from
docs/superpowers/specs/2026-09-25-agents-md-restructure-design.md.
Runs on every CI leg (not slow-marked). Every read is utf-8; every byte
budget asserts stat().st_size, never len(text) (spec LH-SPEC-01).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]  # src/tests/ -> repo root
FIXTURES = Path(__file__).resolve().parent / "fixtures"

AGENTS_ROOT = REPO_ROOT / "AGENTS.md"
AGENTS_NESTED = REPO_ROOT / "hf_taipy_app" / "AGENTS.md"
SHIM_ROOT = REPO_ROOT / "CLAUDE.md"
SHIM_NESTED = REPO_ROOT / "hf_taipy_app" / "CLAUDE.md"
CONTEXT_DIR = REPO_ROOT / "docs" / "context"
INVENTORY = FIXTURES / "agents_md_invariant_inventory.json"

# --- budget constants (bytes) ---
TARGET_ROOT_BYTES = 28_672  # 28 KB design cut (spec 7)
NESTED_MAX_BYTES = 9_497  # no-growth brake (current nested size)
# CEILING starts == TARGET/NESTED_MAX (loosest valid brake). Task 7 tightens
# each to ceil(landed * 1.15) from the measured landed byte size.
CEILING_ROOT_BYTES = 21_378  # ceil(landed 18,589 * 1.15) — the re-bloat brake
CEILING_NESTED_BYTES = 3_879  # ceil(landed 3,373 * 1.15)
CONTEXT_FLOOR_BYTES = 54_966  # round(landed context total 61,073 * 0.9); guards against hollowing
# PINNED to the enumerated total (fixture count == this == len(ids)). A
# fixture-only edit (delete entry + decrement count) still fails here.
INVARIANT_COUNT = 125

_NORM_RE = re.compile(r"[^a-z0-9]+")


def _norm(s: str) -> str:
    """Collapse case + markdown/punctuation for anchor-presence matching.

    The class-1 anchor is a prose phrase; in AGENTS.md its words are wrapped in
    backticks/bold and joined with em-dashes (e.g. "**NOT `pyright src/` — that
    is NARROWER than CI**"). Exact-substring matching would spuriously fail on
    that formatting. Normalising both sides (lowercase, non-alphanumeric runs ->
    single space) keeps the ordered-word-sequence check tight while ignoring
    formatting. Symbol checks stay EXACT (they are verbatim-moved code tokens).
    """
    return _NORM_RE.sub(" ", s.lower()).strip()


BULLET_RE = re.compile(r"^\s*[-*] ")
POINTER_RE = re.compile(r"ADR-\d+|docs/context/|\S+\.(?:py|md|tf|sql|ya?ml|json|toml)\b")
ADR_ONLY_RE = re.compile(r"^ADR-\d+$")
ANCHOR_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")
SUBSTANTIVE_BULLET_CHARS = 120
BULLET_CHAR_CAP = 600
ANCHOR_MIN_CHARS = 12
STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "to",
    "and",
    "or",
    "see",
    "for",
    "in",
    "on",
    "is",
    "are",
    "be",
    "rule",
    "adr",
    "this",
    "that",
    "with",
    "via",
    "per",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _agents_text() -> str:
    parts = [_read(AGENTS_ROOT)]
    if AGENTS_NESTED.exists():
        parts.append(_read(AGENTS_NESTED))
    return "\n".join(parts)


def _surface_text() -> str:
    parts = [_agents_text()]
    for f in sorted(CONTEXT_DIR.glob("*.md")):
        parts.append(_read(f))
    return "\n".join(parts)


def _load_inventory() -> dict:
    return json.loads(_read(INVENTORY))


def test_root_byte_budget() -> None:
    size = AGENTS_ROOT.stat().st_size
    assert size <= TARGET_ROOT_BYTES, f"AGENTS.md {size} B > TARGET {TARGET_ROOT_BYTES} B"
    assert size <= CEILING_ROOT_BYTES, f"AGENTS.md {size} B > CEILING {CEILING_ROOT_BYTES} B"
    assert CEILING_ROOT_BYTES <= TARGET_ROOT_BYTES, "CEILING must not exceed TARGET"


def test_nested_no_growth() -> None:
    size = AGENTS_NESTED.stat().st_size
    assert size <= NESTED_MAX_BYTES, f"nested AGENTS.md {size} B grew past {NESTED_MAX_BYTES} B"
    assert size <= CEILING_NESTED_BYTES


def test_bullet_char_cap_and_pointer() -> None:
    for label, path in (("root", AGENTS_ROOT), ("nested", AGENTS_NESTED)):
        for i, line in enumerate(_read(path).splitlines(), 1):
            if not BULLET_RE.match(line):
                continue
            n = len(line)  # char count on decoded text (readability cap)
            if n <= SUBSTANTIVE_BULLET_CHARS:
                continue
            assert n <= BULLET_CHAR_CAP, f"{label}:{i} bullet {n} chars > {BULLET_CHAR_CAP}"
            assert POINTER_RE.search(line), f"{label}:{i} substantive bullet lacks ADR/docs/file pointer"


def test_shim_integrity() -> None:
    for shim in (SHIM_ROOT, SHIM_NESTED):
        lines = [ln for ln in _read(shim).splitlines() if ln.strip() and not ln.lstrip().startswith("<!--")]
        assert lines == ["@AGENTS.md"], f"{shim} is not a pure @AGENTS.md shim: {lines}"


def test_inventory_schema_valid() -> None:
    inv = _load_inventory()
    ids = [e["id"] for e in inv["invariants"]]
    assert len(ids) == len(set(ids)), "duplicate invariant ids"
    assert len(ids) == inv["count"], f"count {inv['count']} != {len(ids)} entries"
    assert inv["count"] == INVARIANT_COUNT, (
        f"fixture count {inv['count']} != pinned INVARIANT_COUNT {INVARIANT_COUNT} "
        "(silent inventory shrink? edit the test constant deliberately)"
    )
    for e in inv["invariants"]:
        syms = e["symbols"]
        assert syms, f"{e['id']}: empty symbols"
        assert not all(ADR_ONLY_RE.match(s) for s in syms), f"{e['id']}: ADR-only symbols"
        assert e["disposition"] in {"moved", "kept-class1", "dropped"}
        if e["disposition"] == "dropped":
            assert e.get("drop_reason"), f"{e['id']}: dropped without drop_reason"
        if e.get("class1_present"):
            anchor = e.get("class1_anchor", "") or ""
            assert len(anchor) >= ANCHOR_MIN_CHARS, f"{e['id']}: anchor too short"
            toks = [t for t in ANCHOR_TOKEN_RE.findall(anchor.lower()) if t not in STOPWORDS]
            assert len(toks) >= 2, f"{e['id']}: anchor not distinctive: {anchor!r}"


def test_per_invariant_completeness() -> None:
    inv = _load_inventory()
    surface = _surface_text()
    agents = _agents_text()
    for e in inv["invariants"]:
        if e["disposition"] == "dropped":
            continue
        for s in e["symbols"]:
            assert s in surface, f"{e['id']}: symbol {s!r} vanished from AGENTS.md + docs/context"
        if e.get("class1_present"):
            assert _norm(e["class1_anchor"]) in _norm(agents), (
                f"{e['id']}: class-1 anchor {e['class1_anchor']!r} not in an AGENTS.md file"
            )
        if e["disposition"] == "moved":
            cf = REPO_ROOT / e["context_file"]
            assert cf.exists(), f"{e['id']}: context_file {e['context_file']} missing"


def test_context_conservation() -> None:
    files = sorted(CONTEXT_DIR.glob("*.md"))
    assert files, "no docs/context files"
    total = sum(f.stat().st_size for f in files)
    assert total >= CONTEXT_FLOOR_BYTES, f"context total {total} B < floor {CONTEXT_FLOOR_BYTES} B"
    for f in files:
        assert f.stat().st_size >= 500, f"{f.name} too small ({f.stat().st_size} B)"
