"""The `MatchWriter` collaborator (M1 extraction, plan Task 4): choose the kept copy, write the bronze + lineage,
record a not-kept copy, move on a visibility change, and read only the identity fields of `ModuleContext`.

The extraction's behavior preservation is proven end-to-end by the oracle suite (test_run_history, test_run_local,
test_no_private_rows, test_crash_recovery, test_dbt_handoff); these focused tests exercise `MatchWriter` in
isolation and each carries a mutant (plan Task 4 Step 5)."""

from __future__ import annotations

from typing import Any

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, checkpoints_for, entry

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.ids.keys import match_key
from lakehouse.lineage.model import LineageRow, read_lineage
from lakehouse.parsers.base import ParsedMatch
from lakehouse.raw.archive import Delivery, RawArchive
from lakehouse.runs._common import ModuleContext
from lakehouse.runs.history import read_table
from lakehouse.runs.match_files import MatchFiles
from lakehouse.runs.match_writer import MatchWriter

KEY = match_key("idsse", "J03WPY")


def _bronze(kit: LakeKit, root: str, key: int) -> list[str]:
    """The match's physical bronze objects under a root (the move relocates these; `swap` rebuilds the registry,
    so the physical objects — not `ops_match_files.root` — are what makes a dropped/reordered move observable)."""
    return [k for k in kit.objects(root, "data/bronze/") if k.rsplit("/", 1)[-1].startswith(f"{key}-")]


class _Errors:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(
        self,
        *,
        kind: str,
        message: str,
        severity: str,
        module_run_id: str | None = None,
        module_id: str | None = None,
        native: str | None = None,
    ) -> None:
        self.calls.append(kind)


class _RaisingContext:
    """A `ModuleContext` stand-in whose loop fields raise on any access, to prove the writer reads only identity."""

    def __init__(self, base: ModuleContext) -> None:
        self.module = base.module
        self.adapter = base.adapter
        self.parser = base.parser
        self.module_run_id = base.module_run_id

    @property
    def tally(self) -> Any:
        raise AssertionError("MatchWriter read context.tally")

    @property
    def steps(self) -> Any:
        raise AssertionError("MatchWriter read context.steps")


def _writer(kit: LakeKit) -> tuple[MatchWriter, WorkingCatalog]:
    cat = kit.catalog()
    checkpoints = checkpoints_for(cat, kit.storage)
    files = MatchFiles(
        cat, kit.storage, RawArchive(kit.storage, enabled=False), "r", lambda: NOW, checkpoints=checkpoints
    )
    writer = MatchWriter(cat, kit.storage, kit.base / "w", "r", lambda: NOW, files, _Errors(), checkpoints=checkpoints)
    return writer, cat


def _pieces(
    visibility: str = "public", artifacts: tuple[str, ...] = ("events",)
) -> tuple[ModuleContext, Any, dict[str, Delivery], ParsedMatch]:
    files = {("J03WPY", a): f"{a}-x\n{a}-y\n".encode() for a in artifacts}
    adapter = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY", signals=dict.fromkeys(artifacts, "s1"), visibility=visibility)],
        files,
    )
    parser = FakeParser("idsse")
    listed = adapter.entries[0]
    deliveries = {a: adapter.fetch(listed, a) for a in artifacts}
    parsed = parser.parse(listed, deliveries, run_id="r")
    context = ModuleContext(
        module=active("idsse_public"),
        adapter=adapter,  # type: ignore[arg-type]
        parser=parser,  # type: ignore[arg-type]
        module_run_id="r.idsse_public",
    )
    return context, listed, deliveries, parsed


def _public_row() -> LineageRow:
    return LineageRow(
        match_key=KEY,
        provider="idsse",
        channel="public",
        channel_id="J03WPY",
        artifact="events",
        change_signal=None,
        delivery_sha256="h",
        content_sha256="h",
        wrapper="gzip",
        first_seen=NOW,
        last_listed=NOW,
        visibility="public",
        kept=True,
        reason="first",
    )


def test_choose_keeps_public_when_pining_adds_nothing(kit: LakeKit) -> None:
    writer, _ = _writer(kit)
    _, listed, _, parsed = _pieces()
    writer._copy_fields(KEY, "public", parsed)  # pyright: ignore[reportPrivateUsage] - seed the public copy fields
    choice = writer.choose(KEY, "pining", listed, parsed, [_public_row()])
    assert choice.kept == "public"  # pining adds no field the public lacks


def test_choose_keeps_pining_when_it_adds_fields(kit: LakeKit) -> None:
    writer, _ = _writer(kit)
    _, listed, _, parsed = _pieces()
    choice = writer.choose(KEY, "pining", listed, parsed, [_public_row()])
    assert choice.kept == "pining"  # no recorded public copy fields: pining's field is one the public lacks


def test_write_produces_expected_match_and_lineage(kit: LakeKit) -> None:
    writer, cat = _writer(kit)
    context, listed, deliveries, parsed = _pieces()
    choice = writer.choose(KEY, "public", listed, parsed, [])
    writer.write(KEY, context, listed, deliveries, parsed, choice, previous=None)
    match = {r["match_key"]: r for r in read_table(cat.conn, "ops_matches")}[KEY]
    assert (match["provider"], match["native_match_id"], match["visibility"], match["kept_channel"]) == (
        "idsse",
        "J03WPY",
        "public",
        "public",
    )
    lineage = [r for r in read_lineage(cat.conn) if r.match_key == KEY]
    assert lineage and any(r.kept for r in lineage)


def test_write_moves_public_to_private_before_write(kit: LakeKit) -> None:
    writer, cat = _writer(kit)
    context, listed, deliveries, parsed = _pieces("public", ("events", "three_sixty"))
    writer.write(KEY, context, listed, deliveries, parsed, writer.choose(KEY, "public", listed, parsed, []), None)
    previous = {r["match_key"]: r for r in read_table(cat.conn, "ops_matches")}[KEY]
    ctx2, listed2, deliveries2, parsed2 = _pieces("private", ("events",))  # three_sixty is NOT rewritten
    writer.write(KEY, ctx2, listed2, deliveries2, parsed2, writer.choose(KEY, "public", listed2, parsed2, []), previous)
    assert (
        _bronze(kit, "lake", KEY) == []
    )  # the pre-move relocated the old public files (incl. the un-rewritten three_sixty)
    assert _bronze(kit, "restricted", KEY)  # the match's bronze is in the restricted root


def test_write_moves_private_to_public_after_write(kit: LakeKit) -> None:
    writer, cat = _writer(kit)
    context, listed, deliveries, parsed = _pieces("private", ("events", "three_sixty"))
    writer.write(KEY, context, listed, deliveries, parsed, writer.choose(KEY, "public", listed, parsed, []), None)
    previous = {r["match_key"]: r for r in read_table(cat.conn, "ops_matches")}[KEY]
    ctx2, listed2, deliveries2, parsed2 = _pieces("public", ("events",))  # three_sixty is NOT rewritten
    writer.write(KEY, ctx2, listed2, deliveries2, parsed2, writer.choose(KEY, "public", listed2, parsed2, []), previous)
    # The public match has exactly its one current bronze file in lake. A pure DROP of the post-move survives
    # (the swap covers the single-rewrite path redundantly), but the plan's named mutant — the private->public
    # move run BEFORE the write (plan:268) — copies the old restricted files into lake too, so len(lake) > 1.
    # This assertion pins the post-move's ordering (it must run after the write/swap).
    assert len(_bronze(kit, "lake", KEY)) == 1


def test_record_not_kept_writes_lineage_without_bronze(kit: LakeKit) -> None:
    writer, cat = _writer(kit)
    context, listed, deliveries, parsed = _pieces()
    choice = writer.choose(KEY, "pining", listed, parsed, [_public_row()])
    before = len(kit.objects("lake", "data/bronze/"))
    writer.record_not_kept(KEY, context, listed, deliveries, parsed, choice)
    assert len(kit.objects("lake", "data/bronze/")) == before  # no bronze file written for a not-kept copy
    lineage = [r for r in read_lineage(cat.conn) if r.match_key == KEY]
    assert lineage and all(not r.kept for r in lineage)


def test_writer_reads_only_context_identity(kit: LakeKit) -> None:
    writer, cat = _writer(kit)
    context, listed, deliveries, parsed = _pieces()
    raising: Any = _RaisingContext(context)
    choice = writer.choose(KEY, "public", listed, parsed, [])
    writer.write(KEY, raising, listed, deliveries, parsed, choice, previous=None)  # must not touch tally/steps
    assert {r["match_key"] for r in read_table(cat.conn, "ops_matches")} == {KEY}
