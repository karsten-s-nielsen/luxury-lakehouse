"""PR-2a visibility plumbing (spec 2026-08-06, §9 test table).

The over-restriction guard: without R-16's open-path stamp, R-6 threads visibility=None and
the PR-2b flip restricts the ENTIRE open corpus (spec Finding 5).

Review B3/B4: these test the WIRING and construct the FLIP. An earlier draft tested only the
pure helper the plan itself creates — which cannot be wrong — and asserted classifier
behaviour that already existed, so neither guarded the defect they were named for.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

_INGESTION = Path(__file__).resolve().parents[1] / "ingestion"

_PROVIDERS = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports"})


def _stamp_calls() -> list[ast.Call]:
    tree = ast.parse((_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8"))
    return [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id in {"_stamp_tier", "stamp_access_tier"}
    ]


def _calls_for(provider: str) -> list[ast.Call]:
    return [
        c
        for c in _stamp_calls()
        for k in c.keywords
        if k.arg == "source" and isinstance(k.value, ast.Constant) and k.value.value == provider
    ]


# --------------------------------------------------------------------------------------
# R-16 — the open-path stamp
# --------------------------------------------------------------------------------------


def test_open_statsbomb_ingest_stamps_public_visibility() -> None:
    from ingestion.statsbomb import stamp_open_match_visibility

    out = stamp_open_match_visibility(pd.DataFrame({"match_id": [1, 2]}))
    assert list(out["visibility"]) == ["public", "public"]
    assert list(out["access_tier"]) == ["public", "public"]


def test_the_stamp_precedes_the_statsbomb_matches_write() -> None:
    """B3 + E2 — the defect lives in the CALL, and in WHERE the call sits.

    A helper that is never invoked leaves every row NULL, and the PR-2b flip then restricts
    the whole corpus. Asserted via AST because the enclosing ingest function needs Spark.

    E2: an earlier draft set `wired = True` if the helper appeared ANYWHERE in a function
    that also mentioned the string "statsbomb_matches". That function mentions it three
    times across ~180 lines, so the test went green with the stamp placed AFTER the write —
    i.e. on the exact defect it is named for. Compare line numbers instead.
    """
    source = (_INGESTION / "statsbomb.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    stamp_lines = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "stamp_open_match_visibility"
    ]
    assert stamp_lines, "stamp_open_match_visibility is never called. The helper alone stamps nothing (B3)."

    write_lines = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "write_delta_table"
        and any(isinstance(a, ast.Constant) and a.value == "statsbomb_matches" for a in n.args)
    ]
    assert len(write_lines) == 1, (
        f"expected exactly one write_delta_table(..., 'statsbomb_matches', ...); found "
        f"{len(write_lines)}. If the write moved or was duplicated, re-verify this anchor "
        f"rather than loosening the assertion."
    )

    assert min(stamp_lines) < write_lines[0], (
        f"stamp_open_match_visibility is called at line(s) {stamp_lines} but the "
        f"statsbomb_matches write is at line {write_lines[0]}. Stamping after the write "
        f"leaves every persisted row NULL (E2)."
    )


def test_stamped_row_survives_the_pr2b_flip_and_unstamped_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B4 — actually CONSTRUCT the flip instead of restating classifier behaviour.

    Both halves matter: a stamped row stays public without the allowlist (that is why R-16
    is a precondition of PR-2b), and an UNSTAMPED row goes restricted (that is the Finding-5
    corpus withdrawal this whole unit exists to prevent).
    """
    import shared.access_tier as at

    monkeypatch.setattr(at, "PUBLIC_BY_LICENSE_PROVIDERS", frozenset({"wyscout", "idsse", "metrica"}))

    assert at.classify_access_tier(provider="statsbomb", visibility="public").value == "public"
    assert at.classify_access_tier(provider="statsbomb", visibility=None).value == "restricted"


# --------------------------------------------------------------------------------------
# R-6a — required-no-default, guarded mechanically
# --------------------------------------------------------------------------------------


def test_every_stamp_call_passes_visibility_explicitly() -> None:
    """R-6a — the signature is required-no-default, but nothing in CI executes these calls.

    They live inside applyInPandas closures that only run under Spark, so a missing argument
    would surface as a TypeError on Databricks months later — the audit-finding latency R-6a
    exists to remove. This gate is the mechanical guard (review B2).
    """
    calls = _stamp_calls()
    assert calls, "found ZERO _stamp_tier calls — the parser is broken, not the source"
    missing = [c.lineno for c in calls if not any(k.arg == "visibility" for k in c.keywords)]
    assert not missing, f"_stamp_tier call(s) at line(s) {missing} pass no visibility (R-6a)"


def test_stamp_call_providers_match_the_known_set_both_ways() -> None:
    """Both directions: a seventh converter must register here; a removed one must be dropped.

    A per-provider regex structurally cannot catch the provider nobody has written yet.
    """
    seen = {
        k.value.value
        for c in _stamp_calls()
        for k in c.keywords
        if k.arg == "source" and isinstance(k.value, ast.Constant)
    }
    assert seen == _PROVIDERS, f"provider drift: source= literals {seen} != known {_PROVIDERS}"


def test_stamp_access_tier_has_no_visibility_default() -> None:
    """R-6a at the definition, not just the call sites.

    Re-adding a default would make every AST call-site assertion above pass while restoring
    the exact ambiguity R-6a removes: "forgot to thread" vs "has no feed".
    """
    import inspect

    from ingestion.spadl_udf_shared import stamp_access_tier

    param = inspect.signature(stamp_access_tier).parameters["visibility"]
    assert param.default is inspect.Parameter.empty, (
        "stamp_access_tier.visibility has a default again — R-6a requires it be explicit at every call site."
    )
    assert param.kind is inspect.Parameter.KEYWORD_ONLY, "visibility must stay keyword-only"


# --------------------------------------------------------------------------------------
# R-6 — StatsBomb threads a REAL signal
# --------------------------------------------------------------------------------------


def test_statsbomb_threads_a_real_visibility_not_none() -> None:
    """R-6 — `visibility=None` at the statsbomb site IS the Finding-5 defect.

    rev 1's assertion only required the substring `visibility=`, which `visibility=None`
    satisfies — it passed on exactly the defect it named (review B1).
    """
    sb = _calls_for("statsbomb")
    assert sb, "no statsbomb _stamp_tier call found"
    for call in sb:
        vis = next(k for k in call.keywords if k.arg == "visibility")
        assert not (isinstance(vis.value, ast.Constant) and vis.value.value is None), (
            "statsbomb passes visibility=None — after the PR-2b flip that fails safe to "
            "RESTRICTED and withholds the entire open corpus (spec Finding 5 / R-6)."
        )


def test_statsbomb_matches_projection_includes_visibility() -> None:
    """R-6 — the caller-side projection is an EXPLICIT column list, so it can drop the signal.

    Found during execution, not review: the StatsBomb converter reads its match metadata via
    `spark.table(matches_table).select("match_id", "home_team")`. Threading code that builds a
    visibility_map from that frame gets an EMPTY map, so every match threads None — the
    Finding-5 over-restriction, arriving silently rather than as a failure.

    This is the same defect class as the Gradient Sports `_gs_needed_bronze_columns()` backtick
    projection. Two providers, two explicit column lists, one trap.
    """
    source = (_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    projections = [
        {a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)}
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "select"
        and any(isinstance(a, ast.Constant) and a.value == "home_team" for a in n.args)
    ]
    assert projections, (
        "could not find the StatsBomb matches projection (anchored on the 'home_team' literal) "
        "— re-verify this anchor rather than deleting the test."
    )
    for cols in projections:
        assert "visibility" in cols, (
            f"the StatsBomb matches projection {sorted(cols)} omits 'visibility'. visibility_map "
            f"would be empty and every match would thread None (R-6 / spec Finding 5)."
        )


# --------------------------------------------------------------------------------------
# R-6b — the Gradient Sports decision, written down
# --------------------------------------------------------------------------------------


def test_gradientsports_visibility_decision_is_explicit_and_reasoned() -> None:
    """R-6b — whichever branch was taken, it must be WRITTEN DOWN.

    After R-6a the omission is no longer visible as an omission, so the status quo must not be
    able to survive by silence.
    """
    src = (_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8")
    gs = _calls_for("gradientsports")
    assert gs, "no gradientsports _stamp_tier call found"
    assert any(k.arg == "visibility" for k in gs[0].keywords), "R-6b: must pass visibility"

    # Anchor on the STAMP call's own line, not on the first `source="gradientsports"` literal
    # in the file — `_apply_pid_native(actions, source="gradientsports")` carries the same
    # literal and appears earlier, so a plain str.index() inspects the wrong site's comments.
    stamp_lineno = gs[0].lineno
    window = "\n".join(src.splitlines()[max(0, stamp_lineno - 16) : stamp_lineno])
    assert "R-6b" in window, (
        "R-6b: the GS _stamp_tier call must cite R-6b in a comment stating WHY. Window "
        f"inspected: lines {max(1, stamp_lineno - 15)}-{stamp_lineno}."
    )


def test_gradientsports_threads_a_real_visibility_not_none() -> None:
    """R-6b, Unit B specifically: the threaded signal must be REAL.

    Unit A parked this site at an explicit `None` so the AST gate could go green at commit 1,
    and that placeholder satisfies both assertions above. Without this negative half, the
    decision test would certify Unit A's placeholder as Unit B's deliverable — the same shape
    as review B1, where an assertion passed on the exact defect it named.
    """
    gs = _calls_for("gradientsports")
    assert gs, "no gradientsports _stamp_tier call found"
    vis = next(k for k in gs[0].keywords if k.arg == "visibility")
    assert not (isinstance(vis.value, ast.Constant) and vis.value.value is None), (
        "gradientsports still passes visibility=None — that is Unit A's placeholder, not the threaded signal (R-6b)."
    )


def test_gradientsports_visibility_arrives_via_a_metadata_join() -> None:
    """R-6b — the signal must come from a JOIN, not a driver-side capture.

    The GS converter has no pre-existing lookup frame (unlike StatsBomb's `home_sdf`), so one
    must be built from bronze.gradientsports_metadata. Serverless captures closures lazily, so
    a driver-side dict would simply not be present on the executor.

    The join must also be LEFT: an inner join silently DROPS matches present in events but
    absent from metadata, trading an over-restriction for outright data loss.
    """
    source = (_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8")
    assert "gradientsports_metadata" in source, (
        "no reference to bronze.gradientsports_metadata — the GS visibility join is missing."
    )
    tree = ast.parse(source)
    joins = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "join"
        and any(k.arg == "on" and isinstance(k.value, ast.Constant) and k.value.value == "match_id" for k in n.keywords)
    ]
    gs_left = [
        j
        for j in joins
        if any(k.arg == "how" and isinstance(k.value, ast.Constant) and k.value.value == "left" for k in j.keywords)
    ]
    assert gs_left, (
        "found no LEFT join on match_id. The GS metadata join must be how='left' — an inner "
        "join drops events-only matches from SPADL entirely (R-6b)."
    )


# --------------------------------------------------------------------------------------
# R-19 — preconditions cannot outlive their checks
# --------------------------------------------------------------------------------------


def test_every_override_names_a_precondition_that_exists_as_a_callable() -> None:
    """R-19 / A3 — the entry cannot outlive the check.

    A name->name registry passes even after the function is deleted. Mapping to the callable
    makes deletion break the map.
    """
    from ingestion.access_tier_backfill import _EXISTING_CONFIRMED_PUBLIC, _PRECONDITIONS

    assert _EXISTING_CONFIRMED_PUBLIC, "override map is empty — the guard is vacuous"
    for provider, (tier, precondition) in _EXISTING_CONFIRMED_PUBLIC.items():
        assert tier in {"public", "restricted"}, f"{provider}: bad tier {tier!r}"
        assert precondition in _PRECONDITIONS, f"{provider}: {precondition!r} not registered"
        assert callable(_PRECONDITIONS[precondition]), (
            f"{precondition!r} maps to a name, not a callable — deleting the check would "
            "leave the override standing (review A3)"
        )


def test_no_orphan_preconditions() -> None:
    """A3 — the REVERSE direction: a registered check nothing references is dead on arrival."""
    from ingestion.access_tier_backfill import _EXISTING_CONFIRMED_PUBLIC, _PRECONDITIONS

    referenced = {p for _tier, p in _EXISTING_CONFIRMED_PUBLIC.values()}
    orphans = set(_PRECONDITIONS) - referenced
    assert not orphans, f"precondition(s) {orphans} are registered but referenced by no override"


def test_statsbomb_is_not_a_confirmed_public_override() -> None:
    """D4 — registering statsbomb here would defeat the PR-2b fail-safe.

    default_tier_for_provider returns an override INSTEAD of consulting the classifier, so a
    statsbomb entry would keep resolving 'public' after PR-2b removes statsbomb from
    PUBLIC_BY_LICENSE_PROVIDERS — in the one module whose job is confirmed-public facts.
    The OQ-1 precondition statement lives in the migration comment instead.
    """
    from ingestion.access_tier_backfill import _EXISTING_CONFIRMED_PUBLIC

    assert "statsbomb" not in _EXISTING_CONFIRMED_PUBLIC, (
        "statsbomb must NOT be a confirmed-public override — that hardcodes 'public' past "
        "the PR-2b flip and defeats the fail-safe (review D4)."
    )


def test_the_plan_emits_preconditions_before_the_backfills() -> None:
    """A3 / E1 — a registry nothing consults is decoration. Assert the ORDER.

    E1 killed two things in rev 2's version of this test. First, it called
    `build_backfill_statements(["skillcorner"])` — the real signature is keyword-only with a
    tuple, so that raises TypeError and the test errors rather than fails. Second, it asserted
    only that both builders return non-empty output; nothing asserted precedence. Since there
    is no driver in the repo, no caller exists to assert on either — so the coupling has to be
    a property of the CODE. One builder emitting an ordered plan gives it that.
    """
    from ingestion.access_tier_backfill import build_backfill_plan, build_precondition_statements

    pre = build_precondition_statements(providers=("skillcorner",))
    assert pre, "no precondition statements emitted for an override-carrying provider"
    assert all("count(*)" in s.lower() for s in pre), "a precondition must be an answerable query"

    plan = build_backfill_plan(providers=("skillcorner",))
    assert len(plan) > len(pre), "the plan must carry backfills as well as preconditions"
    assert plan[: len(pre)] == pre, (
        "preconditions must come FIRST in the plan. Two independent builders can be run "
        "independently; an ordered plan cannot be (E1)."
    )


def test_default_tier_for_provider_stays_pure() -> None:
    """A2 — the purity argument must survive R-19, not be made and then broken."""
    import inspect

    import ingestion.access_tier_backfill as m

    assert list(inspect.signature(m.default_tier_for_provider).parameters) == ["provider"]
    src = (_INGESTION / "access_tier_backfill.py").read_text(encoding="utf-8")
    for banned in ("import pyspark", "spark.sql", "def _scalar", "conn."):
        assert banned not in src, f"{banned!r} in access_tier_backfill.py — it executes nothing (A2)"
