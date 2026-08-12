"""The pre-commit ruff hook must pin ``uv.lock``'s ruff (ADR-046 lockstep, 2026-08-12).

WHAT THIS GUARDS
----------------
``.pre-commit-config.yaml`` pins its own ruff via ``rev:``, independently of ``uv.lock``.
``.github/dependabot.yml`` declares ``uv``, ``github-actions`` and ``terraform`` — **not
pre-commit** — so nothing bumps that rev and nothing noticed when it fell behind.

Measured during the ruff 0.15.22 -> 0.16.1 bump (#512): ``uv run ruff check src/ scripts/``
reported *All checks passed* while the identical check in the commit hook rejected the **same
nine lines** under BLE001. 0.16.1 refined the rule — it stopped flagging handlers that log with
``exc_info=True``, which preserve the traceback — so the two linters genuinely disagreed about
what the code meant. Two verdicts on one working tree, and whichever ran last "won".

That is worse than either being wrong: `uv run ruff` is what CI enforces and what this
project's Code Quality section documents, while the hook is what actually blocks a commit. A
developer who trusts the documented command gets stopped by a tool nobody told them about.

FIXER =/= CHECKER IS THE FAILURE MODE, SO THEY SHARE A CORE
-----------------------------------------------------------
The rewrite lives in ``scripts/_tf_env_pins.rewrite_precommit_ruff_rev`` and is invoked by
``scripts/sync_tf_env_pins.py``; this module asserts the *result*. Same shape as
``test_ci_dbt_pin_parity.py`` and for the same reason: a checker that re-implements the fixer's
notion of "correct" can drift from it, and then both pass while disagreeing.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts._tf_env_pins import PRECOMMIT_CONFIG, parse_lock_versions, rewrite_precommit_ruff_rev

_REPO = Path(__file__).resolve().parents[2]
_PRECOMMIT_REV_RE = re.compile(
    r"repo:\s*https://github\.com/astral-sh/ruff-pre-commit\s*\n(?:[^\n]*\n)*?\s*rev:\s*v?([\w.]+)"
)


def _lock() -> dict[str, set[str]]:
    return parse_lock_versions((_REPO / "uv.lock").read_text(encoding="utf-8"))


def _lock_ruff() -> str:
    versions = _lock().get("ruff", set())
    assert len(versions) == 1, f"expected exactly one ruff version in uv.lock, got {sorted(versions)}"
    return next(iter(versions))


def test_the_lock_pin_is_discoverable() -> None:
    """Non-vacuity. If uv.lock parsing silently yielded nothing, every check below would pass
    against an empty set forever — the failure mode this repo has paid for more than once."""
    assert _lock_ruff()


def test_the_precommit_hook_pins_the_locked_ruff() -> None:
    """The property itself: one ruff version across the repo."""
    text = (_REPO / PRECOMMIT_CONFIG).read_text(encoding="utf-8")
    found = _PRECOMMIT_REV_RE.findall(text)
    assert found, f"{PRECOMMIT_CONFIG}: no ruff-pre-commit `rev:` found — this scanner is looking in the wrong place"
    want = _lock_ruff()
    assert all(rev == want for rev in found), (
        f"{PRECOMMIT_CONFIG} pins ruff {found}, uv.lock has {want}. "
        "Run `uv run python scripts/sync_tf_env_pins.py` — do not hand-edit."
    )


def test_the_fixer_agrees_the_file_is_already_in_sync() -> None:
    """Fixer =/= checker is the defect this shape exists to prevent, so assert they agree.

    Running the real rewrite over the real file must report no drift and change no bytes. If the
    checker's regex and the fixer's regex ever diverge, this fails even when the check above
    passes.
    """
    path = _REPO / PRECOMMIT_CONFIG
    text = path.read_text(encoding="utf-8")
    rewritten, drifts = rewrite_precommit_ruff_rev(text, _lock())
    assert not drifts, f"fixer reports drift the checker did not: {drifts}"
    assert rewritten == text, "fixer would rewrite a file the checker considers in sync"


def test_the_fixer_actually_rewrites_a_drifted_rev() -> None:
    """A fixer that silently no-ops would keep this pin unguarded while reporting success.

    Uses a deliberately stale rev, so the test fails if the rewrite stops matching the config's
    shape — the way a `rev:` moved into a different block would break it.
    """
    text = (_REPO / PRECOMMIT_CONFIG).read_text(encoding="utf-8")
    stale = _PRECOMMIT_REV_RE.sub(
        lambda m: m.group(0).replace(m.group(1), "0.0.1"),
        text,
        count=1,
    )
    assert "0.0.1" in stale, "could not construct the drifted fixture — the regex is stale"

    rewritten, drifts = rewrite_precommit_ruff_rev(stale, _lock())
    assert drifts, "fixer found no drift in a file pinned to ruff 0.0.1"
    assert drifts[0].pkg == "ruff"
    assert drifts[0].desired == f"v{_lock_ruff()}"
    assert "0.0.1" not in rewritten
    assert f"v{_lock_ruff()}" in rewritten


def _pinned_revs(text: str) -> dict[str, str]:
    """{repo url: rev} for every ACTIVE hook repo.

    Parsed, not grepped. The config carries a commented-out ``sqlfluff`` block whose ``rev:``
    sits behind a ``#`` — a regex sweep matches the repo line, finds no rev, and reports a
    failure that is really its own. yaml skips comments by construction. (Reading only: this
    file is never round-tripped through a dumper, which would eat the comments for real.)
    """
    import yaml

    doc = yaml.safe_load(text) or {}
    return {r["repo"]: str(r.get("rev", "")) for r in (doc.get("repos") or []) if "repo" in r}


def test_only_the_ruff_block_is_rewritten() -> None:
    """Other hooks pin their own revs. Rewriting those to ruff's version would be a silent,
    repo-wide downgrade of unrelated tooling — so the pattern is anchored on the repo URL."""
    text = (_REPO / PRECOMMIT_CONFIG).read_text(encoding="utf-8")
    before = _pinned_revs(text)
    others = {url: rev for url, rev in before.items() if "ruff-pre-commit" not in url}
    assert others, "no non-ruff hook repos in the config — this guard is vacuous"

    after = _pinned_revs(rewrite_precommit_ruff_rev(text, _lock())[0])
    for url, rev in others.items():
        assert after.get(url) == rev, f"{url}: rev changed from {rev} to {after.get(url)}"
