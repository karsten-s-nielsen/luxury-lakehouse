"""HF Hub ↔ in-repo card inventory parity (PR 4c).

Enforces the core invariant of the shared ``hf_publish`` helper:

    every HF repo under ``luxury-lakehouse/`` has an in-repo card
    AND every in-repo card's basename matches an HF repo

If a new HF dataset / model is published without an accompanying in-repo
card, this test fails (catches missing documentation). If an in-repo card
is orphaned — no HF repo of that name — this test fails (catches drift
after a repo rename or deletion).

The test is **online by default** (queries HF Hub). It skips gracefully
when HF_TOKEN is absent or the HF API is unreachable, so the local test
suite stays green on air-gapped CI environments. When HF_TOKEN is
present (any CI run that has Hub access), it runs and enforces parity.

Exclusions:

- ``luxury-lakehouse/build-artifacts`` — this is the wheel-hosting repo;
  it does not carry a per-artifact README card.
- Private / app Spaces (``soccer-analytics-app``, ``staging``) — their
  README lives with the deployable app source (``hf_taipy_app/``), not
  under ``docs/huggingface/``.

Both exclusion sets are explicit constants below so the list of things
"deliberately not enforced" stays visible to future readers.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

_REPO_ROOT = Path(__file__).parent.parent.parent
_DATASET_CARDS_DIR = _REPO_ROOT / "docs" / "huggingface" / "dataset-cards"
_MODEL_CARDS_DIR = _REPO_ROOT / "docs" / "huggingface" / "model-cards"

_HF_ORG = "luxury-lakehouse"

# Datasets that exist on HF Hub but are deliberately NOT required to have
# an in-repo card. Currently empty — every dataset has a card after PR 4c.
_DATASET_CARD_EXEMPT: frozenset[str] = frozenset()

# In-repo dataset cards whose HF dataset is pending first publish.
# Remove from this set once the publisher runs successfully.
_DATASET_CARD_ORPHAN_EXEMPT: frozenset[str] = frozenset(
    {
        "spadl-action-context",  # AC-1 — created in this PR, publisher not yet run
        # ADR-049 — private companion repos; auto-created by each publisher's
        # first run after the ADR-049 PR. Remove each once that run completes
        # (org-scoped tokens DO list private repos, so parity enforcement
        # picks them up the moment they exist).
        "spadl-vaep-action-values-restricted",
        "spadl-action-context-restricted",
    }
)

# Models that exist on HF Hub but deliberately lack an in-repo card.
# ``build-artifacts`` hosts the pre-built wheel (no README).
# ``xg-model-statsbomb-wyscout`` is the retired xG v1 HF repo (XG1-RETIRE,
# SK3-MIG-B 2026-05-03); the in-repo card was deleted in Phase 4.5. The HF
# Hub repo is left in-place for historical reproducibility; an operator
# follow-up may delete it post-PR-alpha.
_MODEL_CARD_EXEMPT: frozenset[str] = frozenset(
    {
        "build-artifacts",
        "xg-model-statsbomb-wyscout",
    }
)


def _hf_token_or_skip() -> str:
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        pytest.skip("HF_TOKEN not set — HF Hub parity test skipped (air-gapped CI)")
    return token


def _skip_on_network_error(exc: Exception, kind: str) -> None:
    """Skip instead of fail when HF Hub is unreachable.

    Anything that looks like a connection / DNS / timeout problem maps to
    a skip; authentic test assertions below still fail the test normally.
    """
    msg = str(exc).lower()
    network_signatures = (
        "connection",
        "timeout",
        "max retries exceeded",
        "name resolution",
        "temporary failure",
    )
    if any(sig in msg for sig in network_signatures):
        pytest.skip(f"HF Hub unreachable — parity test for {kind} skipped: {type(exc).__name__}: {exc}")
    raise exc


def _list_hf_datasets(token: str) -> set[str]:
    from huggingface_hub import HfApi

    try:
        return {d.id.split("/", 1)[1] for d in HfApi(token=token).list_datasets(author=_HF_ORG, token=token)}
    except Exception as exc:
        _skip_on_network_error(exc, "datasets")
        raise  # pragma: no cover — _skip_on_network_error always either skips or re-raises


def _list_hf_models(token: str) -> set[str]:
    from huggingface_hub import HfApi

    try:
        return {m.id.split("/", 1)[1] for m in HfApi(token=token).list_models(author=_HF_ORG, token=token)}
    except Exception as exc:
        _skip_on_network_error(exc, "models")
        raise  # pragma: no cover


def _iter_card_basenames(cards_dir: Path) -> Iterator[str]:
    for p in cards_dir.iterdir():
        if p.is_file() and p.suffix == ".md":
            yield p.stem


class TestDatasetCardParity:
    def test_every_hf_dataset_has_an_in_repo_card(self) -> None:
        token = _hf_token_or_skip()
        hf_datasets = _list_hf_datasets(token)
        in_repo = set(_iter_card_basenames(_DATASET_CARDS_DIR))
        missing = hf_datasets - in_repo - _DATASET_CARD_EXEMPT
        assert not missing, (
            f"HF datasets without an in-repo card: {sorted(missing)}. "
            f"Add docs/huggingface/dataset-cards/<name>.md or list the repo in _DATASET_CARD_EXEMPT with reason."
        )

    def test_every_in_repo_card_matches_an_hf_dataset(self) -> None:
        token = _hf_token_or_skip()
        hf_datasets = _list_hf_datasets(token)
        in_repo = set(_iter_card_basenames(_DATASET_CARDS_DIR))
        orphan = in_repo - hf_datasets - _DATASET_CARD_ORPHAN_EXEMPT
        assert not orphan, (
            f"In-repo dataset cards without a matching HF dataset: {sorted(orphan)}. "
            f"Delete the card OR create the HF dataset OR add to _DATASET_CARD_ORPHAN_EXEMPT."
        )


class TestModelCardParity:
    def test_every_hf_model_has_an_in_repo_card(self) -> None:
        token = _hf_token_or_skip()
        hf_models = _list_hf_models(token)
        in_repo_model_basenames = set(_iter_card_basenames(_MODEL_CARDS_DIR))

        # Some model cards intentionally use a suffix that differs from the
        # HF repo name (historical: xg-v2-model-card.md vs xg-v2-model-set-encoder,
        # football2vec-v2-model-card.md vs football2vec-v2). Pre-declare the
        # known aliases so the invariant ignores them.
        # xg-model-statsbomb-wyscout (v1) was retired SK3-MIG-B 2026-05-03 per
        # ADR-023; the HF Hub repo is exempted via _MODEL_CARD_EXEMPT above.
        _aliases: dict[str, str] = {
            "xg-v2-model-set-encoder": "xg-v2-model-card",
            "football2vec-v2": "football2vec-v2-model-card",
            "football2vec-360": "football2vec-360-model-card",
            "obso-pausa-method": "obso-pausa",
            "space-creation-method": "space-creation",
        }

        missing: list[str] = []
        for hf_name in hf_models:
            if hf_name in _MODEL_CARD_EXEMPT:
                continue
            alias = _aliases.get(hf_name, hf_name)
            if alias not in in_repo_model_basenames:
                missing.append(hf_name)
        assert not missing, (
            f"HF models without an in-repo card: {sorted(missing)}. "
            f"Add docs/huggingface/model-cards/<name>.md or list the repo in _MODEL_CARD_EXEMPT with reason."
        )

    def test_every_in_repo_model_card_matches_an_hf_model(self) -> None:
        token = _hf_token_or_skip()
        hf_models = _list_hf_models(token)
        # Allow known aliases (stem → HF repo basename).
        # xg-model-card (v1) deleted in SK3-MIG-B per ADR-023.
        _card_to_repo: dict[str, str] = {
            "xg-v2-model-card": "xg-v2-model-set-encoder",
            "football2vec-v2-model-card": "football2vec-v2",
            "football2vec-360-model-card": "football2vec-360",
            "obso-pausa": "obso-pausa-method",
            "space-creation": "space-creation-method",
        }
        in_repo = set(_iter_card_basenames(_MODEL_CARDS_DIR))

        orphan: list[str] = []
        for stem in in_repo:
            hf_name = _card_to_repo.get(stem, stem)
            if hf_name not in hf_models:
                orphan.append(stem)
        assert not orphan, (
            f"In-repo model cards without a matching HF model: {sorted(orphan)}. "
            f"Delete the card OR create the HF model."
        )


def test_every_publisher_script_calls_upload_hf_readme() -> None:
    """HF4 invariant 2 (ADR-014 amendment 2026-05-03): every scripts/publish_*_hf.py
    and scripts/compute_*_hf.py that uploads to HF Hub MUST call
    ingestion.hf_publish.upload_hf_readme. Closes the parity gap end-to-end.
    """
    import ast as _ast
    from pathlib import Path as _Path

    repo_root = _Path(__file__).resolve().parents[2]
    scripts_dir = repo_root / "scripts"

    # In-scope: any script that uploads HF datasets/models.
    in_scope = sorted(list(scripts_dir.glob("publish_*_hf.py")) + list(scripts_dir.glob("compute_*_hf.py")))

    missing_call: list[str] = []
    for py_file in in_scope:
        tree = _ast.parse(py_file.read_text(encoding="utf-8"))
        found_call = False
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Call):
                if isinstance(node.func, _ast.Name) and node.func.id == "upload_hf_readme":
                    found_call = True
                    break
                if isinstance(node.func, _ast.Attribute) and node.func.attr == "upload_hf_readme":
                    found_call = True
                    break
        if not found_call:
            missing_call.append(str(py_file.relative_to(repo_root)))

    assert not missing_call, (
        "These HF publisher scripts do NOT call upload_hf_readme (ADR-014 violation):\n  "
        + "\n  ".join(missing_call)
        + "\nAdd `from ingestion.hf_publish import upload_hf_readme` and call it post-upload."
    )


# ---------------------------------------------------------------------------
# ADR-049 — restricted-data lockstep guards
# ---------------------------------------------------------------------------

_REPO_SCRIPTS_DIR = _REPO_ROOT / "scripts"


def _imported_names_from_hf_publish(py_file: Path) -> set[str]:
    """Names a script imports from ingestion.hf_publish (AST, no execution)."""
    import ast

    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "ingestion.hf_publish":
            names.update(alias.name for alias in node.names)
    return names


# Publishers migrated to the ADR-049 restricted split, with the in-repo card
# of each one's PRIVATE companion repo. Mode membership (split vs legacy SQL
# exclusion) is canonically enforced by test_gradientsports_hf_exclusion.py;
# this list pins the ADR-049 mechanics of each split publisher.
_ADR049_SPLIT_PUBLISHER_CARDS: dict[str, str] = {
    "publish_action_context_hf.py": "spadl-action-context-restricted.md",
    "publish_spadl_vaep_hf.py": "spadl-vaep-action-values-restricted.md",
}


class TestRestrictedPublishLockstep:
    """ADR-049: the publish split and the training-corpus expectation derive
    from the SAME constant in ingestion.hf_publish. These guards fail the
    moment either side reverts to a local filter (the Champion-v10 corpus
    bug: the trainer silently inherited a SQL-side publish filter).
    """

    _TRAINER: ClassVar[Path] = _REPO_SCRIPTS_DIR / "train_vaep_model_hf.py"

    @pytest.mark.parametrize("publisher", sorted(_ADR049_SPLIT_PUBLISHER_CARDS))
    def test_publisher_imports_shared_split_helpers(self, publisher: str) -> None:
        names = _imported_names_from_hf_publish(_REPO_SCRIPTS_DIR / publisher)
        missing = {"RESTRICTED_HF_PROVIDERS", "restricted_repo_id", "split_restricted"} - names
        assert not missing, (
            f"{publisher} must import {sorted(missing)} from ingestion.hf_publish "
            "(ADR-049 single source of truth — no local restriction filters)."
        )

    def test_trainer_imports_shared_restriction_constants(self) -> None:
        names = _imported_names_from_hf_publish(self._TRAINER)
        missing = {"RESTRICTED_HF_PROVIDERS", "restricted_repo_id"} - names
        assert not missing, (
            f"{self._TRAINER.name} must import {sorted(missing)} from ingestion.hf_publish "
            "(ADR-049: the corpus expectation derives from the publish-split constant)."
        )

    @pytest.mark.parametrize("publisher", sorted(_ADR049_SPLIT_PUBLISHER_CARDS))
    def test_publisher_sql_does_not_filter_providers(self, publisher: str) -> None:
        # The license gate lives at the PUBLISH split, never in the SQL — a
        # SQL-side filter is exactly what the trainer inherited unnoticed.
        source = (_REPO_SCRIPTS_DIR / publisher).read_text(encoding="utf-8")
        assert "data_source !=" not in source and "data_source <>" not in source, (
            f"{publisher} filters providers in SQL — move the gate to split_restricted (ADR-049)."
        )

    @pytest.mark.parametrize("publisher", sorted(_ADR049_SPLIT_PUBLISHER_CARDS))
    def test_publisher_delete_patterns_sweep_whole_path_in_repo(self, publisher: str) -> None:
        # hf_hub matches delete_patterns against paths RELATIVE to path_in_repo
        # (_prepare_folder_deletions strips the prefix before filtering), so a
        # "data/"-prefixed pattern matches NOTHING and silently no-ops — that
        # no-op left legacy Spark part-files inside partition dirs for months
        # (double-count hazard). The only correct sweep pattern is "**".
        # Re-uploaded files are pruned from the delete set by upload_folder.
        import ast

        tree = ast.parse((_REPO_SCRIPTS_DIR / publisher).read_text(encoding="utf-8"))
        delete_patterns: list[list[str]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "delete_patterns" and isinstance(kw.value, ast.List):
                        delete_patterns.append(
                            [
                                el.value
                                for el in kw.value.elts
                                if isinstance(el, ast.Constant) and isinstance(el.value, str)
                            ]
                        )
        assert delete_patterns, f"{publisher}: no upload_folder(delete_patterns=...) found"
        for patterns in delete_patterns:
            assert patterns == ["**"], (
                f"delete_patterns {patterns!r} must be ['**'] — patterns are matched "
                "RELATIVE to path_in_repo, so any 'data/'-prefixed pattern silently "
                "no-ops and stale files survive (ADR-049, verified 2026-06-10)."
            )

    @pytest.mark.parametrize("card", sorted(_ADR049_SPLIT_PUBLISHER_CARDS.values()))
    def test_restricted_card_exists_for_restricted_repo(self, card: str) -> None:
        # The private companion repo rides the same ADR-014 card mechanism.
        assert (_DATASET_CARDS_DIR / card).is_file(), (
            f"ADR-049 restricted companion repo is missing its dataset card: {card}"
        )

    @pytest.mark.parametrize("publisher", sorted(_ADR049_SPLIT_PUBLISHER_CARDS))
    def test_publisher_uploads_restricted_card(self, publisher: str) -> None:
        # The restricted card must actually ride with the publish (ADR-014):
        # the publisher source must reference its restricted card filename.
        source = (_REPO_SCRIPTS_DIR / publisher).read_text(encoding="utf-8")
        card = _ADR049_SPLIT_PUBLISHER_CARDS[publisher]
        assert card in source, f"{publisher} does not upload its restricted card {card!r} (ADR-014/ADR-049)."
