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
- Private / app Spaces (``soccer-analytics-app``, ``staging``,
  ``soccer-analytics-demo``) — their README lives with the deployable
  app source (``hf_taipy_app/`` or ``demo_space/``), not under
  ``docs/huggingface/``.

Both exclusion sets are explicit constants below so the list of things
"deliberately not enforced" stays visible to future readers.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

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

# Models that exist on HF Hub but deliberately lack an in-repo card.
# ``build-artifacts`` hosts the pre-built wheel (no README). All other
# luxury-lakehouse model repos have in-repo cards.
_MODEL_CARD_EXEMPT: frozenset[str] = frozenset({"build-artifacts"})


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
        orphan = in_repo - hf_datasets
        assert not orphan, (
            f"In-repo dataset cards without a matching HF dataset: {sorted(orphan)}. "
            f"Delete the card OR create the HF dataset."
        )


class TestModelCardParity:
    def test_every_hf_model_has_an_in_repo_card(self) -> None:
        token = _hf_token_or_skip()
        hf_models = _list_hf_models(token)
        in_repo_model_basenames = set(_iter_card_basenames(_MODEL_CARDS_DIR))

        # Some model cards intentionally use a suffix that differs from the
        # HF repo name (historical: xg-model-card.md vs xg-model-statsbomb-wyscout,
        # football2vec-v2-model-card.md vs football2vec-v2). Pre-declare the
        # known aliases so the invariant ignores them.
        _aliases: dict[str, str] = {
            "xg-model-statsbomb-wyscout": "xg-model-card",
            "xg-v2-model-set-encoder": "xg-v2-model-card",
            "vaep-model-statsbomb-wyscout": "vaep-model",
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
        _card_to_repo: dict[str, str] = {
            "xg-model-card": "xg-model-statsbomb-wyscout",
            "xg-v2-model-card": "xg-v2-model-set-encoder",
            "vaep-model": "vaep-model-statsbomb-wyscout",
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
