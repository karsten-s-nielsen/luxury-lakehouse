"""Shared helper for uploading README.md files to HuggingFace Hub.

Peer module to ``artifact_deploy.py``. The distinction:

- ``artifact_deploy.py`` handles the producer-side *weight* delivery chain
  (MLflow @Champion + UC Volume + HF Hub model weights) per ADR-012.
- ``hf_publish.py`` handles the producer-side *documentation* delivery chain —
  the README.md that rides with each published HF dataset, model, or
  organization Space.

Every publisher that creates or refreshes a HF Hub artifact calls
``upload_hf_readme(...)`` as its final step, after the data / weights have
been uploaded. Because HF uploads are idempotent, re-running a publisher
re-uploads both payload and README without harm.

The data-side peer on the compute layer is ``upload_volume_to_hf_hub``
in ``ingestion.utils`` (Spark → UC Volume → HF Hub). Together the two
helpers eliminate the prior drift between in-repo card markdown and what
consumers see on HF Hub.

Validation posture: fail loud on bad inputs (missing file, empty file,
malformed repo_id, unknown repo_type). Propagate ``HfHubHTTPError`` from
the SDK without catching — silent-swallow is forbidden per ADR-002.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import yaml

if TYPE_CHECKING:
    import pandas as pd

from huggingface_hub import HfApi

import ingestion as _ingestion
from shared.access_tier import RESTRICTED_HF_PROVIDERS, AccessTier

logger = logging.getLogger(__name__)

# HF repo_id shape: 'owner/name', with HF's allowed identifier characters
# (alphanumerics + ._-). No path separators or traversal patterns inside
# either segment.
_REPO_ID_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*/[a-zA-Z0-9][a-zA-Z0-9._-]*$")

_SUPPORTED_REPO_TYPES: frozenset[str] = frozenset({"dataset", "model", "space"})
_SUPPORTED_CARD_KINDS: frozenset[str] = frozenset({"dataset", "model"})

# Card filename convention: basename only (no path separators), must end
# in ``.md``. Enforced so the resolver cannot be tricked into reading an
# arbitrary file via path traversal.
_CARD_NAME_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*\.md$")

# Map kind → subdirectory under ``docs/huggingface/``.
_KIND_TO_SUBDIR: dict[str, str] = {
    "dataset": "dataset-cards",
    "model": "model-cards",
}

# Reference the installed ``ingestion`` package to anchor wheel-path
# resolution. Exposed as a private module-level attribute so tests can
# monkeypatch it to simulate a site-packages layout.
_WHEEL_INGESTION_FILE: Path = Path(_ingestion.__file__).resolve()

# ── Restricted-data publishing convention (ADR-049) ─────────────────────────
#
# Providers whose data is license-gated off PUBLIC HF datasets. Each affected
# dataset has a PERMANENT private companion repo (``restricted_repo_id``); the
# publisher splits rows via ``split_restricted`` and writes each side to its
# repo on EVERY run — including when this set is EMPTY (the empty restricted
# publish sweeps previously-restricted partitions out of the private repo
# while the same run's public publish carries them, so granting a provider
# full permission is exactly one edit here: remove it from this set).
#
# SINGLE SOURCE OF TRUTH: ``RESTRICTED_HF_PROVIDERS`` is defined in the pure stdlib core
# ``shared.access_tier`` and re-exported here (spec D5) so publishers AND trainers can keep importing
# it from ``ingestion.hf_publish`` while the stdlib-only core never imports this pandas/HF adapter
# (no zero-dep violation, no import cycle). It is the per-match classifier's NULL-fallback provider set;
# the per-row ``access_tier`` column (stamped at ingestion) is the authoritative split key.
#
# Current policy: gradientsports is the provider-default RESTRICTED set; SkillCorner is mixed and
# classified per-match by ``shared.access_tier.classify_access_tier`` from its pining ``visibility``.
# (``RESTRICTED_HF_PROVIDERS`` is imported at module top; this block documents the re-export.)


def restricted_repo_id(repo_id: str) -> str:
    """The private companion repo for a public dataset repo (ADR-049 naming convention)."""
    return f"{repo_id}-restricted"


def split_restricted(df: pd.DataFrame, column: str = "access_tier") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a publish DataFrame into ``(public_df, restricted_df)`` (ADR-049 / per-match access tiers).

    The split CRITERION lives only here — call sites depend on this function, not on the constant.

    Default ``access_tier`` mode (per-match): a row is PUBLIC **only if** its tier is exactly
    ``"public"``; ``restricted`` AND ``NULL``/unknown route to the restricted partition — **fail-safe
    (spec D1): never leak an unclassified row.** Both repos may hold partitions for the SAME provider
    (public + private SkillCorner in one frame); consumers already concat + dedup.

    Legacy ``column="data_source"`` mode (provider-level) is retained for any un-migrated caller:
    restricted = ``data_source ∈ RESTRICTED_HF_PROVIDERS``.
    """
    if column == "access_tier":
        # NaN/None/unknown -> not public -> restricted (fail-safe, spec D1). The == comparison yields
        # a pandas *nullable* boolean when the column is the "string"/nullable dtype that publishers'
        # normalize_dtypes produces (astype("string")); a nullable-boolean mask with <NA> silently
        # DROPS the NA row from BOTH df[mask] and df[~mask] (verified: a NULL-tier row vanished from
        # public AND restricted). fillna(False).astype(bool) collapses <NA> to a plain False so the
        # NULL-tier row fail-safes into the restricted partition for every dtype (object OR "string"),
        # honoring this function's documented "never leak an unclassified row" contract.
        is_public = (df[column] == AccessTier.PUBLIC.value).fillna(False).astype(bool)
        return df[is_public], df[~is_public]
    mask = df[column].isin(RESTRICTED_HF_PROVIDERS)
    return df[~mask], df[mask]


def build_provider_configs(
    providers: Iterable[str],
    *,
    path_template: str = "data/{provider}.parquet",
    all_config_name: str = "all",
    all_glob: str = "data/*.parquet",
) -> list[dict[str, object]]:
    """Build the HF dataset ``configs`` list: a default ``all`` config that globs every
    provider's parquet, plus one config per provider.

    This lets consumers pull a single provider — ``load_dataset(repo, "<provider>")`` —
    instead of the whole corpus, and gives the dataset viewer a per-provider subset
    selector. The provider list is DATA-DRIVEN (the providers actually published this run),
    so the card's configs can never drift from the data — there is no static per-provider
    list to maintain (the original gap: a Hive-partitioned dataset with no ``configs:``
    collapses to a single set). Providers are de-duplicated and sorted for a stable card.

    Each provider's parquet must be a flat ``data/<provider>.parquet`` carrying its own
    ``data_source`` column, so every config (including ``all``) exposes ``data_source``
    without relying on Hive ``key=value`` path-key recovery (HF does not apply that to
    explicitly-listed ``data_files``).
    """
    configs: list[dict[str, object]] = [
        {"config_name": all_config_name, "data_files": [{"split": "train", "path": all_glob}], "default": True}
    ]
    for provider in sorted(dict.fromkeys(providers)):
        path = path_template.format(provider=provider)
        configs.append({"config_name": provider, "data_files": [{"split": "train", "path": path}]})
    return configs


def inject_frontmatter_configs(card_text: str, configs: list[dict[str, object]]) -> str:
    """Return ``card_text`` with its YAML frontmatter ``configs`` key set to ``configs``.

    Adds (or replaces) ONLY the ``configs`` key; all other frontmatter (license, tags, …)
    and the card body are preserved. If the card has no frontmatter, one is created. Used at
    publish time to inject the data-driven per-provider configs (``build_provider_configs``)
    so the on-disk card needs no static, drift-prone provider list.

    Raises:
        ValueError: if the card opens a ``---`` fence without closing it, or its frontmatter
            is not a YAML mapping.
    """
    if card_text.startswith("---"):
        parts = card_text.split("---", 2)
        if len(parts) < 3:
            raise ValueError("Malformed card: opening '---' without a closing frontmatter fence.")
        front = yaml.safe_load(parts[1]) or {}
        if not isinstance(front, dict):
            raise ValueError("Card frontmatter is not a YAML mapping.")
        body = parts[2]
    else:
        front = {}
        body = "\n" + card_text
    front["configs"] = configs
    dumped = yaml.safe_dump(front, sort_keys=False, default_flow_style=False, allow_unicode=True)
    return f"---\n{dumped}---{body}"


def get_hf_card_path(
    name: str,
    *,
    kind: Literal["dataset", "model"] = "dataset",
) -> Path:
    """Resolve the on-disk path to a HuggingFace card markdown file.

    Dual-mode resolution so the same helper works at runtime inside both a
    wheel install (Databricks workflow task, HF Jobs PEP 723 script) and
    a source-tree checkout (local dev, test runs):

      1. **Wheel install**: the wheel force-includes ``docs/huggingface/``
         as a sibling of the ``ingestion`` package (see ``pyproject.toml``
         ``[tool.hatch.build.targets.wheel.force-include]``). Resolves via
         ``Path(ingestion.__file__).parent.parent / docs / huggingface /
         <kind>-cards / <name>``.

      2. **Source-tree fallback**: when the wheel-side candidate does not
         exist, walks up from this module to the repo root and descends
         into ``docs/huggingface/<kind>-cards/``.

    Args:
        name: Basename of the card file (e.g. ``"spadl-vaep-action-values.md"``,
            ``"psxg-model.md"``). Must match ``^[a-zA-Z0-9][a-zA-Z0-9._-]*\\.md$``.
            No path separators or traversal patterns — anything beyond a
            flat basename is rejected.
        kind: ``"dataset"`` (default) or ``"model"``. Determines which
            subdirectory under ``docs/huggingface/`` the card lives in.

    Returns:
        ``Path`` to the resolved card. Not guaranteed to exist — caller
        validates via ``upload_hf_readme``'s file-existence check.

    Raises:
        ValueError: if ``name`` fails the basename pattern or ``kind`` is
            not one of the supported values.
    """
    if kind not in _SUPPORTED_CARD_KINDS:
        raise ValueError(f"Invalid kind {kind!r}. Supported: {sorted(_SUPPORTED_CARD_KINDS)}")
    if not _CARD_NAME_RE.match(name):
        raise ValueError(
            f"Invalid card name {name!r}. Expected a basename ending in .md, no path separators or traversal patterns."
        )

    subdir = _KIND_TO_SUBDIR[kind]

    # Wheel-first: site-packages layout where docs/ is a sibling of ingestion/.
    wheel_candidate = _WHEEL_INGESTION_FILE.parent.parent / "docs" / "huggingface" / subdir / name
    if wheel_candidate.is_file():
        return wheel_candidate

    # Dev fallback: walk up from this module to repo root.
    # src/ingestion/hf_publish.py → parents[2] = repo root.
    repo_candidate = Path(__file__).resolve().parents[2] / "docs" / "huggingface" / subdir / name
    return repo_candidate


def upload_hf_readme(
    repo_id: str,
    readme_path: Path,
    hf_token: str,
    *,
    repo_type: Literal["dataset", "model", "space"] = "dataset",
    config_providers: Iterable[str] | None = None,
    config_path_template: str = "data/{provider}.parquet",
) -> dict[str, str]:
    """Upload a README.md to a HF dataset, model, or Space repo.

    Validates inputs, LF-normalizes the content, uploads via
    ``HfApi.upload_file``. Returns the commit URL and the SHA-256 of the
    uploaded bytes so callers can log / audit the push.

    Args:
        repo_id: Full HF repo id, ``"owner/name"`` shape (e.g.
            ``"luxury-lakehouse/spadl-vaep-action-values"`` for a dataset,
            ``"luxury-lakehouse/psxg-model"`` for a model,
            ``"luxury-lakehouse/README"`` for the org Space).
        readme_path: Path to the in-repo source markdown file. Resolve
            via ``get_hf_card_path`` for dataset / model cards; use
            ``Path("docs/huggingface/org-card.md")`` for the org Space.
        hf_token: HF API token. Callers pass it explicitly so secret
            handling stays at the call site.
        repo_type: ``"dataset"`` (default), ``"model"``, or ``"space"``.
        config_providers: when given (dataset repos only), inject a data-driven
            per-provider ``configs:`` block into the card's frontmatter before
            upload (via ``build_provider_configs`` + ``inject_frontmatter_configs``)
            so the viewer shows a per-provider subset selector and consumers can
            ``load_dataset(repo, "<provider>")``. ``None`` (default) uploads the
            card byte-for-byte (existing behavior). An empty iterable also skips
            injection (e.g. a sweep-only publish of an empty repo).
        config_path_template: per-provider parquet path pattern for the injected
            configs (default ``"data/{provider}.parquet"``).

    Returns:
        ``{"commit_url": <commit url>, "sha256": <hex digest of LF-normalized bytes>}``.

    Raises:
        ValueError: if the file is missing, empty or whitespace-only, the
            repo_id does not match HF's identifier pattern, or the
            repo_type is not one of the supported values.
        huggingface_hub.errors.HfHubHTTPError: propagated from the SDK on
            auth, network, or API failures. Callers do NOT catch — they
            fail loud so the operator re-runs after fixing the cause
            (ADR-002: no silent-swallow on telemetry / documentation
            delivery paths).
    """
    if repo_type not in _SUPPORTED_REPO_TYPES:
        raise ValueError(f"Invalid repo_type {repo_type!r}. Supported: {sorted(_SUPPORTED_REPO_TYPES)}")
    if not _REPO_ID_RE.match(repo_id):
        raise ValueError(f"Invalid repo_id {repo_id!r}. Expected 'owner/name' pattern.")
    if not readme_path.exists():
        raise ValueError(f"README not found: {readme_path}")

    raw = readme_path.read_bytes()
    if not raw.strip():
        raise ValueError(f"README is empty: {readme_path}")

    # Inject the data-driven per-provider `configs:` block (dataset repos only) so the
    # uploaded card matches the providers actually published — no static, drift-prone list
    # on disk. `config_providers=None` keeps the upload byte-identical (existing callers).
    providers = list(config_providers) if config_providers is not None else []
    if providers:
        if repo_type != "dataset":
            raise ValueError("config_providers is only valid for repo_type='dataset'.")
        injected = inject_frontmatter_configs(
            raw.decode("utf-8"), build_provider_configs(providers, path_template=config_path_template)
        )
        raw = injected.encode("utf-8")

    # LF-normalize: CRLF → LF, then bare CR → LF. Keep the result as bytes
    # so ``upload_file`` treats it as a file payload (avoids the
    # str-vs-path ambiguity on its ``path_or_fileobj`` parameter).
    normalized = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    sha256 = hashlib.sha256(normalized).hexdigest()

    api = HfApi(token=hf_token)
    commit_info = api.upload_file(
        path_or_fileobj=normalized,
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type=repo_type,
        token=hf_token,
        commit_message=f"Update README.md (generated from {readme_path.name})",
    )

    logger.info(
        "Uploaded README for %s (repo_type=%s, bytes=%d, sha256=%s)",
        repo_id,
        repo_type,
        len(normalized),
        sha256[:8],
    )
    return {"commit_url": str(commit_info), "sha256": sha256}
