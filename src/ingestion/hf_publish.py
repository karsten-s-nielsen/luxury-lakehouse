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
from pathlib import Path
from typing import Literal

from huggingface_hub import HfApi

import ingestion as _ingestion

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
