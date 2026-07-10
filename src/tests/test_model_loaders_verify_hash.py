"""SEC2: assert that model loader modules invoke verify_artifact_hash().

These are wiring tests — they assert that each of the 4 loader modules
imports the verification helpers and calls ``verify_artifact_hash(`` at
least once. Behavioral coverage of the helper itself is in
``test_verify_artifact_hash.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTION = REPO_ROOT / "src" / "ingestion"


def _read(path: str) -> str:
    return (INGESTION / path).read_text(encoding="utf-8")


LOADER_MODULES = [
    # xg_model.py (v1) retired SK3-MIG-B 2026-05-03 per ADR-023; xg_model_v2.py retired
    # 2026-07-10 with the v2 producer chain (ADR-066). The canonical pre-shot xG loader is
    # now xg_shot_scorer.py (xg_model_v3), which carries the same verify_artifact_hash sites.
    ("xg_shot_scorer.py", "xG v3 pre-shot scorer loader"),
    ("spadl_vaep.py", "VAEP scores/concedes loader"),
    ("defcon_lite_common.py", "DEFCON regressor loader"),
]


@pytest.mark.parametrize(("module_name", "description"), LOADER_MODULES)
def test_loader_imports_verify_artifact_hash(module_name: str, description: str) -> None:
    """Each of the 4 model loaders must import verify_artifact_hash from ingestion.utils."""
    src = _read(module_name)
    assert "verify_artifact_hash" in src, (
        f"{description} ({module_name}) must import verify_artifact_hash from ingestion.utils. "
        f"Defense-in-depth SEC2 / SEC-AUDIT ML-02."
    )


@pytest.mark.parametrize(("module_name", "description"), LOADER_MODULES)
def test_loader_calls_verify_artifact_hash(module_name: str, description: str) -> None:
    """Each of the 4 model loaders must call verify_artifact_hash( at least once.

    Regression guard: catches a future refactor that removes the verification
    call while leaving the import in place.
    """
    src = _read(module_name)
    # Count call-sites (not the import line)
    call_count = src.count("verify_artifact_hash(")
    # Import form is `verify_artifact_hash,` or `verify_artifact_hash)` — excluded from count
    assert call_count >= 1, (
        f"{description} ({module_name}) imports verify_artifact_hash but does not call it. "
        f"The import must be backed by at least one call site."
    )


@pytest.mark.parametrize(
    ("module_name", "expected_min_call_sites"),
    [
        # xg_model.py (v1) retired SK3-MIG-B 2026-05-03; xg_model_v2.py retired 2026-07-10.
        # xg_shot_scorer.py (v3): MLflow Champion (1) + UC Volume fallback (1) = 2.
        ("xg_shot_scorer.py", 2),
        # spadl_vaep.py: scores + concedes = 2 calls
        ("spadl_vaep.py", 2),
        # defcon_lite_common.py: single regressor = 1 call
        ("defcon_lite_common.py", 1),
    ],
)
def test_loader_has_expected_number_of_verification_sites(module_name: str, expected_min_call_sites: int) -> None:
    """Each loader must call verify_artifact_hash for every distinct artifact it loads.

    Regression guard against partial wiring (e.g., MLflow path verified but
    UC Volume fallback path forgotten).
    """
    src = _read(module_name)
    call_count = src.count("verify_artifact_hash(")
    assert call_count >= expected_min_call_sites, (
        f"{module_name} has {call_count} verify_artifact_hash() call sites, "
        f"expected at least {expected_min_call_sites} (one per distinct artifact bytes loaded)."
    )
