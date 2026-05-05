"""Evolve dataset SHA pin-drift sentinel (env-gated).

Asserts pinned SHAs in evolve consumer scripts are within _MAX_AGE_DAYS
of HF Hub HEAD. Env-gated: skips without HF_TOKEN.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"

EVOLVE_SCRIPTS: list[tuple[str, Path]] = [
    ("eval_f2v_l2", _REPO_ROOT / "scripts" / "evaluate_football2vec_l2_adversary_seeds.py"),
    ("eval_scoutgpt_l2", _REPO_ROOT / "scripts" / "evaluate_scoutgpt_l2_seeds.py"),
    ("f2v_evaluator", _REPO_ROOT / "src" / "evolve" / "targets" / "football2vec" / "evaluator.py"),
    # remote_ssh.py EXCLUDED — dispatcher, not a dataset consumer (C1 review)
]

_MAX_AGE_DAYS = 90


def _load_module(name: str, path: Path) -> ModuleType:
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    scripts_str = str(_SCRIPTS_DIR)
    added = scripts_str not in sys.path
    if added:
        sys.path.insert(0, scripts_str)
    try:
        spec.loader.exec_module(mod)
    finally:
        if added and scripts_str in sys.path:
            sys.path.remove(scripts_str)
    return mod


@pytest.mark.skipif(not os.environ.get("HF_TOKEN"), reason="HF Hub access required")
@pytest.mark.parametrize("name,path", EVOLVE_SCRIPTS, ids=[s[0] for s in EVOLVE_SCRIPTS])
def test_pinned_sha_within_max_age(name: str, path: Path) -> None:
    mod = _load_module(f"_evolve_pin_{name}", path)
    repo = getattr(mod, "PINNED_DATASET_REPO", None)
    sha = getattr(mod, "PINNED_DATASET_SHA", None)
    assert repo is not None, f"{path.name} missing PINNED_DATASET_REPO"
    assert sha is not None, f"{path.name} missing PINNED_DATASET_SHA"
    if sha == "PLACEHOLDER_UNTIL_PHASE_9":
        pytest.skip("SHA not yet set — awaiting Phase 9 operator bump")
    from huggingface_hub import HfApi

    api = HfApi()
    info = api.dataset_info(repo_id=repo)
    if sha != info.sha and info.last_modified is not None:
        age = (datetime.now(timezone.utc) - info.last_modified).days
        assert age < _MAX_AGE_DAYS, (
            f"{path.name} pinned SHA is {age}d behind HEAD ({repo}). "
            f"Bump via: uv run python scripts/bump_evolve_pin.py {path}"
        )
