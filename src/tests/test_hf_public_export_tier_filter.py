"""Row-level exports to PUBLIC HF datasets must be public-only at the source (SEC6 / ADR-064).

`publish_*_hf.py` is a NAMING CONVENTION, not a boundary. The ADR-072 AST gates glob that
pattern, so a module that publishes row-level data under any other name is invisible to
them. Three such modules exist, all writing to public HF datasets via
``upload_volume_to_hf_hub`` rather than the pandas seam:

    export_embeddings_training_data  -> luxury-lakehouse/football2vec-training-data
    export_scoutgpt_training_data    -> luxury-lakehouse/scoutgpt-training-data
    prepare_360_training_data        -> football2vec-360 training data

None was in ``PUBLISHER_REGISTRY``; none carried an ``access_tier`` filter. Their source is
``fct_action_values``, which carries restricted providers (measured 2026-08-08: skillcorner
122,983 + gradientsports 88,958 restricted rows). ``prepare_360_training_data`` was safe by
construction — it filters ``data_source = 'statsbomb'`` — but that is a property nothing
asserted, so it could have been relaxed without a failing test.

ADR-064 states the football2vec chain is "rebuilt public-only UPSTREAM", and
``publish_football2vec_embeddings_hf`` (registry mode ``derived``) asserts exactly that:
"the materialized source had ZERO access_tier != 'public' rows". These exporters ARE that
upstream. This module is what makes the claim true rather than merely documented.

The check is textual on the SQL because these are Spark-path exporters: they never
materialise a pandas frame the seam could guard, which is why the seam's AST gates cannot
see them (TODO SEC6's "Spark-path problem").
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from shared.access_tier import PUBLIC_BY_LICENSE_PROVIDERS

_REPO = Path(__file__).resolve().parents[2]
_INGESTION = _REPO / "src" / "ingestion"
_SCRIPTS = _REPO / "scripts"

# Modules that export ROW-LEVEL data from a gold mart to a PUBLIC HF dataset without
# passing through the pandas seam. Every one must prove public-only at the SOURCE.
_PUBLIC_ROW_EXPORTERS = (
    "export_embeddings_training_data",
    "export_scoutgpt_training_data",
    "prepare_360_training_data",
)

# Uploads row-level data to HF but is NOT one of the above — each needs a reason.
_NOT_ROW_LEVEL = {
    "export_shots_on_target": "asserts access_tier == 'public' and DROPS the column before upload (#509/#513)",
    "hf_publish": "the helper itself — uploads READMEs and model weights, never a mart",
    "hf_upload_seam": "the seam itself — this IS the boundary",
    "utils": "hosts upload_volume_to_hf_hub; not a publisher",
    "hf_jobs_cost": "cost telemetry, no mart rows",
    "publish_freeze_frame_hf": "pandas seam + PUBLISHER_REGISTRY",
    "publish_spadl_vaep_hf": "pandas seam + PUBLISHER_REGISTRY (splits to the restricted repo)",
    "publish_xg_shots_hf": "pandas seam + PUBLISHER_REGISTRY",
}

# scripts/ HF writers, classified by WHAT THEY PUBLISH — not by filename. `publish_*_hf.py`
# being a convention rather than a boundary is the whole point of SEC6, so this bucket must
# cover the writers that do NOT match that glob just as explicitly as the ones that do.
_SCRIPTS_WRITERS = {
    # Row-level datasets — already under the ADR-072 pandas seam + PUBLISHER_REGISTRY.
    "publish_action_context_hf": "seam",
    "publish_football2vec_embeddings_hf": "seam",
    "publish_freeze_frame_hf": "seam",
    "publish_line_breaking_passes_hf": "seam",
    "publish_obso_pausa_inputs_hf": "seam",
    "publish_pitch_control_tracking_hf": "seam",
    "publish_psxg_shots_hf": "seam",
    "publish_shot_freeze_frames_hf": "seam",
    "publish_shots_on_target_hf": "seam",
    "publish_spadl_vaep_hf": "seam",
    "publish_xg_shot_data_v3_hf": "seam",
    "publish_xg_shots_hf": "seam",
    # Model weights — no mart rows, so no access_tier decision to make.
    "evaluate_football2vec_l2_adversary_seeds": "weights",
    "evaluate_scoutgpt_l2_seeds": "weights",
    "train_football2vec": "weights",
    "train_football2vec_360": "weights",
    "train_football2vec_v2": "weights",
    "train_psxg_hf": "weights",
    "train_scoutgpt_hf": "weights",
    "train_vaep_model_hf": "weights",
    "train_xg_v3_hf": "weights",
    # Derived grids/aggregates, not per-entity rows.
    "compute_epv_transition_hf": "aggregate",
    "compute_obso_hf": "aggregate",
    "compute_space_creation_hf": "aggregate",
    "compute_xt_grid_hf": "aggregate",
    "export_embedding_atlas_data": "aggregate",
    # Infrastructure.
    "manage_space": "space",
}

_TIER_PUBLIC = re.compile(r"access_tier\s*=\s*'public'")
_UPLOADS = re.compile(r"upload_volume_to_hf_hub|upload_guarded|upload_folder")


def _source(module: str) -> str:
    return (_INGESTION / f"{module}.py").read_text(encoding="utf-8")


@pytest.mark.parametrize("module", _PUBLIC_ROW_EXPORTERS)
def test_public_row_exporter_is_public_only_at_source(module: str) -> None:
    """Each exporter must filter to public rows, by tier or by public-by-licence provider.

    Two acceptable proofs:
      - ``access_tier = 'public'`` — the general form, valid for any provider mix;
      - a ``data_source`` filter naming ONLY providers in PUBLIC_BY_LICENSE_PROVIDERS —
        narrower, but equally sound (prepare_360 is statsbomb-only by design).
    """
    src = _source(module)
    if _TIER_PUBLIC.search(src):
        return

    named = set(re.findall(r"data_source\s*=\s*'([a-z_]+)'", src))
    assert named, (
        f"{module} exports row-level data to a PUBLIC HF dataset but filters neither on "
        "access_tier nor on data_source. Its source mart carries restricted providers "
        "(skillcorner, gradientsports), so this would publish them. Add "
        "\"AND av.access_tier = 'public'\" to the query (ADR-064 / SEC6)."
    )
    leaked = named - set(PUBLIC_BY_LICENSE_PROVIDERS)
    assert not leaked, (
        f"{module} filters data_source to {sorted(named)}, which includes non-public-by-licence "
        f"provider(s) {sorted(leaked)}. Use an explicit access_tier = 'public' filter instead."
    )


def test_every_hf_row_uploader_is_classified() -> None:
    """Enumerate-all, fail-closed: a NEW module uploading to HF must be classified here.

    This is the half that makes the gate a boundary rather than a list. `export_shots_on_target`
    leaked the internal `access_tier` column for one commit precisely because no discovery step
    forced a decision about it — it was caught by hand, not by a test.
    """
    unclassified: list[str] = []
    for path in sorted(_INGESTION.glob("*.py")):
        src = path.read_text(encoding="utf-8")
        if not _UPLOADS.search(src):
            continue
        name = path.stem
        if name in _PUBLIC_ROW_EXPORTERS or name in _NOT_ROW_LEVEL:
            continue
        unclassified.append(name)

    assert not unclassified, (
        "module(s) upload to HF Hub but are classified in neither _PUBLIC_ROW_EXPORTERS nor "
        f"_NOT_ROW_LEVEL: {unclassified}. Decide which it is — if it publishes mart rows to a "
        "public repo it must prove public-only at the source; if not, record WHY in _NOT_ROW_LEVEL."
    )


def test_classification_is_not_vacuous() -> None:
    """Both buckets must stay non-empty and cover the modules that motivated this gate."""
    assert len(_PUBLIC_ROW_EXPORTERS) >= 3
    assert "export_shots_on_target" in _NOT_ROW_LEVEL
    for module in _PUBLIC_ROW_EXPORTERS:
        assert (_INGESTION / f"{module}.py").exists(), f"{module} listed but does not exist"


def test_every_scripts_hf_writer_is_classified() -> None:
    """Enumerate-all, fail-closed for scripts/ — the OTHER half of the boundary.

    The ADR-072 AST gates glob ``publish_*_hf.py``. Under scripts/ that misses 15 writers
    (7 trainers, 4 compute jobs, 2 evaluators, an atlas export and Space management), which
    is the same filename-convention hole SEC6 names, one directory over. A new writer here
    must be classified before it can ship.
    """
    unclassified = [
        p.stem
        for p in sorted(_SCRIPTS.glob("*.py"))
        if _UPLOADS.search(p.read_text(encoding="utf-8", errors="replace")) and p.stem not in _SCRIPTS_WRITERS
    ]
    assert not unclassified, (
        f"scripts/ module(s) upload to HF Hub but are unclassified: {unclassified}. "
        "Classify as 'seam' (row-level, must use prepare_public_upload + PUBLISHER_REGISTRY), "
        "'weights', 'aggregate' or 'space' in _SCRIPTS_WRITERS — filename is not a boundary."
    )


def test_scripts_seam_writers_are_registered_publishers() -> None:
    """Anything classified 'seam' must actually be in PUBLISHER_REGISTRY.

    Stops the classification itself becoming decorative: labelling a module 'seam' without
    registering it would assert a guarantee nothing enforces.
    """
    from ingestion.hf_leak_guard import PUBLISHER_REGISTRY

    missing = [n for n, kind in _SCRIPTS_WRITERS.items() if kind == "seam" and n not in PUBLISHER_REGISTRY]
    assert not missing, f"classified 'seam' but absent from PUBLISHER_REGISTRY: {missing}"
