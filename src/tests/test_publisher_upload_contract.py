"""Hermetic integration tests for the ADR-072 publish path — the staged tree and upload contract.

These sit between the seam's unit tests (``test_hf_upload_seam.py``, which prove the seam's own
logic) and a live publish (which needs Databricks + HF credentials and is therefore never run in
CI). They drive each publisher's REAL publish function with a fake ``HfApi``, then assert what
actually reached the wire: repo id, ``path_in_repo``, ``delete_patterns``, repo privacy, and the
exact staged file tree.

Why this file exists. The seam migration changed five publishers' ``delete_patterns`` from
``["data/*"]`` — which is matched RELATIVE to ``path_in_repo`` and therefore silently matched
NOTHING — to the mandated ``["**"]``. That converts an inert call into one that genuinely deletes
repo content on the next run. The only pre-existing coverage,
``test_hf_publish_parity.test_publisher_delete_patterns_sweep_whole_path_in_repo``, is parametrized
over the six ADR-049 split publishers, so four of the five changed files had **zero** tests. A
destructive behaviour change reached a green suite untested. These tests close that.

Still NOT covered here, and not coverable without credentials: that the SQL these publishers run
returns the columns assumed, and that HF accepts the resulting upload. Those need a live run.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import pytest

from ingestion.hf_upload_seam import prepare_public_upload

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"

_FAKE_TOKEN = "t"  # noqa: S105 — test double handed to the fake HfApi, not a credential


@pytest.fixture(autouse=True)
def _ensure_publisher_importable() -> None:
    """Add scripts/ to sys.path so the PEP-723 publisher modules import as plain modules."""
    scripts_str = str(_SCRIPTS_DIR)
    if scripts_str not in sys.path:
        sys.path.insert(0, scripts_str)


class _FakeApi:
    """Records every create_repo / upload_folder call the seam makes."""

    instances: ClassVar[list[_FakeApi]] = []

    def __init__(self, token: str | None = None) -> None:
        self.token = token
        self.created: list[tuple[str, bool]] = []
        self.uploads: list[dict[str, Any]] = []
        _FakeApi.instances.append(self)

    def create_repo(self, repo_id: str, **kw: Any) -> None:
        self.created.append((repo_id, bool(kw.get("private", False))))

    def upload_folder(self, **kw: Any) -> None:
        # Snapshot the staged tree BEFORE the TemporaryDirectory is torn down.
        folder = Path(str(kw["folder_path"]))
        kw = dict(kw)
        kw["_staged"] = sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file())
        self.uploads.append(kw)


@pytest.fixture()
def fake_api(monkeypatch: pytest.MonkeyPatch) -> type[_FakeApi]:
    _FakeApi.instances = []
    monkeypatch.setattr("ingestion.hf_upload_seam.HfApi", _FakeApi)
    return _FakeApi


def _uploads(fake: type[_FakeApi]) -> list[dict[str, Any]]:
    return [u for inst in fake.instances for u in inst.uploads]


def _created(fake: type[_FakeApi]) -> list[tuple[str, bool]]:
    return [c for inst in fake.instances for c in inst.created]


# ---------------------------------------------------------------------------
# Every upload the seam performs must sweep correctly and target path_in_repo="data".
# ---------------------------------------------------------------------------


def test_partitioned_publisher_sweeps_whole_path_in_repo(fake_api: type[_FakeApi]) -> None:
    """freeze_frame shape: Hive-partitioned by competition_id, sweeping ["**"].

    Regression for the ``["data/*"]`` no-op: patterns are matched RELATIVE to path_in_repo, so a
    "data/"-prefixed pattern reaches nothing. ``["**"]`` is the only correct whole-path sweep.
    """
    import publish_freeze_frame_hf as pub

    df = pd.DataFrame(
        {
            "access_tier": ["public"] * 3,
            "competition_id": [11, 11, 43],
            "event_id": ["e1", "e2", "e3"],
            "player_x_norm": [0.1, 0.2, 0.3],
        }
    )
    guarded = prepare_public_upload(df, publisher="publish_freeze_frame_hf").public
    pub.publish_to_hf_hub(guarded, _FAKE_TOKEN)

    (upload,) = _uploads(fake_api)
    assert upload["path_in_repo"] == "data"
    assert upload["delete_patterns"] == ["**"], (
        "a 'data/'-prefixed pattern is matched relative to path_in_repo and silently no-ops (CLAUDE.md)"
    )
    assert upload["_staged"] == ["competition_id=11/data.parquet", "competition_id=43/data.parquet"]
    assert _created(fake_api) == [(pub.DATASET_REPO, False)], "public frames must not create a private repo"


def test_freeze_frame_twins_stage_and_sweep_identically(fake_api: type[_FakeApi]) -> None:
    """Both freeze_frame twins publish the SAME repo — divergent behaviour would make the outcome
    depend on run order.

    The ``src/ingestion`` twin previously passed ``["data/*"]`` (a no-op) while the ``scripts`` twin
    passed no ``delete_patterns`` at all, so neither ever swept and the divergence was invisible.
    Now both sweep ``["**"]``. This is the StatsBomb 360 freeze-frame publisher and it had no
    dedicated test module before ADR-072.
    """
    import publish_freeze_frame_hf as scripts_pub

    from ingestion import publish_freeze_frame_hf as src_pub

    def _df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "access_tier": ["public"] * 3,
                "competition_id": [11, 11, 43],
                "event_id": ["e1", "e2", "e3"],
                "is_keeper": [True, False, False],
            }
        )

    for module in (scripts_pub, src_pub):
        module.publish_to_hf_hub(prepare_public_upload(_df(), publisher="publish_freeze_frame_hf").public, _FAKE_TOKEN)

    scripts_up, src_up = _uploads(fake_api)
    assert scripts_up["repo_id"] == src_up["repo_id"], "twins must target the same repo"
    assert scripts_up["path_in_repo"] == src_up["path_in_repo"] == "data"
    assert scripts_up["delete_patterns"] == src_up["delete_patterns"] == ["**"]
    assert (
        scripts_up["_staged"]
        == src_up["_staged"]
        == [
            "competition_id=11/data.parquet",
            "competition_id=43/data.parquet",
        ]
    )


def test_sb360_shot_freeze_frames_splits_and_sweeps(fake_api: type[_FakeApi]) -> None:
    """The StatsBomb-360 shot-freeze-frame publisher: flat per-provider, both tiers, ["**"] sweep."""
    import publish_shot_freeze_frames_hf as pub

    df = pd.DataFrame(
        {
            "access_tier": ["public", "restricted"],
            "data_source": ["statsbomb", "skillcorner"],
            "shot_id": [1, 2],
        }
    )
    prepared = prepare_public_upload(df, publisher="publish_shot_freeze_frames_hf")
    assert prepared.restricted is not None
    pub.publish_to_hf_hub(prepared.public, _FAKE_TOKEN)
    pub.publish_to_hf_hub(prepared.restricted, _FAKE_TOKEN, repo_id=pub.RESTRICTED_DATASET_REPO)

    public_up, restricted_up = _uploads(fake_api)
    assert public_up["_staged"] == ["statsbomb.parquet"]
    assert restricted_up["_staged"] == ["skillcorner.parquet"]
    assert public_up["delete_patterns"] == restricted_up["delete_patterns"] == ["**"]
    assert _created(fake_api) == [(pub.DATASET_REPO, False), (pub.RESTRICTED_DATASET_REPO, True)]


def test_flat_per_provider_publisher_stages_one_file_per_provider(fake_api: type[_FakeApi]) -> None:
    """psxg shape: flat data/<provider>.parquet, KEEPING data_source for the ADR-054 configs."""
    import publish_psxg_shots_hf as pub

    df = pd.DataFrame(
        {
            "access_tier": ["public", "public", "restricted"],
            "data_source": ["statsbomb", "skillcorner", "skillcorner"],
            "shot_id": [1, 2, 3],
        }
    )
    prepared = prepare_public_upload(df, publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    pub.publish_to_hf_hub(prepared.public, _FAKE_TOKEN)
    pub.publish_to_hf_hub(prepared.restricted, _FAKE_TOKEN, repo_id=pub.RESTRICTED_DATASET_REPO)

    public_up, restricted_up = _uploads(fake_api)
    assert public_up["_staged"] == ["skillcorner.parquet", "statsbomb.parquet"]
    assert restricted_up["_staged"] == ["skillcorner.parquet"]
    assert public_up["delete_patterns"] == ["**"] and restricted_up["delete_patterns"] == ["**"]
    # Repo privacy is DERIVED from the frame tier — no caller flag to forget.
    assert _created(fake_api) == [(pub.DATASET_REPO, False), (pub.RESTRICTED_DATASET_REPO, True)]


def test_multi_frame_publisher_shares_one_receipt_and_scopes_deletes(fake_api: type[_FakeApi]) -> None:
    """football2vec shape: three frames, one receipt, per-subdir delete scoping.

    The scoped patterns matter: a fail-closed per-match-only publish must not wipe a
    previously-published career/season table.
    """
    import publish_football2vec_embeddings_hf as pub

    def _pm(ids: list[str]) -> pd.DataFrame:
        return pd.DataFrame(
            {"canonical_player_id": ids, "match_id": ["m1"] * len(ids), "access_tier": ["public"] * len(ids)}
        )

    def _agg(ids: list[str]) -> pd.DataFrame:
        return pd.DataFrame({"canonical_player_id": ids, "access_tier": ["public"] * len(ids)})

    tables, withheld = pub.select_publishable_tables(_pm(["p1", "p2"]), _agg(["p1"]), _agg(["p2"]))
    assert withheld is None
    pub.publish_to_hf_hub(tables, _FAKE_TOKEN)

    (upload,) = _uploads(fake_api)
    assert upload["_staged"] == ["career/data.parquet", "per_match/data.parquet", "season/data.parquet"]
    assert sorted(upload["delete_patterns"]) == ["career/**", "per_match/**", "season/**"]


def test_single_file_publisher_preserves_its_repo_path(fake_api: type[_FakeApi]) -> None:
    """shots_on_target shape: the object stays at data/shots_on_target.parquet.

    This publisher moved from ``api.upload_file(path_in_repo="data/shots_on_target.parquet")`` to a
    folder upload. The resulting repo path must be byte-identical or every consumer using a direct
    file URL breaks (Hyrum's Law).
    """
    import publish_shots_on_target_hf as pub

    df = pd.DataFrame({"access_tier": ["public", "public"], "event_id": ["e1", "e2"], "is_goal": [1, 0]})
    guarded = prepare_public_upload(df, publisher="publish_shots_on_target_hf").public
    pub.publish_to_hf_hub(guarded, _FAKE_TOKEN)

    (upload,) = _uploads(fake_api)
    assert upload["path_in_repo"] == "data"
    assert upload["delete_patterns"] == ["**"]
    assert upload["_staged"] == ["shots_on_target.parquet"], (
        "repo path must remain data/shots_on_target.parquet — a move breaks direct file URLs"
    )


# ---------------------------------------------------------------------------
# The seam's refusals reach the publishers, not just the unit tests.
# ---------------------------------------------------------------------------


def test_restricted_frame_cannot_reach_a_public_repo(fake_api: type[_FakeApi]) -> None:
    """The tier assertion fires through a real publisher's call path, before any repo is created."""
    import publish_psxg_shots_hf as pub

    from ingestion.hf_upload_seam import TierMismatchError

    df = pd.DataFrame({"access_tier": ["restricted"], "data_source": ["gradientsports"], "shot_id": [1]})
    prepared = prepare_public_upload(df, publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    with pytest.raises(TierMismatchError):
        pub.publish_to_hf_hub(prepared.restricted, _FAKE_TOKEN)  # public repo — must refuse
    assert _created(fake_api) == [], "no repo may be created when the tier check fails"
    assert _uploads(fake_api) == []


def test_empty_restricted_frame_still_sweeps(fake_api: type[_FakeApi]) -> None:
    """ADR-049 sweep-only publish: zero partitions is healthy and must still clear the private repo."""
    import publish_psxg_shots_hf as pub

    df = pd.DataFrame({"access_tier": ["public"], "data_source": ["statsbomb"], "shot_id": [1]})
    prepared = prepare_public_upload(df, publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None and prepared.restricted.frame.empty
    pub.publish_to_hf_hub(prepared.restricted, _FAKE_TOKEN, repo_id=pub.RESTRICTED_DATASET_REPO)

    (upload,) = _uploads(fake_api)
    assert upload["_staged"] == []
    assert upload["delete_patterns"] == ["**"]
    assert _created(fake_api) == [(pub.RESTRICTED_DATASET_REPO, True)]


# ---------------------------------------------------------------------------
# EVERY publisher, table-driven. Complete by construction: the ids below are
# cross-checked against the 15 files on disk, so a new publisher cannot escape.
# ---------------------------------------------------------------------------

# (module_id, import_path, partition_column | None, expected staged basenames for a 2-provider frame)
# partition_column None => flat single file; "hive:<col>" => <col>=<v>/data.parquet; "<col>" => <v>.parquet
_ALL_PUBLISHERS: list[tuple[str, str, str | None, list[str]]] = [
    ("publish_action_context_hf", "scripts", "data_source", ["a.parquet", "b.parquet"]),
    (
        "publish_line_breaking_passes_hf",
        "scripts",
        "hive:data_source",
        ["data_source=a/data.parquet", "data_source=b/data.parquet"],
    ),
    (
        "publish_obso_pausa_inputs_hf",
        "scripts",
        "hive:match_id",
        ["match_id=a/data.parquet", "match_id=b/data.parquet"],
    ),
    (
        "publish_pitch_control_tracking_hf",
        "scripts",
        "hive:source_provider",
        ["source_provider=a/data.parquet", "source_provider=b/data.parquet"],
    ),
    ("publish_psxg_shots_hf", "scripts", "data_source", ["a.parquet", "b.parquet"]),
    ("publish_shot_freeze_frames_hf", "scripts", "data_source", ["a.parquet", "b.parquet"]),
    (
        "publish_spadl_vaep_hf",
        "scripts",
        "hive:data_source",
        ["data_source=a/data.parquet", "data_source=b/data.parquet"],
    ),
    ("publish_xg_shot_data_v3_hf", "scripts", "data_source", ["a.parquet", "b.parquet"]),
    (
        "publish_xg_shots_hf",
        "scripts",
        "hive:data_source",
        ["data_source=a/data.parquet", "data_source=b/data.parquet"],
    ),
    (
        "publish_xg_shots_hf",
        "ingestion",
        "hive:data_source",
        ["data_source=a/data.parquet", "data_source=b/data.parquet"],
    ),
    (
        "publish_freeze_frame_hf",
        "scripts",
        "hive:competition_id",
        ["competition_id=a/data.parquet", "competition_id=b/data.parquet"],
    ),
    (
        "publish_freeze_frame_hf",
        "ingestion",
        "hive:competition_id",
        ["competition_id=a/data.parquet", "competition_id=b/data.parquet"],
    ),
    ("publish_shots_on_target_hf", "scripts", None, ["shots_on_target.parquet"]),
]

# Two publishers do not take a single GuardedFrame and are covered by their own tests above /
# below: football2vec (dict[str, GuardedFrame]) and the spadl_vaep src twin (_publish_partitioned,
# which takes repo_id + a logger). Named here so the completeness check can account for them.
_NON_UNIFORM = {
    ("publish_football2vec_embeddings_hf", "scripts"),
    ("publish_spadl_vaep_hf", "ingestion"),
}


def _import(module_id: str, where: str) -> Any:
    if where == "scripts":
        return __import__(module_id)
    return __import__(f"ingestion.{module_id}", fromlist=[module_id])


def test_every_publisher_file_is_covered_by_this_module() -> None:
    """Complete by construction — a new publisher cannot silently escape the contract tests."""
    repo_root = Path(__file__).resolve().parents[2]
    on_disk = {(p.stem, "scripts") for p in (repo_root / "scripts").glob("publish_*_hf.py")} | {
        (p.stem, "ingestion") for p in (repo_root / "src" / "ingestion").glob("publish_*_hf.py")
    }
    covered = {(m, w) for m, w, _, _ in _ALL_PUBLISHERS} | _NON_UNIFORM
    assert on_disk == covered, (
        f"publisher files not covered by an upload-contract test: {sorted(on_disk - covered)}; "
        f"table entries with no file on disk: {sorted(covered - on_disk)}"
    )


@pytest.mark.parametrize(
    ("module_id", "where", "partition", "expected"),
    _ALL_PUBLISHERS,
    ids=[f"{w}/{m}" for m, w, _, _ in _ALL_PUBLISHERS],
)
def test_publisher_upload_contract(
    module_id: str, where: str, partition: str | None, expected: list[str], fake_api: type[_FakeApi]
) -> None:
    """Every publisher: path_in_repo="data", a correct sweep, and the expected staged tree."""
    pub = _import(module_id, where)
    col = partition.removeprefix("hive:") if partition else None
    frame = {"access_tier": ["public", "public"], "value": [1, 2]}
    if col:
        frame[col] = ["a", "b"]
    guarded = prepare_public_upload(pd.DataFrame(frame), publisher=module_id).public
    pub.publish_to_hf_hub(guarded, _FAKE_TOKEN)

    (upload,) = _uploads(fake_api)
    assert upload["path_in_repo"] == "data", f"{where}/{module_id} must publish under data/"
    assert upload["_staged"] == expected, f"{where}/{module_id} staged tree changed"
    # A "data/"-prefixed pattern is matched RELATIVE to path_in_repo and silently no-ops (CLAUDE.md).
    patterns = upload["delete_patterns"]
    assert patterns is None or patterns == ["**"], (
        f"{where}/{module_id} delete_patterns={patterns!r} — must be ['**'] or absent, never 'data/'-prefixed"
    )
    assert _created(fake_api) == [(upload["repo_id"], False)], "a public frame must not create a private repo"
