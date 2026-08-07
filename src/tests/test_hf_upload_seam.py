"""Unit tests for the ADR-072 publish seam.

Hermetic: ``HfApi`` is monkeypatched, no network, no credentials.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd
import pytest

from ingestion.hf_leak_guard import LeakDetectedError
from ingestion.hf_upload_seam import (
    GuardedFrame,
    TierMismatchError,
    UnauthorizedFrameError,
    UnguardedFileError,
    UploadReceipt,
    prepare_public_upload,
    upload_guarded,
)

# A test double handed to the monkeypatched HfApi — never a real credential. Declared once so the
# nine call sites below do not each need an S106 suppression.
_FAKE_TOKEN = "t"  # noqa: S105 — test double, not a credential


def _guarded(df: pd.DataFrame, publisher: str = "publish_action_context_hf") -> GuardedFrame:
    """Build a GuardedFrame the way the seam does — authorizing the frame on the receipt."""
    receipt = UploadReceipt(publisher)
    receipt._authorize(df)  # test stands in for prepare_public_upload
    return GuardedFrame(frame=df, tier="public", publisher=publisher, receipt=receipt)


def _tiered(tiers: list[str | None]) -> pd.DataFrame:
    return pd.DataFrame({"access_tier": tiers, "v": list(range(len(tiers)))})


# ---------------------------------------------------------------------------
# GuardedFrame / UploadReceipt
# ---------------------------------------------------------------------------


def test_write_parquet_records_the_path_on_the_receipt(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"a": [1, 2]}))
    out = tmp_path / "data" / "x.parquet"
    g.write_parquet(out)
    assert out.exists()
    assert g.receipt.paths == frozenset({out.resolve()})


def test_groupby_children_share_the_parent_receipt(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"k": ["a", "b"], "v": [1, 2]}))
    for key, child in g.groupby("k"):
        child.write_parquet(tmp_path / f"{key}.parquet")
    assert len(g.receipt.paths) == 2


def test_drop_columns_preserves_the_receipt_and_tier(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"k": ["a"], "v": [1]}))
    child = g.drop_columns(["k"])
    assert child.receipt is g.receipt
    assert child.tier == "public"
    assert list(child.frame.columns) == ["v"]
    child.write_parquet(tmp_path / "c.parquet")  # derived frames are authorized


def test_guarded_frames_are_usable_in_sets_and_comparisons() -> None:
    # frozen+eq would synthesise __eq__/__hash__ over a DataFrame field; both raise at runtime.
    g = _guarded(pd.DataFrame({"a": [1]}))
    assert g in {g}
    assert g == g


def test_directly_constructed_guarded_frame_refuses_to_write(tmp_path: Path) -> None:
    # Forgery route 1: the public constructor. Borrowing a real receipt does not help — the
    # authorization is on the FRAME object, which the seam never produced.
    real = _guarded(pd.DataFrame({"a": [1]}))
    forged = GuardedFrame(
        frame=pd.DataFrame({"a": [999]}), tier="public", publisher=real.publisher, receipt=real.receipt
    )
    with pytest.raises(UnauthorizedFrameError):
        forged.write_parquet(tmp_path / "forged.parquet")


def test_replace_substituted_frame_refuses_to_write(tmp_path: Path) -> None:
    # Forgery route 2: dataclasses.replace. frozen=True does not block it, and a constructor
    # sentinel would not either — replace carries unreplaced fields straight through.
    g = _guarded(pd.DataFrame({"a": [1]}))
    forged = dataclasses.replace(g, frame=pd.DataFrame({"a": [999]}))
    with pytest.raises(UnauthorizedFrameError):
        forged.write_parquet(tmp_path / "forged.parquet")


# ---------------------------------------------------------------------------
# prepare_public_upload
# ---------------------------------------------------------------------------


def test_split_mode_returns_both_sides_without_access_tier() -> None:
    prepared = prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_psxg_shots_hf")
    assert len(prepared.public.frame) == 1
    assert prepared.restricted is not None and len(prepared.restricted.frame) == 1
    assert prepared.public.tier == "public" and prepared.restricted.tier == "restricted"
    assert "access_tier" not in prepared.public.frame.columns
    assert "access_tier" not in prepared.restricted.frame.columns


def test_split_mode_routes_null_tier_to_restricted() -> None:
    prepared = prepare_public_upload(_tiered(["public", None]), publisher="publish_psxg_shots_hf")
    assert len(prepared.public.frame) == 1
    assert prepared.restricted is not None and len(prepared.restricted.frame) == 1


def test_fail_closed_mode_has_no_restricted_side() -> None:
    prepared = prepare_public_upload(_tiered(["public", "public"]), publisher="publish_xg_shots_hf")
    assert prepared.restricted is None
    assert len(prepared.public.frame) == 2


def test_fail_closed_mode_raises_on_a_restricted_row() -> None:
    with pytest.raises(LeakDetectedError):
        prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_xg_shots_hf")


def test_unregistered_publisher_raises() -> None:
    with pytest.raises(LeakDetectedError):
        prepare_public_upload(_tiered(["public"]), publisher="publish_brand_new_hf")


@pytest.mark.parametrize("publisher", ["publish_xg_shots_hf", "publish_psxg_shots_hf"])
def test_missing_access_tier_column_raises_leak_error_in_every_mode(publisher: str) -> None:
    # split_restricted subscripts the column directly and would raise a bare KeyError for the
    # "split" publisher if the check were not hoisted above the branch.
    with pytest.raises(LeakDetectedError, match="access_tier"):
        prepare_public_upload(pd.DataFrame({"v": [1]}), publisher=publisher)


def test_shared_receipt_accumulates_across_prepares(tmp_path: Path) -> None:
    receipt = UploadReceipt("publish_football2vec_embeddings_hf")
    for name in ("per_match", "career"):
        prepared = prepare_public_upload(
            _tiered(["public"]), publisher="publish_football2vec_embeddings_hf", receipt=receipt
        )
        prepared.public.write_parquet(tmp_path / name / "data.parquet")
    assert len(receipt.paths) == 2


# ---------------------------------------------------------------------------
# upload_guarded
# ---------------------------------------------------------------------------


class _FakeApi:
    def __init__(self, token: str | None = None) -> None:
        self.token = token
        self.created: list[tuple[str, bool]] = []
        self.uploaded: list[dict[str, object]] = []

    def create_repo(self, repo_id: str, **kw: object) -> None:
        self.created.append((repo_id, bool(kw.get("private", False))))

    def upload_folder(self, **kw: object) -> None:
        self.uploaded.append(kw)


@pytest.fixture()
def fake_api(monkeypatch: pytest.MonkeyPatch) -> _FakeApi:
    api = _FakeApi()
    monkeypatch.setattr("ingestion.hf_upload_seam.HfApi", lambda token=None: api)
    return api


def test_public_frames_create_a_public_repo(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_xg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "x.parquet")
    url = upload_guarded(staging, frames=[prepared.public], repo_id="org/repo", token=_FAKE_TOKEN)
    assert url == "https://huggingface.co/datasets/org/repo"
    assert fake_api.created == [("org/repo", False)]


def test_restricted_frames_create_a_private_repo_without_a_caller_flag(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.restricted.write_parquet(staging / "x.parquet")
    upload_guarded(staging, frames=[prepared.restricted], repo_id="org/repo-restricted", token=_FAKE_TOKEN)
    assert fake_api.created == [("org/repo-restricted", True)]


def test_restricted_frames_refuse_a_non_restricted_repo_id(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.restricted.write_parquet(staging / "x.parquet")
    with pytest.raises(TierMismatchError, match="-restricted"):
        upload_guarded(staging, frames=[prepared.restricted], repo_id="org/repo", token=_FAKE_TOKEN)
    assert fake_api.created == []


def test_public_frames_refuse_a_restricted_repo_id(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_psxg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "x.parquet")
    with pytest.raises(TierMismatchError, match="restricted companion"):
        upload_guarded(staging, frames=[prepared.public], repo_id="org/repo-restricted", token=_FAKE_TOKEN)


def test_mixed_tier_frames_refuse(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "a.parquet")
    prepared.restricted.write_parquet(staging / "b.parquet")
    with pytest.raises(TierMismatchError, match="one tier"):
        upload_guarded(staging, frames=[prepared.public, prepared.restricted], repo_id="org/repo", token=_FAKE_TOKEN)


def test_upload_guarded_refuses_an_unrecorded_file(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_xg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "guarded.parquet")
    (staging / "smuggled.parquet").write_bytes(b"not guarded")
    with pytest.raises(UnguardedFileError, match=r"smuggled.parquet"):
        upload_guarded(staging, frames=[prepared.public], repo_id="org/repo", token=_FAKE_TOKEN)
    assert fake_api.uploaded == []


def test_upload_guarded_allows_an_empty_staging_dir(tmp_path: Path, fake_api: _FakeApi) -> None:
    # ADR-049 sweep-only publish: zero partitions is legitimate — delete_patterns clears stale data.
    prepared = prepare_public_upload(_tiered([]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    staging.mkdir(parents=True)
    upload_guarded(
        staging, frames=[prepared.restricted], repo_id="org/repo-restricted", token=_FAKE_TOKEN, delete_patterns=["**"]
    )
    assert fake_api.created == [("org/repo-restricted", True)]
    assert fake_api.uploaded[0]["delete_patterns"] == ["**"]


def test_upload_guarded_refuses_an_empty_frames_list(tmp_path: Path, fake_api: _FakeApi) -> None:
    # ValueError, not TierMismatchError — an empty list is a caller bug, and without match= this
    # test would pass even if the mixed-tier branch fired for the wrong reason.
    with pytest.raises(ValueError, match="at least one GuardedFrame"):
        upload_guarded(tmp_path, frames=[], repo_id="org/repo", token=_FAKE_TOKEN)


def test_partitioned_write_records_every_partition(tmp_path: Path, fake_api: _FakeApi) -> None:
    df = pd.DataFrame({"access_tier": ["public"] * 3, "competition_id": [11, 11, 43], "v": [1, 2, 3]})
    prepared = prepare_public_upload(df, publisher="publish_freeze_frame_hf")
    staging = tmp_path / "data"
    for comp_id, part in prepared.public.groupby("competition_id"):
        part.drop_columns(["competition_id"]).write_parquet(staging / f"competition_id={comp_id}" / "data.parquet")
    assert len(prepared.public.receipt.paths) == 2
    upload_guarded(staging, frames=[prepared.public], repo_id="org/ff", token=_FAKE_TOKEN)
    assert len(fake_api.uploaded) == 1


def test_restricted_repo_suffix_matches_the_shared_helper() -> None:
    # Anti-drift: the seam string-matches the ADR-049 suffix rather than importing
    # restricted_repo_id, because hf_upload_seam importing hf_publish at module level would
    # reintroduce the cycle the bottom-of-module re-export avoids. This test ties the two together.
    from ingestion.hf_publish import restricted_repo_id
    from ingestion.hf_upload_seam import _RESTRICTED_REPO_SUFFIX

    assert restricted_repo_id("org/x").endswith(_RESTRICTED_REPO_SUFFIX)


def test_shots_on_target_query_selects_access_tier() -> None:
    # R-12: the dim_matches join already existed but access_tier was never selected, so the
    # publisher had no tier column and could not be guarded. NOTE: the SQL is built by a private
    # function, not exposed as a module constant.
    from ingestion.export_shots_on_target import _build_query

    assert "dm.access_tier" in _build_query("soccer_analytics", "dev_gold")
