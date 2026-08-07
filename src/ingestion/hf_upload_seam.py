"""The single door for publishing a public HuggingFace artifact (ADR-072).

Replaces the prior convention — "every publisher remembers to call ``assert_no_private_leak``" —
with a seam that proves it. ``prepare_public_upload`` performs guard -> split -> drop; the returned
``GuardedFrame`` records every path it writes; ``upload_guarded`` refuses to upload a staging
directory containing any file no receipt accounts for, and derives repo privacy from the frame's
tier rather than from a caller-supplied flag.

Three independent controls, each closing a hole the others cannot:

1. **Frame authorization** — ``GuardedFrame`` is a public dataclass, so a caller could construct
   one around an unguarded frame, or substitute one via ``dataclasses.replace``. Both routes are
   inert: ``write_parquet`` refuses any frame the seam did not itself produce.
2. **Path diff** — a publisher could guard one frame and stage a second, unguarded one into the
   same directory. ``upload_guarded`` refuses on any file no receipt recorded.
3. **AST ban** (``src/tests/test_publisher_seam_conformance.py``) — makes an attempt at any of the
   above visible at lint time rather than only at publish time.

Perfect enforcement is not reachable in Python and is not the goal. The achievable goal — that a
bypass requires a line which is both obviously wrong to a reviewer *and* fails a gate — is met by
these together, and by none of them alone.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from huggingface_hub import HfApi

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

_ACCESS_TIER_COLUMN = "access_tier"

# ADR-049 companion-repo naming. Deliberately a literal rather than an import of
# ``ingestion.hf_publish.restricted_repo_id``: importing hf_publish at module level would
# reintroduce the cycle the bottom-of-module re-export avoids, and a function-local import inside a
# hot check is worse than a constant. Drift is blocked by
# ``test_hf_upload_seam.test_restricted_repo_suffix_matches_the_shared_helper``.
_RESTRICTED_REPO_SUFFIX = "-restricted"


class UnguardedFileError(RuntimeError):
    """A staging directory contains a file that no ``GuardedFrame`` wrote."""


class TierMismatchError(RuntimeError):
    """A repo's privacy or naming does not match the tier of the frames being uploaded to it."""


class UnauthorizedFrameError(RuntimeError):
    """A ``GuardedFrame`` holds a DataFrame the seam never produced."""


class UploadReceipt:
    """Records paths written through a ``GuardedFrame``, and which frame objects are authorized.

    The authorization list is what makes ``GuardedFrame`` non-forgeable. ``GuardedFrame`` is a
    public dataclass, so both ``GuardedFrame(frame=arbitrary_df, ...)`` and
    ``dataclasses.replace(guarded, frame=arbitrary_df)`` produce a wrapper that never passed
    ``prepare_public_upload`` — and neither the path diff nor the AST ban would notice, because the
    forged wrapper writes through the normal path and touches no HF symbol. Authorization attaches
    to the **DataFrame object**, not to the receipt, precisely because a forger can borrow the real
    receipt but cannot fabricate a frame the seam itself created.
    """

    def __init__(self, publisher: str) -> None:
        self.publisher = publisher
        self._paths: set[Path] = set()
        # Strong references, never id(): a freed DataFrame's id can be reused by a later
        # allocation, which would authorize an arbitrary frame by coincidence. The frames are alive
        # for the duration of the publish anyway, so this costs one pointer each.
        self._authorized: list[pd.DataFrame] = []

    def record(self, path: Path) -> None:
        self._paths.add(path.resolve())

    def _authorize(self, frame: pd.DataFrame) -> None:
        """Register a frame the SEAM produced.

        Called only by ``prepare_public_upload`` and by ``GuardedFrame``'s own derivations — never
        by a publisher, which the AST ban enforces.
        """
        self._authorized.append(frame)

    def is_authorized(self, frame: pd.DataFrame) -> bool:
        return any(frame is f for f in self._authorized)

    @property
    def paths(self) -> frozenset[Path]:
        return frozenset(self._paths)


# eq=False: the synthesised __eq__/__hash__ would compare/hash a DataFrame field. Comparing two
# DataFrames returns a DataFrame whose bool() raises, and hashing one raises TypeError.
@dataclasses.dataclass(frozen=True, eq=False)
class GuardedFrame:
    """A frame that has passed the access-tier guard and had ``access_tier`` dropped.

    A ``GuardedFrame`` can only write a frame the seam itself produced: ``prepare_public_upload``,
    ``groupby`` and ``drop_columns`` register their outputs on the receipt, and ``write_parquet``
    refuses anything else. Direct construction, or ``dataclasses.replace`` with a substituted
    frame, is therefore **inert** — and the AST ban makes the attempt visible at lint time.

    (``frozen=True`` alone does not close this: ``dataclasses.replace`` re-runs ``__post_init__``
    but carries every unreplaced field through, so a constructor sentinel would block direct
    construction and still permit frame substitution. Verified empirically on Python 3.10.)

    Derivations return children sharing the SAME receipt, so a partitioned write stays fully
    accounted for.
    """

    frame: pd.DataFrame
    tier: str
    publisher: str
    receipt: UploadReceipt

    def write_parquet(self, path: Path) -> None:
        if not self.receipt.is_authorized(self.frame):
            raise UnauthorizedFrameError(
                f"{self.publisher}: GuardedFrame holds a frame the seam did not produce — it was "
                f"constructed directly or substituted via dataclasses.replace. Obtain frames from "
                f"prepare_public_upload / groupby / drop_columns only (ADR-072)."
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        self.frame.to_parquet(path, index=False, engine="pyarrow")
        self.receipt.record(path)
        logger.info("seam: %s wrote %d rows -> %s", self.publisher, len(self.frame), path)

    def groupby(self, column: str) -> Iterator[tuple[Any, GuardedFrame]]:
        for key, sub in self.frame.groupby(column):
            self.receipt._authorize(sub)  # same-module private; the seam owns authorization
            yield key, dataclasses.replace(self, frame=sub)

    def drop_columns(self, columns: list[str]) -> GuardedFrame:
        child = self.frame.drop(columns=columns, errors="ignore")
        self.receipt._authorize(child)  # same-module private; the seam owns authorization
        return dataclasses.replace(self, frame=child)


@dataclasses.dataclass(frozen=True, eq=False)
class PreparedUpload:
    """Guard result. ``restricted`` is None for non-``split`` publishers."""

    public: GuardedFrame
    restricted: GuardedFrame | None


def prepare_public_upload(
    df: pd.DataFrame,
    *,
    publisher: str,
    receipt: UploadReceipt | None = None,
) -> PreparedUpload:
    """Guard -> split -> drop ``access_tier``, returning guarded frames ready to stage.

    Mode is read from ``PUBLISHER_REGISTRY`` — a property of the call, not of a docstring.

    Fail-closed preconditions run BEFORE any mode branch: an unregistered publisher and a frame
    with no ``access_tier`` column both raise ``LeakDetectedError`` in every mode. This ordering is
    deliberate — ``split_restricted`` subscripts the column directly, so splitting first would
    surface a missing column as a bare ``KeyError`` on the ``split`` path only, which reads as a
    bug rather than a security refusal and would escape an ``except LeakDetectedError`` caller.

    Pass ``receipt`` to accumulate several frames under one receipt (the football2vec
    per_match/career/season case) so a single ``upload_guarded`` can account for all of them.
    """
    from ingestion.hf_leak_guard import PUBLISHER_REGISTRY, assert_no_private_leak, assert_publishable_frame
    from ingestion.hf_publish import split_restricted

    assert_publishable_frame(df, publisher=publisher)
    shared = receipt if receipt is not None else UploadReceipt(publisher)

    def _guard(frame: pd.DataFrame, tier: str) -> GuardedFrame:
        stripped = frame.drop(columns=[_ACCESS_TIER_COLUMN], errors="ignore")
        shared._authorize(stripped)  # same-module private; the seam owns authorization
        return GuardedFrame(frame=stripped, tier=tier, publisher=publisher, receipt=shared)

    if PUBLISHER_REGISTRY[publisher] == "split":
        public_df, restricted_df = split_restricted(df, column=_ACCESS_TIER_COLUMN)
        assert_no_private_leak(public_df, publisher=publisher)
        return PreparedUpload(public=_guard(public_df, "public"), restricted=_guard(restricted_df, "restricted"))

    # "fail_closed" and "derived": the whole frame must already be public.
    assert_no_private_leak(df, publisher=publisher)
    return PreparedUpload(public=_guard(df, "public"), restricted=None)


def upload_guarded(
    staging_dir: Path,
    *,
    frames: list[GuardedFrame],
    repo_id: str,
    token: str,
    path_in_repo: str = "data",
    delete_patterns: list[str] | None = None,
    repo_type: str = "dataset",
) -> str:
    """Upload a staging directory, refusing any file no ``GuardedFrame`` recorded.

    Repo privacy is DERIVED from the frames' tier — there is no caller-supplied ``private`` flag to
    forget. All frames must share one tier, and the repo id must match the ADR-049 naming
    convention for that tier.

    An empty staging directory is legitimate — the ADR-049 sweep-only publish uploads zero
    partitions so ``delete_patterns`` clears previously-restricted data. Emptiness is NOT an error;
    an *unaccounted* file is.

    ``delete_patterns`` are matched RELATIVE to ``path_in_repo``, so the only correct whole-path
    sweep is ``["**"]`` — a ``"data/"``-prefixed pattern silently matches nothing (ADR-049). No
    pattern can reach a file ABOVE ``path_in_repo``.
    """
    if not frames:
        # A caller bug, not a tier mismatch — reusing TierMismatchError would make the name lie.
        raise ValueError(f"upload_guarded requires at least one GuardedFrame (repo {repo_id!r})")
    tiers = {f.tier for f in frames}
    if len(tiers) != 1:
        raise TierMismatchError(f"all frames must share one tier, got {sorted(tiers)} for repo {repo_id!r}")
    tier = tiers.pop()
    publisher = frames[0].publisher
    private = tier == "restricted"
    if private and not repo_id.endswith(_RESTRICTED_REPO_SUFFIX):
        raise TierMismatchError(
            f"{publisher}: restricted frames target {repo_id!r}, which lacks the "
            f"{_RESTRICTED_REPO_SUFFIX!r} suffix (ADR-049 companion-repo convention)"
        )
    if not private and repo_id.endswith(_RESTRICTED_REPO_SUFFIX):
        raise TierMismatchError(f"{publisher}: public frames target the restricted companion {repo_id!r}")

    recorded = {p for f in frames for p in f.receipt.paths}
    actual = {p.resolve() for p in Path(staging_dir).rglob("*") if p.is_file()}
    unaccounted = sorted(str(p) for p in actual - recorded)
    if unaccounted:
        logger.error(
            "UPLOAD BLOCKED: %s staged %d file(s) no GuardedFrame recorded: %s",
            publisher,
            len(unaccounted),
            unaccounted,
        )
        raise UnguardedFileError(
            f"{publisher}: {len(unaccounted)} unguarded file(s) in staging dir — every file must be "
            f"written via GuardedFrame.write_parquet: {unaccounted}"
        )

    api = HfApi(token=token)
    api.create_repo(repo_id, exist_ok=True, repo_type=repo_type, token=token, private=private)
    api.upload_folder(
        folder_path=str(staging_dir),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type=repo_type,
        token=token,
        delete_patterns=delete_patterns,
    )
    logger.info(
        "seam: %s uploaded %d file(s) to %s (tier=%s, private=%s)", publisher, len(actual), repo_id, tier, private
    )
    return f"https://huggingface.co/{repo_type}s/{repo_id}"
