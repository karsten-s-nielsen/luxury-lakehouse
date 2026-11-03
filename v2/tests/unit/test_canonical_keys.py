"""Canonical object keys (spec §10.3.1, §23.6 "Canonical keys").

Every stored-file reference and every listing maps to one key: the root name plus the POSIX path relative to that root.
The form of the reference decides how it is parsed, never the host OS, so every case runs on Windows, macOS and Linux.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse.storage.keys import CanonicalKey, RootMap, StorageRootInfo, canonical_key

LOCAL_WIN = RootMap(
    lake=StorageRootInfo(local_path=Path(r"C:\Lakes\main"), hf_bucket=None, s3_bucket_path=None),
    restricted=StorageRootInfo(local_path=Path(r"C:\Lakes\main-restricted"), hf_bucket=None, s3_bucket_path=None),
)
LOCAL_POSIX = RootMap(
    lake=StorageRootInfo(local_path=Path("/Volumes/Development/lakehouse/lake"), hf_bucket=None, s3_bucket_path=None),
    restricted=StorageRootInfo(
        local_path=Path("/Volumes/Development/lakehouse/lake-restricted"), hf_bucket=None, s3_bucket_path=None
    ),
)
HF = RootMap(
    lake=StorageRootInfo(local_path=None, hf_bucket="ns/v2-lake", s3_bucket_path="s3://ns/v2-lake"),
    restricted=StorageRootInfo(local_path=None, hf_bucket="ns/v2-restricted", s3_bucket_path="s3://ns/v2-restricted"),
)


def test_relative_native_path() -> None:
    key = canonical_key("main/t/f.parquet", roots=HF, relative_to_data_path=True)
    assert key == CanonicalKey(root="lake", path="data/main/t/f.parquet")
    assert str(key) == "lake:data/main/t/f.parquet"


def test_relative_native_path_with_backslashes() -> None:
    key = canonical_key(r"main\t\f.parquet", roots=LOCAL_WIN, relative_to_data_path=True)
    assert str(key) == "lake:data/main/t/f.parquet"


def test_hf_uri_lake() -> None:
    key = canonical_key("hf://buckets/ns/v2-lake/data/bronze/t/1-a-b.parquet", roots=HF, relative_to_data_path=False)
    assert key == CanonicalKey(root="lake", path="data/bronze/t/1-a-b.parquet")


def test_hf_uri_restricted() -> None:
    key = canonical_key("hf://buckets/ns/v2-restricted/raw/abc.zst", roots=HF, relative_to_data_path=False)
    assert key == CanonicalKey(root="restricted", path="raw/abc.zst")


def test_s3_uri() -> None:
    key = canonical_key("s3://ns/v2-restricted/data/bronze/t/f.parquet", roots=HF, relative_to_data_path=False)
    assert key == CanonicalKey(root="restricted", path="data/bronze/t/f.parquet")


def test_s3_roots_sharing_a_namespace_bucket_stay_apart() -> None:
    # On HF both roots live in one gateway bucket (the namespace): s3://<namespace>/<name>. The full prefix, not the
    # bucket alone, decides the root, and a sibling name that starts with a root's name is not that root.
    assert canonical_key("s3://ns/v2-lake/data/x.parquet", roots=HF, relative_to_data_path=False) == CanonicalKey(
        root="lake", path="data/x.parquet"
    )
    assert canonical_key("s3://ns/v2-restricted/data/x.parquet", roots=HF, relative_to_data_path=False) == (
        CanonicalKey(root="restricted", path="data/x.parquet")
    )
    with pytest.raises(ValueError):
        canonical_key("s3://ns/v2-lakehouse/data/x.parquet", roots=HF, relative_to_data_path=False)


def test_bucket_name_prefix_does_not_match_another_bucket() -> None:
    # "ns/v2-lake" must not claim "ns/v2-lakehouse/..."
    with pytest.raises(ValueError):
        canonical_key("hf://buckets/ns/v2-lakehouse/data/x.parquet", roots=HF, relative_to_data_path=False)


def test_local_windows_path() -> None:
    ref = r"C:\Lakes\main-restricted\data\bronze\t\f.parquet"
    key = canonical_key(ref, roots=LOCAL_WIN, relative_to_data_path=False)
    assert key == CanonicalKey(root="restricted", path="data/bronze/t/f.parquet")
    # NTFS paths compare case-insensitively, and forward slashes are accepted
    assert str(canonical_key("c:/lakes/MAIN/data/x.parquet", roots=LOCAL_WIN, relative_to_data_path=False)) == (
        "lake:data/x.parquet"
    )


def test_local_posix_path() -> None:
    key = canonical_key(
        "/Volumes/Development/lakehouse/lake/data/bronze/t/f.parquet", roots=LOCAL_POSIX, relative_to_data_path=False
    )
    assert key == CanonicalKey(root="lake", path="data/bronze/t/f.parquet")


def test_sibling_root_is_not_confused() -> None:
    # "<lake>-restricted" starts with "<lake>" as text, but it is the other root
    key = canonical_key(
        "/Volumes/Development/lakehouse/lake-restricted/data/f.parquet", roots=LOCAL_POSIX, relative_to_data_path=False
    )
    assert key.root == "restricted"


def test_outside_roots_raises() -> None:
    for ref in (
        r"D:\elsewhere\f.parquet",
        "/srv/other/f.parquet",
        "hf://buckets/other/bucket/f.parquet",
        "s3://other/f",
    ):
        roots = LOCAL_WIN if ref.startswith("D:") else (LOCAL_POSIX if ref.startswith("/") else HF)
        with pytest.raises(ValueError, match="neither root"):
            canonical_key(ref, roots=roots, relative_to_data_path=False)


def test_traversal_refused() -> None:
    with pytest.raises(ValueError):
        canonical_key("main/../../etc/passwd", roots=HF, relative_to_data_path=True)
    with pytest.raises(ValueError):
        canonical_key("/Volumes/Development/lakehouse/lake/../x", roots=LOCAL_POSIX, relative_to_data_path=False)


def test_canonical_key_with_spaces_and_unicode() -> None:
    roots = RootMap(
        lake=StorageRootInfo(local_path=Path(r"C:\Users\Karsten\My Lake"), hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(
            local_path=Path(r"C:\Users\Karsten\My Lake-restricted"), hf_bucket=None, s3_bucket_path=None
        ),
    )
    ref = r"C:\Users\Karsten\My Lake\data\bronze\Lukáš Hrádecký.parquet"
    key = canonical_key(ref, roots=roots, relative_to_data_path=False)
    assert str(key) == "lake:data/bronze/Lukáš Hrádecký.parquet"
    assert CanonicalKey.parse(str(key)) == key


def test_parse_round_trip_and_rejects() -> None:
    key = CanonicalKey(root="restricted", path="data/bronze/t/f.parquet")
    assert CanonicalKey.parse(str(key)) == key
    for bad in ("data/x", "other:data/x", "lake:", "lake:/abs", r"lake:data\x", "lake:data/../x"):
        with pytest.raises(ValueError):
            CanonicalKey.parse(bad)
