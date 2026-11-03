"""HF storage with in-memory fakes of the S3 gateway and HfApi (spec §10.5, §29.9). The real gateway runs in Task 33."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError
from huggingface_hub import BucketFile, BucketFolder
from huggingface_hub.errors import EntryNotFoundError
from pydantic import SecretStr

from lakehouse.config.model import load_defaults
from lakehouse.errors import ConflictError, IntegrityError, StorageLimitError
from lakehouse.runs.dbt import render_profile
from lakehouse.storage import hf
from lakehouse.storage.hf import HfStorage, S3Credentials
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.roots import ResolvedRoots, open_storage

SECRET_KEY = "hf-gateway-secret-value-123"  # noqa: S105 - fake test value
TOKEN = "hf_faketokenvalue12345"  # noqa: S105 - fake test value
ROOTS = RootMap(
    lake=StorageRootInfo(local_path=None, hf_bucket="ns/v2-lake", s3_bucket_path="s3://ns/v2-lake"),
    restricted=StorageRootInfo(
        local_path=None, hf_bucket="ns/v2-restricted", s3_bucket_path="s3://ns/v2-restricted/pre"
    ),
)


def _client_error(code: str, status: int) -> ClientError:
    response: Any = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    return ClientError(response, "PutObject")


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.uploads: dict[str, list[bytes]] = {}

    def _etag(self, data: bytes) -> str:
        return '"' + hashlib.md5(data).hexdigest() + '"'  # noqa: S324 - an S3-style ETag in a fake

    def put_object(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("put_object", kw))
        body = kw["Body"]
        data = body if isinstance(body, bytes) else body.read()
        key = (kw["Bucket"], kw["Key"])
        if kw.get("IfNoneMatch") == "*" and key in self.objects:
            raise _client_error("PreconditionFailed", 412)
        if "IfMatch" in kw and (key not in self.objects or self._etag(self.objects[key]) != kw["IfMatch"]):
            raise _client_error("PreconditionFailed", 412)
        self.objects[key] = data
        return {"ETag": self._etag(data)}

    def get_object(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("get_object", kw))
        key = (kw["Bucket"], kw["Key"])
        if key not in self.objects:
            raise _client_error("NoSuchKey", 404)
        data = self.objects[key]

        class Body:
            def read(self) -> bytes:
                return data

        return {"Body": Body(), "ETag": self._etag(data)}

    def head_object(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("head_object", kw))
        key = (kw["Bucket"], kw["Key"])
        if key not in self.objects:
            raise _client_error("404", 404)
        return {"ContentLength": len(self.objects[key]), "LastModified": datetime(2026, 10, 26, tzinfo=UTC)}

    def delete_object(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("delete_object", kw))
        self.objects.pop((kw["Bucket"], kw["Key"]), None)
        return {}

    def create_multipart_upload(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("create_multipart_upload", kw))
        upload_id = f"u{len(self.uploads)}"
        self.uploads[upload_id] = []
        return {"UploadId": upload_id}

    def upload_part(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("upload_part", {k: v for k, v in kw.items() if k != "Body"}))
        self.uploads[kw["UploadId"]].append(kw["Body"])
        return {"ETag": '"part' + str(kw["PartNumber"]) + '"'}

    def complete_multipart_upload(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("complete_multipart_upload", kw))
        parts = kw["MultipartUpload"]["Parts"]
        assert [p["PartNumber"] for p in parts] == list(range(1, len(parts) + 1))
        conditions = {k: v for k, v in kw.items() if k in ("IfNoneMatch", "IfMatch")}
        body = b"".join(self.uploads[kw["UploadId"]])
        reply = self.put_object(Bucket=kw["Bucket"], Key=kw["Key"], Body=body, **conditions)
        del self.uploads[kw["UploadId"]]
        return reply

    def abort_multipart_upload(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("abort_multipart_upload", kw))
        self.uploads.pop(kw["UploadId"], None)
        return {}

    def list_objects_v2(self, **kw: Any) -> dict[str, Any]:
        self.calls.append(("list_objects_v2", kw))
        contents = [
            {"Key": k, "Size": len(v), "LastModified": datetime(2026, 10, 26, tzinfo=UTC)}
            for (b, k), v in sorted(self.objects.items())
            if b == kw["Bucket"] and k.startswith(kw["Prefix"])
        ]
        return {"Contents": contents, "IsTruncated": False}


def _bucket_file(path: str, size: int, *, uploaded: bool = True) -> BucketFile:
    # the real huggingface_hub type, built from its JSON form
    fields: dict[str, Any] = {"type": "file", "path": path, "size": size, "xetHash": f"xet-{path}-{size}"}
    if uploaded:
        fields["uploadedAt"] = "2026-10-26T00:00:00Z"
    return BucketFile(**fields)


class FakeApi:
    """An in-memory `BucketApi` with the huggingface_hub 2.3.0 signatures and return types."""

    def __init__(self) -> None:
        self.files: dict[tuple[str, str], bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.no_upload_time = False

    def batch_bucket_files(
        self,
        bucket_id: str,
        *,
        add: list[tuple[str | Path | bytes, str]] | None = None,
        delete: list[str] | None = None,
    ) -> None:
        self.calls.append(("batch_bucket_files", {"bucket_id": bucket_id, "add": add, "delete": delete}))
        for src, remote in add or []:
            self.files[(bucket_id, remote)] = src if isinstance(src, bytes) else Path(src).read_bytes()
        for remote in delete or []:
            self.files.pop((bucket_id, remote), None)

    def list_bucket_tree(
        self, bucket_id: str, prefix: str | None = None, *, recursive: bool | None = None
    ) -> list[BucketFile | BucketFolder]:
        self.calls.append(("list_bucket_tree", {"bucket_id": bucket_id, "prefix": prefix, "recursive": recursive}))
        return [
            _bucket_file(path, len(data), uploaded=not self.no_upload_time)
            for (bucket, path), data in sorted(self.files.items())
            if bucket == bucket_id and path.startswith(prefix or "")
        ]

    def get_bucket_paths_info(self, bucket_id: str, paths: Iterable[str]) -> list[BucketFile]:
        wanted = set(paths)
        return [
            _bucket_file(path, len(data), uploaded=not self.no_upload_time)
            for (bucket, path), data in sorted(self.files.items())
            if bucket == bucket_id and path in wanted
        ]

    def download_bucket_files(
        self,
        bucket_id: str,
        files: list[tuple[str | BucketFile, str | Path]],
        *,
        raise_on_missing_files: bool = False,
    ) -> None:
        # as in huggingface_hub 2.3.0: a missing file is skipped unless raise_on_missing_files is True
        for remote, local in files:
            name = remote if isinstance(remote, str) else remote.path
            if (bucket_id, name) not in self.files:
                if raise_on_missing_files:
                    raise EntryNotFoundError(f"1 file(s) not found in bucket '{bucket_id}': {name}")
                continue
            Path(local).write_bytes(self.files[(bucket_id, name)])


def _storage(*, gateway: bool = True) -> tuple[HfStorage, FakeS3, FakeApi]:
    s3, api = FakeS3(), FakeApi()
    creds = (
        S3Credentials(access_key_id=SecretStr("AKIAFAKE"), secret_access_key=SecretStr(SECRET_KEY)) if gateway else None
    )
    return HfStorage(ROOTS, hf_token=SecretStr(TOKEN), s3_key=creds, api=api, s3_client=s3), s3, api


def test_hf_routing() -> None:
    storage, s3, api = _storage()
    for key in ("data/bronze/t/1-a-b.parquet", "raw/abc.zst", "runs/lease/000000000001.json", "catalog/v/1.ducklake"):
        storage.put("lake", key, b"x")
        assert ("ns", "v2-lake/" + key) in s3.objects  # the gateway bucket is the namespace; the name is a prefix
    assert storage.put_if_absent("lake", "catalog/CURRENT", b"c")
    assert ("ns", "v2-lake/catalog/CURRENT") in s3.objects
    for key in ("config/settings.json", "runs/progress/r1.json", "runs/skipped/r1.json"):
        storage.put("lake", key, b"y")
        assert ("ns/v2-lake", key) in api.files
    storage.list("lake", "data/bronze/t/1-").__next__()
    assert ("list_objects_v2", {"Bucket": "ns", "Prefix": "v2-lake/data/bronze/t/1-"}) in [
        (n, {k: v for k, v in kw.items() if k in ("Bucket", "Prefix")}) for n, kw in s3.calls
    ]


def test_restricted_prefix_in_s3_path() -> None:
    storage, s3, _ = _storage()
    storage.put("restricted", "data/f.parquet", b"x")
    assert ("ns", "v2-restricted/pre/data/f.parquet") in s3.objects
    assert [i.key for i in storage.list("restricted", "data/")] == ["data/f.parquet"]


def test_production_gateway_path_layout() -> None:
    """The Job's S3 paths have the form `s3://<namespace>/<name>`: the HF S3 gateway treats the namespace as the bucket
    and the bucket name as the first key segment. With the production values, a write lands at bucket
    `luxury-lakehouse`, key `lake/...`. `s3://lake` and `s3://<namespace>--<name>` fail at the gateway with
    NoSuchBucket (load test, 2026-10-31)."""
    roots = RootMap(
        lake=StorageRootInfo(
            local_path=None, hf_bucket="luxury-lakehouse/lake", s3_bucket_path="s3://luxury-lakehouse/lake"
        ),
        restricted=StorageRootInfo(
            local_path=None, hf_bucket="luxury-lakehouse/restricted", s3_bucket_path="s3://luxury-lakehouse/restricted"
        ),
    )
    s3, api = FakeS3(), FakeApi()
    creds = S3Credentials(access_key_id=SecretStr("AKIAFAKE"), secret_access_key=SecretStr(SECRET_KEY))
    storage = HfStorage(roots, hf_token=SecretStr(TOKEN), s3_key=creds, api=api, s3_client=s3)
    storage.put("lake", "catalog/v/1.ducklake", b"x")
    storage.put("restricted", "data/f.parquet", b"y")
    assert ("luxury-lakehouse", "lake/catalog/v/1.ducklake") in s3.objects
    assert ("luxury-lakehouse", "restricted/data/f.parquet") in s3.objects
    # a gateway listing strips the `lake/` name segment back off (catalog/v/ is a gateway prefix, hf._GATEWAY_PREFIXES)
    assert [i.key for i in storage.list("lake", "catalog/v/")] == ["catalog/v/1.ducklake"]


def test_space_without_gateway_cannot_write_catalog_control_objects() -> None:
    storage, _, api = _storage(gateway=False)
    with pytest.raises(PermissionError):
        storage.replace_if_match("lake", "catalog/CURRENT", b"c", expected_tag='"etag"')
    with pytest.raises(PermissionError):
        storage.put_if_absent("lake", "runs/lease/000000000001.json", b"c")
    with pytest.raises(PermissionError):
        storage.put("lake", "data/x.parquet", b"x")
    storage.put("lake", "config/settings.json", b"{}")
    assert ("ns/v2-lake", "config/settings.json") in api.files


def test_hf_put_if_absent_sends_if_none_match() -> None:
    storage, s3, _ = _storage()
    assert storage.put_if_absent("lake", "runs/lease/000000000001.json", b"a") is True
    assert s3.calls[-1][1]["IfNoneMatch"] == "*"
    assert storage.put_if_absent("lake", "runs/lease/000000000001.json", b"b") is False
    assert s3.objects[("ns", "v2-lake/runs/lease/000000000001.json")] == b"a"


def test_hf_put_if_absent_refuses_non_gateway_keys() -> None:
    storage, _, _ = _storage()
    with pytest.raises(ValueError, match="conditional"):
        storage.put_if_absent("lake", "config/settings.json", b"x")


def test_hf_replace_if_match_uses_etag() -> None:
    storage, s3, _ = _storage()
    assert storage.put_if_absent("lake", "catalog/CURRENT", b"one")
    data, tag1 = storage.get_versioned("lake", "catalog/CURRENT")
    assert data == b"one"
    assert ("get_object", {"Bucket": "ns", "Key": "v2-lake/catalog/CURRENT"}) in s3.calls
    storage.replace_if_match("lake", "catalog/CURRENT", b"two", expected_tag=tag1)
    assert s3.calls[-1][1]["IfMatch"] == tag1
    with pytest.raises(ConflictError):
        storage.replace_if_match("lake", "catalog/CURRENT", b"three", expected_tag=tag1)


def test_hf_get_stat_delete() -> None:
    storage, _, _ = _storage()
    storage.put("lake", "data/x.parquet", b"abc")
    storage.put("lake", "config/c.json", b"{}")
    assert storage.get("lake", "data/x.parquet") == b"abc"
    assert storage.get("lake", "config/c.json") == b"{}"
    assert storage.stat("lake", "data/x.parquet").size == 3
    assert storage.exists("lake", "config/c.json")
    storage.delete("lake", "data/x.parquet")
    with pytest.raises(FileNotFoundError):
        storage.delete("lake", "data/x.parquet")
    storage.delete("lake", "data/x.parquet", missing_ok=True)
    with pytest.raises(FileNotFoundError):
        storage.get("lake", "data/x.parquet")


def test_read_uri_hf() -> None:
    storage, _, _ = _storage()
    assert storage.read_uri("restricted", "data/f.parquet") == "hf://buckets/ns/v2-restricted/data/f.parquet"


def test_secret_reprs_masked() -> None:
    storage, _, _ = _storage()
    creds = S3Credentials(access_key_id=SecretStr("AKIAFAKE"), secret_access_key=SecretStr(SECRET_KEY))
    for text in (repr(creds), repr(storage), str(storage)):
        assert SECRET_KEY not in text
        assert TOKEN not in text


def test_conditional_writes_are_single_part(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T6 spec D2: the gateway ignores `IfNoneMatch` on `CompleteMultipartUpload` (an overwrite, 2026-10-31), so a
    conditional write is one `put_object` whatever its size, never a multipart upload (it replaced
    `test_hf_multipart_put_if_absent`, which pinned the unfenced multipart path)."""
    monkeypatch.setattr(hf, "_MULTIPART_THRESHOLD", 4)
    monkeypatch.setattr(hf, "_PART_SIZE", 4)
    storage, s3, _ = _storage()
    src = tmp_path / "v.ducklake"
    src.write_bytes(b"0123456789")
    assert storage.put_if_absent("lake", "catalog/v/2.ducklake", src) is True
    assert s3.objects[("ns", "v2-lake/catalog/v/2.ducklake")] == b"0123456789"
    assert [name for name, _ in s3.calls] == ["put_object"]
    assert s3.calls[-1][1]["IfNoneMatch"] == "*"
    assert storage.put_if_absent("lake", "catalog/v/2.ducklake", src) is False
    tag = storage.get_versioned("lake", "catalog/v/2.ducklake")[1]
    storage.replace_if_match("lake", "catalog/v/2.ducklake", b"0123456789abcdef", expected_tag=tag)
    assert "create_multipart_upload" not in [name for name, _ in s3.calls]


@pytest.mark.parametrize("write", ["put_if_absent", "replace_if_match"])
def test_conditional_write_above_the_limit_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write: str) -> None:
    monkeypatch.setattr(hf, "SINGLE_PART_LIMIT", 5)
    storage, s3, _ = _storage()
    src = tmp_path / "v.ducklake"
    src.write_bytes(b"0123456789")
    with pytest.raises(StorageLimitError) as raised:
        if write == "put_if_absent":
            storage.put_if_absent("lake", "catalog/v/3.ducklake", src)
        else:
            storage.replace_if_match("lake", "catalog/CURRENT", b"0123456789", expected_tag='"x"')
    assert "10" in str(raised.value) and "5" in str(raised.value)
    assert s3.calls == [], "nothing may reach the gateway above the limit"


def test_plain_put_keeps_multipart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hf, "_MULTIPART_THRESHOLD", 4)
    monkeypatch.setattr(hf, "_PART_SIZE", 4)
    storage, s3, _ = _storage()
    src = tmp_path / "big.bin"
    src.write_bytes(b"0123456789")
    storage.put("lake", "data/bronze/t/big.bin", src)
    assert [name for name, _ in s3.calls].count("upload_part") == 3
    assert s3.objects[("ns", "v2-lake/data/bronze/t/big.bin")] == b"0123456789"


def test_single_part_limit_is_reported() -> None:
    storage, _, _ = _storage()
    assert storage.single_part_limit == hf.SINGLE_PART_LIMIT


def test_hf_list_pages_and_api_listing() -> None:
    storage, s3, _ = _storage()
    when = datetime(2026, 10, 26, tzinfo=UTC)
    pages = [
        {
            "Contents": [{"Key": "raw/a", "Size": 1, "LastModified": when}],
            "IsTruncated": True,
            "NextContinuationToken": "t1",
        },
        {"Contents": [{"Key": "raw/b", "Size": 2, "LastModified": when}], "IsTruncated": False},
    ]
    requests: list[dict[str, Any]] = []

    def paged(**kw: Any) -> dict[str, Any]:
        requests.append(kw)
        return pages[len(requests) - 1]

    s3.list_objects_v2 = paged
    assert [i.key for i in storage.list("lake", "raw/")] == ["raw/a", "raw/b"]
    assert requests[1]["ContinuationToken"] == "t1"
    storage.put("lake", "config/settings/v1.json", b"{}")
    storage.put("lake", "config/settings/v2.json", b"{}")
    storage.put("lake", "config/other.json", b"{}")
    keys = [i.key for i in storage.list("lake", "config/settings/v")]
    assert keys == ["config/settings/v1.json", "config/settings/v2.json"]


def test_hf_missing_upload_time_fails_loudly() -> None:
    storage, _, api = _storage()
    storage.put("lake", "config/c.json", b"{}")
    api.no_upload_time = True
    with pytest.raises(IntegrityError, match="upload time"):
        list(storage.list("lake", "config/"))


def test_open_storage_hf_from_env() -> None:
    resolved = ResolvedRoots(roots=ROOTS, source="env")
    with pytest.raises(ValueError, match="HF_TOKEN"):
        open_storage(resolved, {})
    with pytest.raises(ValueError, match="both"):
        open_storage(resolved, {"HF_TOKEN": TOKEN, "HF_S3_ACCESS_KEY_ID": "AKIAFAKE"})
    space = open_storage(resolved, {"HF_TOKEN": TOKEN})
    assert isinstance(space, HfStorage)
    assert "gateway=False" in repr(space)
    env = {"HF_TOKEN": TOKEN, "HF_S3_ACCESS_KEY_ID": "AKIAFAKE", "HF_S3_SECRET_ACCESS_KEY": SECRET_KEY}
    job = open_storage(resolved, env)
    assert "gateway=True" in repr(job)
    assert SECRET_KEY not in repr(job)
    assert TOKEN not in repr(job)


def test_duckdb_secrets_follow_spec_and_scope_the_gateway() -> None:
    hf_statement = "CREATE OR REPLACE SECRET lakehouse_hf (TYPE huggingface, PROVIDER credential_chain)"
    space, _, _ = _storage(gateway=False)
    assert space.duckdb_secrets() == [hf_statement]
    job, _, _ = _storage()
    statements = job.duckdb_secrets()
    assert statements[0] == hf_statement
    assert all(TOKEN not in statement for statement in statements)
    gateway = statements[1:]
    assert len(gateway) == 2
    for statement, scope in zip(gateway, ("s3://ns/v2-lake", "s3://ns/v2-restricted/pre"), strict=True):
        assert "TYPE s3" in statement
        assert "ENDPOINT 's3.hf.co'" in statement
        assert "URL_STYLE 'path'" in statement
        assert "REGION 'us-east-1'" in statement
        assert "USE_SSL true" in statement
        assert f"SCOPE '{scope}'" in statement
        assert SECRET_KEY in statement  # the writer needs it; the masking layer keeps it out of every log


def test_dbt_profile_names_no_secret_value() -> None:
    """The dbt profile of an HF Job reads the gateway key from the Job's environment (spec §10.5.1, §20): the rendered
    text names the variables, never a value."""
    storage, _, _ = _storage()
    text = render_profile(
        catalog_path=Path("work/lake.ducklake"), caps=load_defaults("hf").caps, storage=storage, env={}
    )
    assert "env_var('HF_S3_ACCESS_KEY_ID')" in text and "env_var('HF_S3_SECRET_ACCESS_KEY')" in text
    assert "type: huggingface" in text and 'provider: "credential_chain"' in text
    for secret in (TOKEN, SECRET_KEY, "AKIAFAKE"):
        assert secret not in text
