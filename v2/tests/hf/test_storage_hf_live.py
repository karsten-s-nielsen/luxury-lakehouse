"""HF storage against the real buckets and the real S3 gateway (spec §10.5, §29.9; plan Task 8 step 4, Task 33).

These tests run only inside an owner-launched HF Job that holds the Job secrets (Gate G-B). The roots come from the
non-secret environment variables of the Job, resolved by the `job` and `space` fixtures in `conftest.py`. Every object
lives under a per-run test prefix and is removed at session end; the tests never touch `catalog/CURRENT` of the lake.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from hfkit import MIB, RUN

from lakehouse.errors import ConflictError
from lakehouse.storage import hf
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.keys import ROOTS, Root

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]


@pytest.mark.parametrize("root", ROOTS)
def test_roundtrip_both_buckets(job: HfStorage, root: Root) -> None:
    gateway_key, api_key = f"data/_live/{RUN}/{root}.bin", f"runs/_live-{RUN}/{root}.json"
    job.put(root, gateway_key, b"gateway")
    job.put(root, api_key, b"{}")
    assert job.get(root, gateway_key) == b"gateway"
    assert job.get(root, api_key) == b"{}"
    assert job.stat(root, gateway_key).size == 7
    assert [i.key for i in job.list(root, f"data/_live/{RUN}/")] == [gateway_key]
    job.delete(root, gateway_key)
    assert not job.exists(root, gateway_key)


def _race(job: HfStorage, key: str, payloads: list[bytes | Path]) -> list[bool]:
    barrier = threading.Barrier(len(payloads))
    results: list[bool] = [False] * len(payloads)

    def take(index: int) -> None:
        barrier.wait()
        results[index] = job.put_if_absent("lake", key, payloads[index])

    threads = [threading.Thread(target=take, args=(i,)) for i in range(len(payloads))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def test_put_if_absent_race_single_part(job: HfStorage) -> None:
    """A characterization (T6 spec, Evidence): the gateway's `If-None-Match` is not atomic under concurrency, so more
    than one of six concurrent creates may succeed (6 of 6 in one Job), and v2 never relies on it for the single
    writer: the lease ledger is the fence (ADR-007 T6 amendment). What v2 does rely on holds: at least one create
    succeeds, and the stored object is exactly one complete payload, never a torn mix."""
    key = f"runs/lease/_live-{RUN}/000000000001.json"
    payloads: list[bytes | Path] = [f"holder-{i}".encode() for i in range(6)]
    results = _race(job, key, payloads)
    assert results.count(True) >= 1
    assert job.get("lake", key) in [payloads[i] for i, won in enumerate(results) if won]


def test_put_if_absent_race_large_path(job: HfStorage, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Since T6 (spec D2) a conditional write is one part whatever its size; with the multipart threshold lowered to
    5 MiB, a race of three 11 MiB creates still stores exactly one complete payload (a multipart upload could mix
    parts, and its `If-None-Match` was ignored). The winner count is not asserted: the gateway's condition is not
    atomic under concurrency (see the single-part characterization above)."""
    monkeypatch.setattr(hf, "_MULTIPART_THRESHOLD", 5 * MIB)
    monkeypatch.setattr(hf, "_PART_SIZE", 5 * MIB)
    payloads: list[bytes | Path] = []
    for i in range(3):
        path = tmp_path / f"v{i}.ducklake"
        path.write_bytes(bytes([i]) * (11 * MIB))
        payloads.append(path)
    key = f"catalog/v/_live-{RUN}/1.ducklake"
    results = _race(job, key, payloads)
    assert results.count(True) >= 1
    stored = job.get("lake", key)
    assert stored in [p.read_bytes() for p, won in zip(payloads, results, strict=True) if won and isinstance(p, Path)]


def test_lease_renew_and_release_are_plain_puts(job: HfStorage) -> None:
    key = f"runs/lease/_live-{RUN}/000000000002.json"
    assert job.put_if_absent("lake", key, b'{"state": "held"}') is True
    job.put("lake", key, b'{"state": "renewed"}')
    job.put("lake", key, b'{"state": "released"}')
    assert job.get("lake", key) == b'{"state": "released"}'


def test_replace_if_match_stale_etag_is_412(job: HfStorage) -> None:
    key = f"catalog/v/_live-{RUN}/cas"
    assert job.put_if_absent("lake", key, b"one") is True
    _, tag1 = job.get_versioned("lake", key)
    tag2 = job.replace_if_match("lake", key, b"two", expected_tag=tag1)
    assert tag2 != tag1
    with pytest.raises(ConflictError) as caught:
        job.replace_if_match("lake", key, b"three", expected_tag=tag1)
    cause = caught.value.__cause__
    assert isinstance(cause, ClientError)
    assert cause.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412
    assert job.get("lake", key) == b"two"


@pytest.mark.parametrize("root", ROOTS)
def test_s3_write_and_hf_read_name_the_same_object(job: HfStorage, space: HfStorage, root: Root) -> None:
    key = f"data/_live/{RUN}/{root}-mapping.bin"
    job.put(root, key, b"same object")
    assert space.get(root, key) == b"same object", "the s3:// path and the hf:// bucket name different objects"
    assert space.read_uri(root, key).startswith("hf://buckets/")


def test_hf_read_sees_gateway_write_at_once(job: HfStorage, space: HfStorage) -> None:
    key = f"data/_live/{RUN}/visibility.bin"
    job.put("lake", key, b"first")
    assert space.get("lake", key) == b"first", "an hf:// read did not see a new gateway write at once"
    job.put("lake", key, b"second")
    assert space.get("lake", key) == b"second", "an hf:// read returned a stale object after a gateway overwrite"


# -- item 15: the key scope, the narrow-prefix list and the list/stat storage clock --------------------------------


@pytest.mark.parametrize("root", ROOTS)
def test_gateway_reaches_both_buckets_and_they_are_isolated(job: HfStorage, root: Root) -> None:
    """The one gateway key writes and reads both v2 buckets (item 15), and the two buckets are separate: an object in
    one never appears in the other. Whether the key is limited to exactly these two buckets (no third) the owner reads
    from the HF token page before first use, because a test cannot enumerate every bucket a key might reach."""
    key = f"data/_live/{RUN}/{root}-scope.bin"
    job.put(root, key, b"scope")
    assert job.get(root, key) == b"scope"
    other: Root = "restricted" if root == "lake" else "lake"
    assert key not in [i.key for i in job.list(other, f"data/_live/{RUN}/")], "the two buckets share objects"
    job.delete(root, key)


def test_gateway_list_narrows_to_one_match(job: HfStorage) -> None:
    """A live gateway `list` of the narrow prefix `data/bronze/<table>/<match_key>-` returns exactly that match's
    keys and no key of another match (item 15). Two match-key tokens, one with two files, the other with one."""
    table = "skillcorner_tracking_players"
    base = f"data/bronze/{table}"
    match_a, match_b = f"7{RUN}", f"8{RUN}"
    keys_a = [f"{base}/{match_a}-v1-000.parquet", f"{base}/{match_a}-v1-001.parquet"]
    key_b = f"{base}/{match_b}-v1-000.parquet"
    written = [*keys_a, key_b]
    for key in written:
        job.put("lake", key, b"x")
    try:
        listed = [i.key for i in job.list("lake", f"{base}/{match_a}-")]
        assert sorted(listed) == sorted(keys_a)
        assert key_b not in listed
    finally:
        for key in written:
            job.delete("lake", key, missing_ok=True)


def test_list_and_stat_agree_on_the_storage_clock(job: HfStorage) -> None:
    """`list` and `stat` report the same storage time for a gateway-written object and an `HfApi`-written object
    (item 15): both read the one storage clock, not a local one. The precision can differ: on 2026-10-31 an HF Job
    measured milliseconds from the gateway listing and whole seconds (truncated) from `HeadObject`. The code relies
    only on `stat` <= `list` within one second: the sweep's age fence compares listed times with the lease start from
    `stat`, and a truncated start only ever spares an object, never dooms one stored after the start."""
    gateway_key, api_key = f"data/_live/{RUN}/clock.bin", f"runs/_live-{RUN}/clock.json"
    job.put("lake", gateway_key, b"gateway")
    job.put("lake", api_key, b"{}")
    for key, prefix in ((gateway_key, f"data/_live/{RUN}/"), (api_key, f"runs/_live-{RUN}/")):
        [listed] = [i for i in job.list("lake", prefix) if i.key == key]
        stat = job.stat("lake", key)
        assert listed.modified.tzinfo is not None, f"{key}: the storage time is not timezone-aware"
        assert stat.modified.tzinfo is not None, f"{key}: the stat time is not timezone-aware"
        assert listed.modified.replace(microsecond=0) == stat.modified.replace(microsecond=0), key
        assert stat.modified <= listed.modified, f"{key}: stat is later than the listing"
