"""A pining module's listing failure and adapter warnings never store private text (audit T5, spec §9). The sibling
handlers already store only the error class for a private match (`error_message`); the module-level listing failure
and the adapter warnings now follow the same rule on the pining channel, and stay in full on a public channel."""

from __future__ import annotations

from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import ModuleConfig, ModuleId
from lakehouse.runs.history import read_table
from lakehouse.runs.run import run_ingestion

SECRET = "Secret FC vs Hidden United, 2026-10-31"  # noqa: S105 - a private match label, not a password


def _errors(kit: LakeKit) -> list[dict[str, Any]]:
    with CatalogReader(kit.storage).session() as conn:
        return read_table(conn, "ops_errors")


def _run(kit: LakeKit, adapter: FakeAdapter) -> None:
    run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},  # type: ignore[dict-item]
        parsers={adapter.provider: FakeParser(adapter.provider)},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )


@pytest.mark.parametrize(
    ("module", "channel", "redacted"), [("statsbomb_pining", "pining", True), ("statsbomb_public", "public", False)]
)
def test_listing_failure_text(kit: LakeKit, module: ModuleId, channel: str, redacted: bool) -> None:
    save_settings(kit.storage, [active(module)])
    adapter = FakeAdapter(module, "statsbomb", channel, [], {})  # type: ignore[arg-type]
    adapter.fail_listing = RuntimeError(f"listing failed for {SECRET}")
    _run(kit, adapter)
    [row] = [r for r in _errors(kit) if r["kind"] == "list_failed"]
    assert (SECRET in row["message"]) is not redacted
    if redacted:
        assert row["message"] == f"{module}: RuntimeError"


@pytest.mark.parametrize(
    ("module", "channel", "redacted"), [("statsbomb_pining", "pining", True), ("statsbomb_public", "public", False)]
)
def test_adapter_warning_text(kit: LakeKit, module: ModuleId, channel: str, redacted: bool) -> None:
    save_settings(kit.storage, [active(module)])
    files = {("3788741", "events"): b"a\nb"}
    adapter = FakeAdapter(module, "statsbomb", channel, [entry(module, "3788741")], files)  # type: ignore[arg-type]
    adapter.warnings.append(f"skipped a malformed entry: {SECRET}")
    _run(kit, adapter)
    [row] = [r for r in _errors(kit) if r["kind"] == "adapter_warning"]
    assert (SECRET in row["message"]) is not redacted


def test_visibility_listing_failure_text(kit: LakeKit) -> None:
    """The visibility pass lists only pining modules, so its listing failure stores the error class, never the
    text (Reviewer A T3-IMPL-01: the sibling of the T5 listing redaction)."""
    module: ModuleId = "gradientsports_pining"
    files = {("3812", "events"): b"a\nb"}
    adapter = FakeAdapter(module, "gradientsports", "pining", [entry(module, "3812")], files)  # type: ignore[arg-type]
    save_settings(kit.storage, [active(module)])
    _run(kit, adapter)  # an ingested pining match makes the visibility pass list this module
    save_settings(kit.storage, [ModuleConfig(module_id=module, active=False, filter=[])])
    adapter.fail_listing = RuntimeError(f"listing failed for {SECRET}")
    _run(kit, adapter)
    [row] = [r for r in _errors(kit) if r["kind"] == "visibility_check_failed"]
    assert row["message"] == f"{module}: RuntimeError"
