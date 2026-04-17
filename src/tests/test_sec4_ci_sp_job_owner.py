"""SEC4: the daily-job ACL resource must be authoritative, correctly
named, and carry both hf_app_v2 CAN_VIEW + CI SP IS_OWNER access_control
blocks sorted alphabetically by principal.

Rename from `hf_app_view_ingestion_job` to `ingestion_job_acl` is done via
a Terraform `moved` block for safe state migration.

This test parses Terraform statically. Live plan verification happens in
Step 4.6.
"""

from __future__ import annotations

import re
from pathlib import Path

_DEV = Path(__file__).resolve().parents[2] / "terraform" / "environments" / "dev" / "main.tf"

_CI_SP_REF = "module.service_principals.terraform_ci_sp_application_id"
_APP_SP_REF = "module.service_principals.hf_app_sp_application_id"


def _extract_resource_body(text: str, resource_type: str, resource_name: str) -> str | None:
    """Return the full body of a named resource block, or None if not found."""
    pattern = re.compile(
        rf'^resource\s+"{re.escape(resource_type)}"\s+"{re.escape(resource_name)}"\s*\{{', re.MULTILINE
    )
    m = pattern.search(text)
    if not m:
        return None
    lines = text.splitlines(keepends=True)
    start_line = text[: m.start()].count("\n")
    depth = 0
    out: list[str] = []
    for line in lines[start_line:]:
        out.append(line)
        depth += line.count("{") - line.count("}")
        if depth == 0 and out:
            return "".join(out)
    return None


def _access_control_principals_in_order(body: str) -> list[tuple[str, str]]:
    """Return [(principal_ref, permission_level), ...] in declared order.

    Matches `access_control { ... }` sub-blocks with any attribute order
    inside. Principal ref is captured verbatim (may include module path
    like `module.service_principals.hf_app_sp_application_id`).
    """
    # Split on access_control { ... } blocks (no nested braces expected inside).
    block_re = re.compile(r"access_control\s*\{([^{}]*)\}", re.DOTALL)
    sp_re = re.compile(r"service_principal_name\s*=\s*(\S+)")
    level_re = re.compile(r'permission_level\s*=\s*"([^"]+)"')
    out: list[tuple[str, str]] = []
    for m in block_re.finditer(body):
        inner = m.group(1)
        sp_m = sp_re.search(inner)
        lvl_m = level_re.search(inner)
        if sp_m and lvl_m:
            out.append((sp_m.group(1).rstrip(","), lvl_m.group(1)))
    return out


def _assert_acl_resource_correctly_shaped(body: str, resource_name: str) -> None:
    principals = _access_control_principals_in_order(body)
    principal_refs = [p for p, _ in principals]
    perms = dict(principals)
    assert _CI_SP_REF in perms, f"{resource_name}: CI SP access_control block missing"
    assert perms[_CI_SP_REF] == "IS_OWNER", f"{resource_name}: CI SP must be IS_OWNER, got {perms[_CI_SP_REF]!r}"
    assert _APP_SP_REF in perms, f"{resource_name}: hf_app_v2 CAN_VIEW block missing"
    assert perms[_APP_SP_REF] == "CAN_VIEW", f"{resource_name}: hf_app_v2 must be CAN_VIEW, got {perms[_APP_SP_REF]!r}"
    assert principal_refs == sorted(principal_refs), (
        f"{resource_name}: access_control blocks must be sorted alphabetically by "
        f"service_principal_name; got {principal_refs}"
    )


def test_ingestion_job_acl_exists_and_is_correctly_shaped() -> None:
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "ingestion_job_acl")
    assert body, "resource databricks_permissions.ingestion_job_acl not found"
    _assert_acl_resource_correctly_shaped(body, "ingestion_job_acl")


def test_sync_hf_costs_job_acl_resource_removed() -> None:
    """The standalone sync_hf_costs_daily job + its ACL were removed — the
    sub-operation already runs daily via hf_sync super-task. Guard against
    accidental re-introduction."""
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "sync_hf_costs_job_acl")
    assert body is None, "databricks_permissions.sync_hf_costs_job_acl must not be re-introduced"
    job_body = _extract_resource_body(text, "databricks_job", "sync_hf_costs_daily")
    assert job_body is None, "databricks_job.sync_hf_costs_daily must not be re-introduced"


def test_old_resource_names_have_moved_blocks() -> None:
    """Rename must be declared via Terraform `moved` block so apply is a
    rename rather than destroy/create — zero ACL gap."""
    text = _DEV.read_text(encoding="utf-8")
    old = "databricks_permissions.hf_app_view_ingestion_job"
    new = "databricks_permissions.ingestion_job_acl"
    pattern = re.compile(
        rf"moved\s*\{{\s*from\s*=\s*{re.escape(old)}\s*to\s*=\s*{re.escape(new)}\s*\}}",
        re.DOTALL,
    )
    assert pattern.search(text), f"missing moved block: from {old} to {new}"


def test_no_orphaned_old_resource_names() -> None:
    """After rename, the old resource name must not remain as a resource."""
    text = _DEV.read_text(encoding="utf-8")
    for old in ("hf_app_view_ingestion_job", "hf_app_view_sync_hf_costs_job"):
        pattern = re.compile(rf'^resource\s+"databricks_permissions"\s+"{old}"\s*\{{', re.MULTILINE)
        assert not pattern.search(text), f"old resource name {old!r} still declared — rename incomplete"
