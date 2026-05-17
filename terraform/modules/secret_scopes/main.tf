# ──────────────────────────────────────────────────────────────────────────────
# Secret Scopes
#
# Creates Databricks secret scopes and grants READ ACLs to service principals.
# Secret VALUES are NOT managed by Terraform (never stored in state).
# After `terraform apply`, populate each scope via CLI:
#
#   databricks secrets put-secret --scope <scope> --key <key>
#
# ───────────────────────────────────────────────────────────────────────��──────

resource "databricks_secret_scope" "scope" {
  for_each = var.scopes
  name     = each.key
}

locals {
  # Flatten scopes × read_acl_sps into a list of (scope, principal) pairs
  acls = flatten([
    for scope_name, scope_cfg in var.scopes : [
      for sp in scope_cfg.read_acl_sps : {
        scope     = scope_name
        principal = sp
      }
    ]
  ])
}

resource "databricks_secret_acl" "read" {
  for_each   = { for acl in local.acls : "${acl.scope}-${acl.principal}" => acl }
  scope      = databricks_secret_scope.scope[each.value.scope].name
  principal  = each.value.principal
  permission = "READ"
}
