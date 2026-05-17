variable "scopes" {
  description = "Secret scopes to create with ACLs. Values are added via CLI (never in Terraform state)."
  type = map(object({
    keys         = list(string)
    read_acl_sps = list(string)
  }))
}
