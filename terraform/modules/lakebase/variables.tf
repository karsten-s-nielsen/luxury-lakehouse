# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "capacity" {
  description = "Capacity SKU for the Lakebase instance (CU_1, CU_2, CU_4, CU_8)"
  type        = string
  default     = "CU_1"
}

variable "stopped" {
  description = "Whether the Lakebase instance is stopped (hibernated). Set true to save costs when not in use."
  type        = bool
  default     = false
}
