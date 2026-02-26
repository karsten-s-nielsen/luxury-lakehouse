# ──────────────────────────────────────────────────────────────────────────────
# Module: SQL Warehouse — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "cluster_size" {
  description = "SQL warehouse cluster size (2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large)"
  type        = string
  default     = "2X-Small"
}

variable "auto_stop_mins" {
  description = "Minutes of inactivity before the warehouse auto-stops (lower = cheaper)"
  type        = number
  default     = 10
}
