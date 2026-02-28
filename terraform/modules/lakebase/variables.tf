# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "autoscaling_min_cu" {
  description = "Minimum compute units for autoscaling (scales to zero when suspended)"
  type        = number
  default     = 0.5
}

variable "autoscaling_max_cu" {
  description = "Maximum compute units for autoscaling"
  type        = number
  default     = 4
}

variable "suspend_timeout_duration" {
  description = "Duration of inactivity before the endpoint suspends (e.g. '300s')"
  type        = string
  default     = "300s"
}
