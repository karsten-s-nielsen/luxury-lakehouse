# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "catalog_name" {
  description = "Unity Catalog name for target schemas"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "wheel_path" {
  description = "Full path to the luxury_lakehouse wheel on a UC Volume (e.g. /Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.5.20-py3-none-any.whl). Do NOT append a #sha256= fragment — serverless pip rejects it on UC Volume paths."
  type        = string
}

variable "notification_emails" {
  description = "Email addresses to notify on job start, success, and failure"
  type        = list(string)
  default     = []
}

variable "run_as_sp_application_id" {
  description = "Service principal application ID to run the ingestion job as (empty = run as job owner)"
  type        = string
  default     = ""
}

variable "ghost_gk_backend_default" {
  description = "Per-installation default AC-1 ghost-GK KDE backend (the job-parameter default; the mega-job's installation knob). Override (e.g. \"cpu-numba\") for always-accurate ghost-GK. One of scipy/vectorized/cpu-numba/fft/fft-cic."
  type        = string
  default     = "fft-cic"
}

variable "watchdog_budget_s" {
  description = "Optional per-game watchdog override seconds for the AC-1 drain worker (empty => in-code WATCHDOG_BUDGET_S=2700). Raise for slower exact ghost-GK backends."
  type        = string
  default     = ""
}
