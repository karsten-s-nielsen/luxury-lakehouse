"""ScoutGPT target — architecture search for the ScoutGPT player embedding model."""

from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE

# Generic name used by runner.py to look up the profile for any target
VALIDATION_PROFILE = SCOUTGPT_PROFILE

__all__ = ["SCOUTGPT_PROFILE", "VALIDATION_PROFILE"]
