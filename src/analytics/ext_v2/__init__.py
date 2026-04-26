"""ExT v2 reproduction harness — Optuna-driven Singh/KDE/KNN xT search.

Phase 0 is a Singh-2018 baseline reimplementation that must match
``analytics.expected_threat.compute_expected_threat_grid`` to numerical
tolerance (see docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md
§6 stop condition).

Phases 1-4 add Optuna axes (KDE smoothing, KNN transitions, contextual
features) via the same harness skeleton.
"""
