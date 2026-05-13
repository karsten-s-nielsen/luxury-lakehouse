# Decision: Taipy Selected as Production Dashboard Framework

**Status:** Accepted
**Date:** 2026-04-02

## Context

The platform needed a production dashboard framework for 14+ analytics pages served on HF Spaces. Streamlit was the initial implementation choice. As the page count grew, several structural limitations became clear: no WebSocket-based state sync (full page re-renders on state changes), no template-driven layout mechanism (each page hand-crafted in isolation), and a single-threaded page model that made multi-user concurrency unreliable. By 12 pages, layout drift, missing tooltips, and inconsistent metric formats had accumulated across pages — a direct consequence of the hand-crafted-per-page approach.

## Decision

Migrate from Streamlit to Taipy 4.x, deployed on HF Spaces Docker SDK. Taipy's WebSocket-based state sync handles reactive filter cascades (competition → team → match) without full page reloads. Its Python-native page model allowed introducing `build_page(PageConfig)` — a template-driven architecture where a page is declarative configuration (title, metrics, widgets, content blocks, citations), not imperative layout code.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Streamlit (incumbent) | No WebSocket reactivity, no template architecture, drift inevitable at scale |
| Gradio | Was used for a lightweight demo Space (deprecated 2026-05); not suitable for 14-page analytics dashboard with sidebar filters |
| Panel | Python-native, mature ecosystem, but more complex for non-Bokeh chart stacks |
| Dash | React-based, stronger for production, but requires JavaScript for customization and has higher operational complexity |

Taipy won for WebSocket-based state sync, template-driven `build_page(PageConfig)` architecture, and Python-native development with no JavaScript required.

## Consequences

**Positive:**
- Template-first architecture makes missing tooltips and inconsistent formatting structurally impossible — required fields are constructor parameters, not afterthoughts.
- A CHI audit against the template produces template-level fixes (one change, all 14 pages); without it, the same audit would produce 14 per-page fixes.
- Zero JavaScript in the dashboard codebase.

**Negative:**
- Smaller ecosystem than Streamlit; fewer third-party component options.
- Single-worker WebSocket constraint means horizontal scaling requires sticky sessions or session affinity — not currently relevant on HF Spaces but limits future scale-out options.
- Taipy's `|filter|` modifier breaks dropdown LOVs and must be avoided (opt-in only, documented in feedback).
