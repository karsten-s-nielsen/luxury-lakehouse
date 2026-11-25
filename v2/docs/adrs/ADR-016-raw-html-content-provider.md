# ADR-016: Raw HTML/SVG via a Taipy content provider, framed same-origin

## Status

Accepted (2026-11-05). Decided after the AI/ML Workflows DAG did not render on the live Mac Space: the page inlined a
raw `<svg>` and Taipy dropped it. Root-caused in Taipy 4.1.2. The same-origin framing carve-out (Decision 3) is a
security-boundary change; both impl reviewers (A-IMPL-04 / B-CALC-IMPL-02) held that the DECISION, not only the commit,
needs the owner's sign-off — the owner **explicitly approved** it on 2026-11-05. Uncommitted, still pending the owner's
commit approval. Amends ADR-008 (Taipy for the Ops UI) and ADR-013 (the page interface), which fixed the declarative
element set (`Prose`/`Text`/`Table`/`Control`/`Button`/`Section`) with no raw-HTML element. Carves one scoped exception
into the audit-S2 defence-in-depth framing headers (`X-Frame-Options`, CSP `frame-ancestors`).

## Context

- v2 pages are declarative: a `PageConfig` is turned into Taipy Markdown by `build_page`. `Prose` renders a fixed
  Markdown string in the page definition; `Text` renders a reactive variable with HTML escaped.
- The AI/ML Workflows page (spec D9, plan Task 9) needs a forward-DAG diagram — a self-contained inline SVG (no CDN,
  CSP-safe). The first implementation inlined it in a `Prose` element.
- Taipy's Markdown→JSX layer renders only whitelisted host tags (`<div>`, `<span>` pass). It intercepts a raw `<svg>`
  as an unknown component and drops it, rendering the literal text **"Component svg is not registered"**. The Task-9
  unit tests checked the SVG *string* and the page *registration*, not the Taipy *render*, so they were green on a DAG
  that never displayed. The Puppeteer e2e on the Mac caught it (2026-11-05).
- v1 (`hf_taipy_app`) never inlines raw HTML. It registers a Taipy content provider for a `RawHtml` type and binds it
  with `<|part|content={var}|>`, which Taipy renders inside a same-origin iframe served from `/taipy-user-content/`.
  The iframe bypasses the Markdown→JSX whitelist, so arbitrary HTML/SVG renders (`hf_taipy_app/src/main.py`).
- The content iframe is same-origin, but the audit-S2 headers set `X-Frame-Options: DENY` and CSP
  `frame-ancestors 'none'` on **every** response. The browser therefore refused to frame the app's own content
  endpoint, and the iframe showed a broken-content icon. The endpoint itself returned `200` with the real
  `<svg width="1460" …>` body — verified live by fetching it from the signed-in page (`x-frame-options: DENY`).

## Decision

1. **Add a `RawHtml` marker type and a `Content` element to the template.** `shell` registers the provider once at
   import (`Gui.register_content_provider(RawHtml, …)` — Taipy keeps a class-level registry every Gui shares). A page
   binds a `RawHtml` variable; the template emits `<|part|content={var}|height=Npx|>`. The content part does not
   auto-size, so `height` is fixed to the content's own pixel height (the DAG supplies `dag_frame_height`).
2. **A `Content`/`RawHtml` element carries NON-identity structure only** (a static card DAG). A content provider is an
   iframe, so its markup bypasses the `render_for` text proof (spec §6.2); the §6.3 anon-view gating
   (`reads_match_data` + `ANON_VIEWABLE_ROUTES`) stays the control that keeps private data off a public page. A page
   whose raw HTML could hold match identity must not be anon-viewable.
3. **Relax framing for the `/taipy-user-content/` endpoint ONLY** → `X-Frame-Options: SAMEORIGIN` and CSP
   `frame-ancestors 'self'`, set by the `_security_headers` `after_request` on that path prefix. Every real page keeps
   `DENY` / `'none'`. A cross-origin attacker still cannot frame the app (no clickjacking); only the app's own
   same-origin page can frame its own content endpoint.
4. **Both halves are test-enforced.** `test_content_element_renders_a_content_provider_part`,
   `test_rawhtml_wraps_a_markup_string`, `test_dag_embedded_via_content_provider_not_prose` and
   `test_dag_frame_height_matches_the_svg_height` cover the element and the page; the existing
   `test_security_headers_on_every_response` holds `DENY` / `'none'` on a real page, and
   `test_content_provider_endpoint_allows_same_origin_framing` holds `SAMEORIGIN` / `'self'` on the content endpoint.

## Consequences

- The AI/ML Workflows DAG renders — verified live on the Mac (2026-11-05): source nodes, the three SPADL computes plus
  pitch-control and team-shape, the gold mart node, left-to-right edges, the type legend and the cards table. Raw
  HTML/SVG is now a first-class page element, matching v1.
- The audit-S2 framing posture is unchanged on every real page. Only the same-origin content endpoint is framable,
  which is the standard posture for an app that renders its own content in iframes. No cross-origin framing is added.
- The content provider bypasses the `render_for` scope proof, so a unit test cannot assert a page's raw HTML by reading
  its page Markdown (the SVG is served from the content endpoint, not the page body); it asserts the bound `RawHtml` or
  the endpoint. The anon-view gating, not the text proof, keeps private data off public pages for a `Content` element.
- The DAG SVG is wider than its frame and scrolls horizontally inside the content iframe — the same behaviour as v1,
  owner-accepted (2026-11-05).
