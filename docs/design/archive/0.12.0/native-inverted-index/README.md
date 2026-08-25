# Native Inverted Index Design

This folder preserves revision 0.2 of the 0.12.0 design package for replacing Bluge and ICE with a minimal BanyanDB-owned inverted index.

## Review entry points

1. [Implementation specification](index.html) — five linked chapters covering architecture, format, query/lifecycle, safety, and delivery.
2. [ICE segment walkthrough](ice-segment-animated-walkthrough.html) — animated write, filter, and sort explanations.
3. [Visual research report](research-report.html) — research findings, diagrams, compatibility risks, and implementation cutovers.
4. [Research plan](research-plan.md) — evidence and implementation-plan gates.
5. [Markdown research report](research-report.md) — source-level research detail.

## Layout

- `index.html`, `write-format.html`, `query-lifecycle.html`, `safety-verification.html`, and `delivery-review.html` form one split implementation specification.
- `spec.css` and `spec.js` are shared by those five pages.
- The former one-page revision 0.1 draft is preserved as [a monolith](native-inverted-index-spec-r0.1-monolith.html).
