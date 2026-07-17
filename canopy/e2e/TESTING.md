<!--
  Licensed to Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for
  additional information regarding copyright ownership. Apache Software
  Foundation (ASF) licenses this file to you under the Apache License,
  Version 2.0 (the "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations
  under the License.
-->

# Canopy Testing Framework — Redesign

This document is the architecture for canopy's test suite. It maps the
principles in *"Architectural Principles of Frontend End-to-End Testing"* onto
canopy's stack (React SPA + Fastify BFF + a real BanyanDB backend) and defines
the target structure, the patterns every test must follow, and the phased
migration that gets us there without dropping coverage.

## 1. Testing pyramid — where each layer lives

| Layer | Tool | Location | What it covers | Speed |
|---|---|---|---|---|
| Unit / component | Vitest + Testing Library | `web/src/**/*.test.tsx`, `shared` | component logic, flatteners, BydbQL codegen, role inference | ms |
| BFF integration | Vitest | `server/test/*.test.ts` | proxy, auth envelopes, static serving | ms–s |
| **E2E (this dir)** | Playwright | `e2e/tests/**` | system-wide journeys through the real BFF + real BanyanDB | s–min |
| Visual regression | Playwright `toHaveScreenshot` | `e2e/tests/**` (scoped captures) | layout/styling of **stable** regions | s |

The base is broad (≈350 unit/component + BFF tests today) and healthy. E2E is
the **narrow capstone** — reserved for high-value journeys (query console,
schema CRUD, auth), never for exhaustively re-testing logic the unit layer
already covers.

## 2. Gap analysis (legacy → redesign)

| Principle | Legacy state | Redesign |
|---|---|---|
| **Semantic locators** | CSS classes (`.qb-card`, `button.qb-btn-primary`, `.rv-root`, `input[name=…]`) | `getByRole`/`getByLabel` only; CSS/XPath banned. Missing handles → add an ARIA landmark (done for the builder + results regions), `data-testid` as the last-resort escape hatch. |
| **Web-first assertions** | `waitForTimeout(1000)`, `waitForSelector(…, 30_000)` | `await expect(locator).toBeVisible()/toBeEnabled()`; auto-retrying, per-assertion timeouts. **No hard sleeps.** |
| **Isolation & secrets** | 10 ad-hoc `.mjs` probes with a hard-coded session cookie + `localhost:4000` | Deleted during migration. All setup via fixtures + the API seed factory; no secrets in repo. |
| **Auth** | UI login in `beforeEach` of every m4 test | Login **once** in a `setup` project → `storageState`, injected everywhere. CI keeps `CANOPY_DEV_NOAUTH` for the fast path; a dedicated auth suite validates the login form. |
| **Structure** | Inline `page.locator`, copy-pasted `login()` | Page Object Model + Playwright fixtures (dependency injection). Specs express intent, never selectors. |
| **Seeding** | Manual `cmd/m4-seed` Go binary, out of band | `SeedFactory` (HTTP, dynamic names, auto-cleanup) for schema preconditions; `cmd/m4-seed` invoked once per run for bulk demo data. |
| **VRT** | `screenshot: 'on'` global, local baselines, 3 configs | Animations disabled + caret hidden + CSS scale globally; captures scoped to **stable** regions; baselines generated in CI; masks for dynamic content. |
| **Organization / CI** | Milestone names (m0–m4), one project, no tags | Domain + speed tags (`@e2e @query @seed`), tiered gates, sharding-ready. |

What we **keep**: `setup/global-setup.ts` already spins up a **real, isolated**
BanyanDB standalone in a fresh temp data dir per run (strong instance-level
isolation) — the redesign reuses it verbatim.

## 3. Directory layout

```
e2e/
  framework/
    fixtures.ts            # base.extend → injects POMs + seed factory (import this, not @playwright/test)
    paths.ts               # shared constants (STORAGE_STATE) — no test() calls, config-safe
    pages/
      BasePage.ts          # POM root
      LoginPage.ts         # the ONLY UI driver of the login form
      ShellPage.ts         # app shell + Home (sidebar, nav, connection, Home)
      SchemaPage.ts        # metadata CRUD (overview / group / detail + forms)
      IndexRulePage.ts     # IndexPage Rule/Binding tabs + index-rule/binding modals
      LifecyclePage.ts     # lifecycle-stage editor inside the group modal
      QueryConsolePage.ts  # /query console + result views
    seed/
      factory.ts           # SeedFactory: HTTP schema seeding, dynamic names, cleanup, seedDemoData()
  auth/
    auth.setup.ts          # login-once → storageState (the `setup` project)
  tests/
    boot.spec.ts           # BFF health + SPA boot + shell-on-boot   (@boot @smoke)
    auth.spec.ts           # the real login form (empty session)     (@auth)
    shell.spec.ts          # sidebar / nav / Home / collapse          (@shell)
    schema.spec.ts         # group/measure/stream/trace CRUD + paging (@schema @seed)
    index-rule.spec.ts     # IndexRule/Binding API + UI CRUD          (@index-rule @seed)
    lifecycle.spec.ts      # lifecycle-stage editor                   (@lifecycle @seed)
    query.spec.ts          # query console + result views + chrome VRT (@query @vrt)
    query.spec.ts-snapshots/ # committed chrome VRT baselines (CI-generated, Linux)
  setup/                   # global-setup/teardown (reused BanyanDB bring-up) — unchanged
  playwright.config.ts     # the single canonical config (redesigned; end state)
```

The milestone-numbered `m*.spec.ts` files, the 11 ad-hoc `.mjs`/`.spec.ts`
probes, the handoff design-comparison harness (`m3-handoff.spec.ts`,
`handoff-server.cjs`), and the `playwright.{legacy,handoff,reuse}.config.ts`
variants were all removed at cutover — see §8.

## 4. Patterns (every new spec follows these)

### Fixtures + POM
Specs import `{ test, expect }` from `framework/fixtures.js` and consume
injected objects — no `new`, no `page.locator`:

```ts
import { test, expect } from '../framework/fixtures.js';

test('runs a query', async ({ consolePage }) => {
  await consolePage.goto();
  await consolePage.selectCatalog('Measure');
  await consolePage.run();
  await consolePage.expectResultsVisible();
});
```

### Semantic locators (priority order)
`getByRole` → `getByLabel` → `getByPlaceholder` → `getByText` → `getByAltText`
→ `getByTitle` → `getByTestId`. Canopy already exposes rich ARIA (tablists for
mode/target-type, `aria-label`ed selects, a role=radiogroup login). If a target
has no handle, **add a landmark or `data-testid`** — a complex CSS/XPath
selector is a signal to fix the markup, not the test. (This redesign added
`role="region"` landmarks for the query builder and results panels.)

### Web-first assertions
Never read an instantaneous boolean. Use auto-retrying assertions and let the
Run button's disabled-while-running state be the settle signal:

```ts
await this.runButton().click();
await expect(this.runButton()).toBeEnabled({ timeout: 60_000 }); // NOT waitForTimeout
```

### Programmatic seeding
Seed preconditions over HTTP via `SeedFactory`, with dynamic names so parallel
workers never collide and re-runs never inherit stale rows; teardown is
automatic via the `seed` fixture:

```ts
test('seeded group shows up', async ({ consolePage, seed }) => {
  const group = await seed.createGroup('e2e-measure'); // e2e-measure-1737059…
  await consolePage.goto();
  await consolePage.selectCatalog('Measure');
  await expect(consolePage.groupSelect()).toContainText(group);
});
```

> **Backend constraint + how demo data is seeded.** BanyanDB accepts **schema**
> writes over HTTP (group / measure / stream / trace / index-rule registries —
> proxied by the BFF) but **data** writes only over streaming gRPC. So
> `SeedFactory` covers schema end-to-end, and every schema-only spec seeds its
> own preconditions through it.
>
> **Bulk demo data is now seeded automatically by `global-setup`**: after the
> isolated BanyanDB is ready it runs `cmd/m4-seed` (the native gRPC seeder) to
> create the known demo resources + rows — `sw_metric` (`service_traffic`,
> `service_cpm_minute`, `top_service`), `m4-stream/service_logs`,
> `m4-traces/service_spans`. The query-console specs select those resources by
> name so runs are deterministic (measure uses `service_cpm_minute`, whose
> `entity_id`+`total` schema matches the builder's default SELECT). Set
> `E2E_SKIP_SEED=1` for a schema-only run. `SeedFactory.seedDemoData()` wraps
> the same binary for ad-hoc use.
>
> A test-only HTTP data-seed *endpoint* was **assessed and deferred**: the
> Fastify BFF is a pure HTTP proxy with no gRPC client, so a clean prod-safe
> route would require pulling an invasive gRPC stack (client + generated write
> stubs) into the BFF — not worth it now that `global-setup` seeds via the
> native binary. Data-dependent result assertions (Top-N
> leaderboard shape, trace decode/proto-bind, decoded entity ids) that the old
> `.mjs` probes exercised live in the **web unit suite** instead
> (`query/results/result-views.test.tsx`, `query/bydbql.test.ts`,
> `query/proto-decoder.test.ts`) — deterministic, no live rows required.

## 5. Authentication

- **CI fast path (default):** the BFF runs with `CANOPY_DEV_NOAUTH=true`, so the
  main suite never pays for login.
- **storageState:** the `setup` project (`auth/auth.setup.ts`) logs in once and
  writes `e2e/.auth/user.json`; the `chromium` project loads it via
  `storageState` (project dependency guarantees ordering). The file is
  gitignored — it carries a live session token.
- **Login form itself:** validated by a dedicated auth suite (added during
  migration) against an auth-enabled server.
- **MFA/OTP (future):** if staging enforces it, expose an env-gated test-backdoor
  endpoint to fetch/reset the current OTP — never reachable in production,
  guarded by an internal secret header.

## 6. Visual regression discipline

Enforced globally in `playwright.config.ts`:
- `animations: 'disabled'`, `caret: 'hide'`, `scale: 'css'` on every screenshot.
- `screenshot: 'only-on-failure'` (the pixel gate is the **explicit**
  `toHaveScreenshot`, not the global capture).
- **Scope captures to stable regions.** The `@vrt` block captures only the
  static chrome regions (sidebar, page-header, empty-result panel) via `clip`.
  Data-driven result views are checked *functionally*, never by pixels: the
  demo seed uses fixed RNG seeds (deterministic values) but `time.Now()`-based
  timestamps, so result rows/labels — and a chart's time axis — shift every
  run and cannot be cleanly masked.

### Baselines are committed; regenerate in CI

Baselines live at `e2e/tests/query.spec.ts-snapshots/*-chromium-linux.png` and
are **committed** (the gate diffs against them — see §"Screenshots" in
`.gitignore`, which ignores transient shots but keeps these). They are
environment-specific (OS + font rendering), so they must be generated in the
**same container the gate runs in** to avoid false diffs:

```bash
# In the CI Linux container (or a matching local env), then commit the PNGs:
npm run e2e:update-baselines     # playwright ... --grep @vrt --update-snapshots
npm run e2e:vrt                  # verify they pass without --update
```

Because `--update-snapshots` writes whatever renders, a maintainer must
**eyeball the diff** in the PR before committing regenerated baselines. If they
outgrow the repo, move them to Git LFS.

## 7. CI quality gates (tiered)

### Tag taxonomy

Every `test.describe` title carries `@e2e` plus one **domain** tag and, where
applicable, **speed/kind** tags, so pipelines can run the right subset with
`--grep`:

| Kind | Tags |
|---|---|
| Base | `@e2e` (every suite) |
| Domain | `@boot`, `@auth`, `@shell`, `@schema`, `@index-rule`, `@lifecycle`, `@query` |
| Speed / kind | `@smoke` (fast sanity), `@seed` (seeds schema via the factory), `@vrt` (pixel gate) |

Tag by speed + domain so pipelines run the right subset:

```
PR:        unit + BFF (vitest)  →  component/VRT of changed areas  →  E2E smoke (--grep @smoke)
Post-merge / nightly:            full E2E suite, sharded across workers
```

- **Sharding:** `playwright test --shard=1/4` across workers; `fullyParallel`.
- **Determinism:** pin `TZ`/`LANG`, identical container runtimes.
- **Flake policy:** `retries: 2` in CI; quarantine + triage repeat offenders
  rather than blanket-retrying.
- **Diagnostics:** `trace: on-first-retry`, `video: retain-on-failure`, HTML report.

## 8. Migration roadmap — COMPLETE

The migration is done; this section is the record of what shipped.

1. **Scaffold:** framework/, auth setup, seed factory, redesigned config, one
   reference spec, builder/results landmarks. ✅
2. **Ported specs by domain** (each with a POM + semantic locators):
   - `m0-boot` + `m1-bff` → `tests/boot.spec.ts`
   - `m2-login` → `tests/auth.spec.ts` (real login form, empty session; does
     **not** rely on the `CANOPY_DEV_NOAUTH` bypass)
   - `m2-shell` → `tests/shell.spec.ts` (+ `ShellPage`)
   - `m3-crud` + `m3-metadata` → `tests/schema.spec.ts` (+ `SchemaPage`)
   - `m3-indexrule` + `m3-indexrule-ui` → `tests/index-rule.spec.ts` (+ `IndexRulePage`)
   - `m3-lifecycle` → `tests/lifecycle.spec.ts` (+ `LifecyclePage`)
   - `m4-query` + `m4-query-chrome` → `tests/query.spec.ts` (chrome VRT baselines
     migrated verbatim into `tests/query.spec.ts-snapshots/`)
   To make the semantic-locator port possible, the app markup gained landmarks:
   `role="dialog"` on every CRUD modal, `aria-pressed` on the catalog/segment and
   binding-scope toggles, `aria-label`ed lifecycle-stage inputs and a `role="group"`
   per stage card, a `role="tablist"` on the Index tabs, a `<main>`/`aria-label`ed
   sidebar/connection landmark in the shell, and a small set of `data-testid`s for
   div-based rows/chips (`res-row`, `idx-rule-row`, `idx-bind-row`, `meta-*`,
   `pager-page`). No CSS-class or XPath selectors remain in the suite.
3. **Deleted the `.mjs` probes** (hard-coded session cookie + `localhost:4000`).
   Their unique *interaction* checks (WHERE add/edit, builder⇄code toggle) moved
   into `tests/query.spec.ts`; their *data-dependent* checks are covered by the
   web unit suite (see §4). Removed with them: `observe-query.spec.ts`,
   `trace-verify.spec.ts`.
4. **Data seeding — DONE.** `global-setup` now seeds bulk demo data via
   `cmd/m4-seed` after the isolated BanyanDB starts (see §4), so the query-console
   specs run deterministically against known resources+rows. (A native HTTP
   data-seed BFF endpoint was assessed and deferred — the BFF has no gRPC client
   and BanyanDB writes are gRPC-only; not worth the invasive gRPC stack.)
5. **Cutover:** `playwright.next.config.ts` → `playwright.config.ts`; the legacy,
   handoff, and reuse configs plus the handoff harness were removed. Single-config
   end state; `npm run e2e` points at it. ✅

**Removed as obsolete:** the handoff design-comparison suites
(`m3-handoff.spec.ts`, `m4-handoff-capture.spec.ts`, `handoff-server.cjs`,
`playwright.handoff.config.ts`). They compared a *static, expired* handoff design
bundle (`.handoff-import/…`, not in the repo) rather than the live app, spawned
their own Python/Node server outside the single-config webServer model, and were
M3/M4-era design DoD gates whose purpose has passed. Their baselines were removed
with them.

## 9. Commands

```bash
npm run e2e                           # full suite (builds first via pree2e)
npm run e2e -- --grep @smoke          # PR smoke subset
npm run e2e -- --grep @schema         # one domain
npm run e2e -- --grep-invert @vrt     # skip the pixel gate
npm run e2e -- --ui                   # Playwright UI mode
npm run e2e -- --list                 # discover + transpile-check every spec
npm run e2e:vrt                       # run only the VRT pixel gate
npm run e2e:update-baselines          # regenerate VRT baselines (run in CI container, then commit)

# Demo DATA (optional; only needed for data-bound manual exploration). Runs the
# native gRPC seeder — CI does this once, out of band, before the suite:
go run ./cmd/m4-seed -addr 127.0.0.1:17912
```
