/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// query.spec.ts — the M4 Query console + result views. Consolidates the legacy
// m4-query.spec.ts (run every catalog), m4-query-chrome.spec.ts (pixel VRT of
// the STABLE chrome regions), and the unique interaction checks that used to
// live in the ad-hoc .mjs probes (WHERE editing, builder⇄code toggle). Every
// principle is applied: fixtures inject the POM + seed factory, semantic
// locators only, web-first assertions, dynamic-name API seeding, and VRT scoped
// to stable chrome (data-bound results are checked functionally, not by pixels).

import { test, expect } from '../framework/fixtures.js';
import type { Catalog } from '../framework/pages/QueryConsolePage.js';

test.describe('query console @e2e @query', () => {
  test.beforeEach(async ({ consolePage }) => {
    await consolePage.goto();
  });

  // Resources seeded by cmd/m4-seed in global-setup (see e2e/setup/global-setup.ts).
  // Selecting a known group+resource makes the run deterministic; the default
  // builder state would otherwise pick an arbitrary resource that may not exist.
  const SEEDED: Record<Catalog, { group: string; resource: string }> = {
    // service_cpm_minute has the entity_id tag + total field that the builder's
    // default SELECT (entity_id · mean(total)) expects, so the default query is
    // valid without reconfiguring SELECT.
    Measure: { group: 'sw_metric', resource: 'service_cpm_minute' },
    Stream: { group: 'm4-stream', resource: 'service_logs' },
    Trace: { group: 'm4-traces', resource: 'service_spans' },
    'Top-N': { group: 'sw_metric', resource: 'top_service' },
  };
  for (const catalog of Object.keys(SEEDED) as Catalog[]) {
    test(`runs a ${catalog} query and renders the result region`, async ({ consolePage }) => {
      await consolePage.selectCatalog(catalog);
      await consolePage.selectGroup(SEEDED[catalog].group);
      await consolePage.selectResource(SEEDED[catalog].resource);
      await consolePage.run();
      // Web-first: the results region is a semantic landmark that appears once
      // the run settles — no sleep, no class selector. (Traces auto-default
      // ORDER BY time, satisfying the trace-query validation.)
      await consolePage.expectResultsVisible();
    });
  }

  test('a programmatically seeded group appears in the builder @seed', async ({ consolePage, seed }) => {
    // Seed a precondition over HTTP (dynamic name → no collision, auto-cleanup)
    // instead of driving the UI to create it.
    const group = await seed.createGroup('e2e-measure');
    // The builder reads groups on load; reload so the new group is fetched.
    await consolePage.goto();
    await consolePage.selectCatalog('Measure');
    await expect(consolePage.groupSelect()).toContainText(group);
  });

  // Folded from observe-query.mjs / verify-query-regressions.mjs: a WHERE
  // condition can be added and its value edited. This exercises the builder
  // clause editing path without depending on any seeded rows.
  test('a WHERE condition can be added and its value edited', async ({ consolePage }) => {
    await consolePage.selectCatalog('Trace');
    // A resource must be selected so its tags populate — "Add condition" is
    // disabled otherwise.
    await consolePage.selectGroup('m4-traces');
    await consolePage.selectResource('service_spans');
    await consolePage.addWhereCondition();
    await consolePage.setConditionValue('sw-demo-trace-001');
    await expect(consolePage.conditionValue()).toHaveValue('sw-demo-trace-001');
    await consolePage.setConditionValue('payment');
    await expect(consolePage.conditionValue()).toHaveValue('payment');
  });

  // Folded from topn-bydbql-roundtrip.mjs / verify-query-regressions.mjs: the
  // builder⇄code mode toggle works (BydbQL codegen correctness itself is
  // covered by the web unit suite — query/bydbql.test.ts).
  test('toggles between builder and code mode', async ({ consolePage }) => {
    await consolePage.switchToCode();
    await consolePage.switchToBuilder();
    await expect(consolePage.builder()).toBeVisible();
  });
});

test.describe('query console visual regression @e2e @query @vrt', () => {
  // Pixel-regression gate for the STABLE chrome regions (sidebar, page header,
  // empty-result panel). The data-bound builder rail is intentionally excluded —
  // its contents reflect the live BanyanDB group/resource list, which has no
  // fixed canonical seed. Baselines migrated verbatim from the legacy
  // m4-query-chrome.spec.ts-snapshots (committed, CI-generated for Linux).
  const REGIONS = [
    { name: 'sidebar', clip: { x: 0, y: 0, width: 256, height: 720 } },
    { name: 'page-header', clip: { x: 256, y: 0, width: 1184, height: 110 } },
    { name: 'empty-result', clip: { x: 624, y: 110, width: 816, height: 200 } },
  ] as const;

  test('chrome regions match the committed baselines', async ({ consolePage, page }) => {
    await consolePage.goto();
    // Canonical fresh-boot capture: clear any user-persisted builder state and
    // reload so defaultState() runs, then wait web-first for the builder to be
    // present (the excluded rail may still be settling — it is not captured).
    await page.evaluate(() => localStorage.clear()).catch(() => {});
    await page.reload();
    await expect(consolePage.builder()).toBeVisible();

    for (const r of REGIONS) {
      // Flat snapshot name (matches the migrated baseline filenames verbatim).
      await expect(page).toHaveScreenshot(`m4-chrome-${r.name}.png`, {
        clip: r.clip,
        maxDiffPixelRatio: 0.01,
      });
    }
  });
});
