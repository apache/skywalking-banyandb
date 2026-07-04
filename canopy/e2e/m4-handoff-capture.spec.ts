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

// Handoff comparison spec for M4.
// Boots the handoff static server on :18099 from .handoff-import/banyandb/project/
// and asserts each captured handoff screen matches a committed baseline via
// toHaveScreenshot(). These baselines ARE the visual reference set (per plan
// §"M4 Handoff design source" the static reference PNGs were deleted as expired);
// committing these PNGs gives the implementation side a stable target.
// The handoff bundle must be extracted at .handoff-import/banyandb/project/
// before this spec can run. If the directory is missing, the suite fails with
// the exact `unzip` instruction (no silent skip).

import { test, expect } from '@playwright/test';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import * as fs from 'node:fs';
import * as path from 'node:path';

const SHOTS_DIR = path.resolve(__dirname, 'screenshots', 'handoff');
const HANDOFF_DIR = path.resolve(__dirname, '..', '..', '.handoff-import', 'banyandb', 'project');
const HANDOFF_PORT = 18099;

// Fail loudly if the handoff bundle is missing — this is a precondition for
// the spec, not an optional environmental gate.
expect(fs.existsSync(HANDOFF_DIR), `Handoff bundle missing at ${HANDOFF_DIR}; run: unzip BanyanDB-handoff.zip -d .handoff-import/`).toBe(true);

let server: ChildProcessWithoutNullStreams | null = null;

test.beforeAll(async () => {
  fs.mkdirSync(SHOTS_DIR, { recursive: true });
  server = spawn('python3', ['-m', 'http.server', String(HANDOFF_PORT)], {
    cwd: HANDOFF_DIR,
    stdio: 'pipe',
  });
  await new Promise<void>((resolve) => setTimeout(resolve, 1500));
});

test.afterAll(async () => {
  if (server) server.kill('SIGTERM');
});

test.describe('M4 handoff pixel-regression baselines', () => {
  // Map each handoff route to a baseline name. The route captures the live
  // handoff render from :18099; the baseline is the committed PNG that the
  // implementation side compares against.
  const HREF_MAP: Array<readonly [string, string]> = [
    ['/index.html#query', 'handoff-query-builder'],
    ['/Measure Result Styles.html', 'handoff-result-measure'],
    ['/Stream Result Styles.html', 'handoff-result-stream'],
    ['/Trace Result Styles.html', 'handoff-result-trace'],
    // TopN styles are co-located with measure results in the handoff.
    ['/Measure Result Styles.html', 'handoff-result-topn'],
  ];

  for (const [route, name] of HREF_MAP) {
    test(`baseline ${name} from ${route}`, async ({ page }) => {
      await page.goto(`http://127.0.0.1:${HANDOFF_PORT}${route}`);
      await page.waitForLoadState('domcontentloaded');
      // Pixel-regression assertion: this PNG is BOTH the captured reference
      // AND the committed baseline. A future change to the handoff bundle
      // will fail the spec, forcing an explicit baseline update.
      await expect(page).toHaveScreenshot(`handoff/${name}.png`, {
        maxDiffPixelRatio: 0.01,
      });
    });
  }
});