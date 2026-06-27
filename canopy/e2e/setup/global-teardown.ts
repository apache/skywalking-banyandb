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

import { rmSync, readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

export default async function globalTeardown() {
  const stateFile = join(tmpdir(), 'canopy-e2e-state.json');
  if (!existsSync(stateFile)) return;

  let state: { banyandbPid?: number; dataDir?: string } = {};
  try {
    state = JSON.parse(readFileSync(stateFile, 'utf-8'));
  } catch {
    // ignore parse errors
  }

  if (state.banyandbPid) {
    console.log(`[e2e] Stopping BanyanDB process (pid ${state.banyandbPid})`);
    try {
      process.kill(state.banyandbPid, 'SIGTERM');
    } catch {
      // process may have already exited
    }
  }

  if (state.dataDir) {
    try {
      rmSync(state.dataDir, { recursive: true, force: true });
    } catch {
      // best-effort cleanup
    }
  }

  console.log('[e2e] E2E teardown complete');
}
