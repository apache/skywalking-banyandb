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

import { execSync, spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

const HTTP_PORT = parseInt(process.env.E2E_BANYANDB_HTTP_PORT || '17913', 10);
const GRPC_PORT = parseInt(process.env.E2E_BANYANDB_GRPC_PORT || '17912', 10);
const BANYANDB_TARGET = `http://127.0.0.1:${HTTP_PORT}`;

// Repo root: canopy/e2e/setup/ → ../../../
const REPO_ROOT = resolve(__dirname, '..', '..', '..');

// Generated proto files (.pb.go) are not committed; they live only in the primary
// worktree where `make generate` was last run. Build from there — the server code
// is identical across branches (canopy adds no Go server changes).
// Override with E2E_BANYANDB_SRC for CI or non-standard setups.
function findBuildRoot(): string {
  if (process.env.E2E_BANYANDB_SRC) return process.env.E2E_BANYANDB_SRC;
  const out = execSync('git worktree list', { cwd: REPO_ROOT, encoding: 'utf-8' });
  return out.split('\n')[0].trim().split(/\s+/)[0];
}

async function waitForReady(url: string, maxAttempts = 60): Promise<void> {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const res = await fetch(`${url}/api/healthz`);
      if (res.ok) {
        // Extra wait for internal services (property node registry) to register after healthz.
        await sleep(3000);
        return;
      }
    } catch {
      // not ready yet
    }
    await sleep(1000);
  }
  throw new Error(`BanyanDB at ${url} did not become ready after ${maxAttempts}s`);
}

// Seed deterministic demo data (groups + measures/streams/traces/top-N + rows)
// so the query-console specs have known resources to select and run against.
// BanyanDB data writes are gRPC-only, so this uses the native seeder
// (cmd/m4-seed) rather than the HTTP BFF. Idempotent (the seeder ignores
// already-exists) and skippable with E2E_SKIP_SEED for schema-only runs.
function seedDemoData(): void {
  if (process.env.E2E_SKIP_SEED) {
    console.log('[e2e] E2E_SKIP_SEED set — skipping demo data seed');
    return;
  }
  console.log(`[e2e] Seeding demo data via cmd/m4-seed → 127.0.0.1:${GRPC_PORT}`);
  execSync(`go run ./cmd/m4-seed --addr 127.0.0.1:${GRPC_PORT} --rows 80`, {
    cwd: REPO_ROOT,
    stdio: 'inherit',
    timeout: 180_000,
  });
  console.log('[e2e] Demo data seeded');
}

export default async function globalSetup() {
  const buildRoot = findBuildRoot();
  const binaryPath = join(tmpdir(), 'banyand-e2e');
  // Each E2E run gets its own isolated data directory so state from a prior
  // run (leftover groups, measures, index rules, etc.) cannot leak in. The
  // directory is named with a UTC timestamp + PID for easy debugging and is
  // deleted unconditionally in global-teardown.
  const runStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const dataDir = mkdtempSync(join(tmpdir(), `canopy-e2e-data-${runStamp}-`));
  console.log(`[e2e] Isolated data directory: ${dataDir}`);

  console.log(`[e2e] Building BanyanDB from source at ${buildRoot}…`);
  execSync(`go build -o ${binaryPath} ./banyand/cmd/server`, {
    cwd: buildRoot,
    stdio: 'inherit',
    timeout: 300_000,
  });
  console.log('[e2e] Build complete');

  console.log('[e2e] Starting BanyanDB standalone');
  const proc = spawn(binaryPath, [
    'standalone',
    `--stream-root-path=${dataDir}`,
    `--measure-root-path=${dataDir}`,
    `--property-root-path=${dataDir}`,
    `--trace-root-path=${dataDir}`,
    `--http-port=${HTTP_PORT}`,
    `--grpc-port=${GRPC_PORT}`,
  ], { stdio: 'pipe' });

  proc.stderr?.on('data', (chunk: Buffer) => {
    if (process.env.E2E_BANYANDB_VERBOSE) process.stderr.write(chunk);
  });

  process.env.BANYANDB_TARGET = BANYANDB_TARGET;

  console.log(`[e2e] Waiting for BanyanDB to be ready at ${BANYANDB_TARGET}`);
  await waitForReady(BANYANDB_TARGET);
  console.log('[e2e] BanyanDB ready');

  seedDemoData();

  writeFileSync(join(tmpdir(), 'canopy-e2e-state.json'), JSON.stringify({
    banyandbTarget: BANYANDB_TARGET,
    banyandbPid: proc.pid,
    binaryPath,
    dataDir,
  }));
}
