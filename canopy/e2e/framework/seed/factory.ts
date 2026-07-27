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

// SeedFactory — programmatic, per-test data seeding via the BFF HTTP API,
// following the design doc's "Direct API Triggering (cy.request())" strategy:
// seed preconditions over HTTP instead of driving the browser GUI. Two
// properties keep it deterministic and isolated:
//
//   1. DYNAMIC NAMES. Every resource is named with `uniqueName(prefix)`, which
//      appends Date.now(), so parallel workers and re-runs never collide on a
//      unique constraint and never inherit a prior run's rows. This is the
//      programmatic-factory answer to schema drift + shared mutable state.
//   2. CLEANUP. Everything created is tracked and torn down in `cleanup()`,
//      wired to the `seed` fixture's teardown so each test leaves no footprint.
//      Resources are removed before their groups (BanyanDB rejects deleting a
//      group that still owns resources) and in reverse creation order.
//
// SCOPE NOTE: BanyanDB accepts *schema* writes over HTTP (group / measure /
// stream / trace / index-rule registries) but *data* writes only over streaming
// gRPC. This factory therefore covers schema-level seeding end-to-end; bulk demo
// DATA is seeded once per run by cmd/m4-seed (see setup/global-setup.ts and
// `seedDemoData()` below). A test-only HTTP data-seed endpoint remains the
// tracked follow-up (TESTING.md §8) that would let this factory seed rows
// directly too — it is deferred because the Fastify BFF has no gRPC client and
// BanyanDB writes are gRPC-only.

import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import type { APIRequestContext } from '@playwright/test';

export type Catalog = 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE' | 'CATALOG_PROPERTY';
export type IndexRuleType = 'TYPE_TREE' | 'TYPE_INVERTED';

// A schema resource scheduled for teardown; the DELETE path is stored verbatim.
interface TrackedResource {
  readonly path: string;
}

export class SeedFactory {
  private readonly createdGroups: string[] = [];
  private readonly createdResources: TrackedResource[] = [];
  // Per-instance counter so back-to-back calls within the same millisecond
  // still produce distinct names (Date.now() alone collides in tight async
  // seed sequences, which would flake the suite).
  private seq = 0;

  constructor(private readonly request: APIRequestContext) {}

  // Append a monotonic suffix so names are unique across workers and re-runs.
  uniqueName(prefix: string): string {
    return `${prefix}-${Date.now()}-${this.seq++}`;
  }

  // Register an externally-created group (e.g. one created through the UI under
  // test) for automatic teardown by the `seed` fixture.
  trackGroup(name: string): void {
    this.createdGroups.push(name);
  }

  // Register an externally-created resource by its DELETE path for teardown.
  trackResource(path: string): void {
    this.createdResources.push({ path });
  }

  // Create a group via the BFF and register it for cleanup. Returns the name.
  async createGroup(prefix: string, catalog: Catalog = 'CATALOG_MEASURE'): Promise<string> {
    const name = this.uniqueName(prefix);
    const res = await this.request.post('/api/v1/group/schema', {
      data: {
        group: {
          metadata: { name },
          catalog,
          resourceOpts: {
            shardNum: 1,
            segmentInterval: { unit: 'UNIT_DAY', num: 1 },
            ttl: { unit: 'UNIT_DAY', num: 7 },
          },
        },
      },
    });
    if (!res.ok()) {
      throw new Error(`seed createGroup(${name}) failed: ${res.status()} ${await res.text()}`);
    }
    this.createdGroups.push(name);
    return name;
  }

  // Create a minimal measure in `group`. Returns the measure name.
  async createMeasure(group: string, prefix = 'm', fields: readonly string[] = ['value']): Promise<string> {
    const name = this.uniqueName(prefix);
    const res = await this.request.post('/api/v1/measure/schema', {
      data: {
        measure: {
          metadata: { group, name },
          entity: { tagNames: ['id'] },
          tagFamilies: [{ name: 'default', tags: [{ name: 'id', type: 'TAG_TYPE_STRING' }] }],
          fields: fields.map((f) => ({
            name: f,
            fieldType: 'FIELD_TYPE_INT',
            encodingMethod: 'ENCODING_METHOD_GORILLA',
            compressionMethod: 'COMPRESSION_METHOD_ZSTD',
          })),
          interval: '1m',
        },
      },
    });
    if (!res.ok()) {
      throw new Error(`seed createMeasure(${group}/${name}) failed: ${res.status()} ${await res.text()}`);
    }
    this.createdResources.push({ path: `/api/v1/measure/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}` });
    return name;
  }

  // Create a minimal stream in `group` with a single `host` string tag as the
  // entity. Returns the stream name.
  async createStream(group: string, prefix = 's', entityTag = 'host'): Promise<string> {
    const name = this.uniqueName(prefix);
    const res = await this.request.post('/api/v1/stream/schema', {
      data: {
        stream: {
          metadata: { name, group },
          tagFamilies: [{ name: 'default', tags: [{ name: entityTag, type: 'TAG_TYPE_STRING' }] }],
          entity: { tagNames: [entityTag] },
        },
      },
    });
    if (!res.ok()) {
      throw new Error(`seed createStream(${group}/${name}) failed: ${res.status()} ${await res.text()}`);
    }
    this.createdResources.push({ path: `/api/v1/stream/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}` });
    return name;
  }

  // Create an index rule over `tags` in `group`. Returns the rule name.
  async createIndexRule(group: string, tags: readonly string[], prefix = 'rule', type: IndexRuleType = 'TYPE_TREE'): Promise<string> {
    const name = this.uniqueName(prefix);
    const res = await this.request.post('/api/v1/index-rule/schema', {
      data: { indexRule: { metadata: { name, group }, tags: [...tags], type } },
    });
    if (!res.ok()) {
      throw new Error(`seed createIndexRule(${group}/${name}) failed: ${res.status()} ${await res.text()}`);
    }
    this.createdResources.push({ path: `/api/v1/index-rule/schema/${encodeURIComponent(group)}/${encodeURIComponent(name)}` });
    return name;
  }

  // seedDemoData — DEFERRED HTTP data-seed backdoor (TESTING.md §8).
  //
  // BanyanDB accepts DATA writes only over streaming gRPC, and the Fastify BFF
  // is a pure HTTP proxy with no gRPC client, so there is no clean, prod-safe
  // HTTP endpoint to seed rows through. Rather than pull an invasive gRPC stack
  // into the BFF, this method wraps the existing native seeder (cmd/m4-seed),
  // which is the same binary the review workflow already uses for bulk demo
  // data. It is a best-effort convenience for local runs; CI should invoke the
  // seeder once per suite out of band (see TESTING.md §9).
  //
  // Returns true when the seeder ran successfully, false otherwise (missing Go
  // toolchain, unreachable BanyanDB, etc.). Callers that need real rows should
  // treat a false return as "demo data unavailable" and skip data-dependent
  // assertions rather than failing the run.
  seedDemoData(banyandbGrpcAddr = process.env.BANYANDB_GRPC || '127.0.0.1:17912'): boolean {
    // e2e specs run with cwd = canopy/ (npm run e2e); the repo root that holds
    // cmd/m4-seed is its parent.
    const repoRoot = resolve(process.cwd(), '..');
    const run = spawnSync('go', ['run', './cmd/m4-seed', '-addr', banyandbGrpcAddr], {
      cwd: repoRoot,
      stdio: 'inherit',
      timeout: 300_000,
    });
    return run.status === 0;
  }

  // Tear down everything this factory created. Best-effort: a failed delete is
  // logged, not thrown, so one bad teardown never masks the test's own result.
  async cleanup(): Promise<void> {
    // Resources first (reverse creation order), then their groups.
    for (const { path } of this.createdResources.reverse()) {
      const res = await this.request.delete(path);
      if (!res.ok() && res.status() !== 404) {
        // eslint-disable-next-line no-console
        console.warn(`[seed] cleanup: delete ${path} → ${res.status()}`);
      }
    }
    this.createdResources.length = 0;
    for (const name of this.createdGroups.reverse()) {
      const res = await this.request.delete(`/api/v1/group/schema/${encodeURIComponent(name)}`);
      if (!res.ok() && res.status() !== 404) {
        // eslint-disable-next-line no-console
        console.warn(`[seed] cleanup: delete group ${name} → ${res.status()}`);
      }
    }
    this.createdGroups.length = 0;
  }
}
