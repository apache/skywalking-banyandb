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

import { request as undiciRequest } from 'undici';
import type { FastifyInstance } from 'fastify';
import { checkEndpointSafe, SsrfError } from '../lib/ssrf.js';
import { parsePrometheusText, sumOf, type Sample } from '../lib/prometheus.js';
import { requireMonitorRole } from '../lib/role.js';
import type { Config } from '../config.js';

// The response shape is MonitorSnapshot in canopy-shared; the server does not import shared code.
// The deadline covers reading the body too, so a target that streams forever cannot hold a request.
const SCRAPE_DEADLINE_MS = 5_000;
const MAX_METRICS_BYTES = 8 * 1024 * 1024;
// Viewers refreshing within this window share one scrape.
const REUSE_MS = 2_000;
const HISTORY_MS = 15 * 60_000;
// History keeps at most one sample per step, however many viewers poll.
const HISTORY_STEP_MS = 5_000;
const MAX_HISTORY = HISTORY_MS / HISTORY_STEP_MS + 1;
// Rates over a shorter window are mostly noise.
const MIN_WINDOW_MS = 1_000;

// The query panel counts executions by the data services. BydbQL is left out: a BydbQL
// query runs through one of these services too, so counting both would count it twice.
const QUERY_SERVICES = ['measure', 'stream', 'trace', 'property'];

// The counters behind the rates, summed the same way as the Workload dashboard
// in docs/operation/observability/metrics.md. BanyanDB registers the write, query
// and error counters on first use, so a missing one counts as 0, like the
// dashboard's `or vector(0)`.
interface Counters {
  readonly t: number;
  readonly cpuSeconds?: number;
  readonly written?: number;
  readonly queryStarted?: number;
  readonly queryFinished?: number;
  readonly queryLatencySeconds?: number;
  readonly errors?: number;
}

type CounterKey = Exclude<keyof Counters, 't'>;

function readCounters(samples: readonly Sample[], t: number): Counters {
  const total = (...names: string[]) => names.reduce((sum, name) => sum + (sumOf(samples, name) ?? 0), 0);
  const queries = (name: string) =>
    QUERY_SERVICES.reduce((sum, service) => sum + (sumOf(samples, name, { service, method: 'query' }) ?? 0), 0);
  return {
    t,
    cpuSeconds: sumOf(samples, 'process_cpu_seconds_total'),
    written: total('banyandb_measure_total_written', 'banyandb_stream_tst_total_written', 'banyandb_trace_tst_total_written'),
    queryStarted: queries('banyandb_liaison_grpc_total_started'),
    // Latency is added when a request finishes, so its average divides by finished requests.
    queryFinished: queries('banyandb_liaison_grpc_total_finished'),
    queryLatencySeconds: queries('banyandb_liaison_grpc_total_latency'),
    errors: total(
      'banyandb_liaison_grpc_total_err',
      'banyandb_liaison_grpc_total_registry_err',
      'banyandb_liaison_grpc_total_stream_msg_received_err',
      'banyandb_queue_pub_total_err',
    ),
  };
}

// A missing counter or one that went backwards (process restart) has no rate.
function delta(prev: Counters, cur: Counters, key: CounterKey): number | undefined {
  const a = prev[key];
  const b = cur[key];
  return a === undefined || b === undefined || b < a ? undefined : b - a;
}

function perSecond(prev: Counters | undefined, cur: Counters, key: CounterKey): number | undefined {
  if (!prev) return undefined;
  const d = delta(prev, cur, key);
  return d === undefined ? undefined : d / ((cur.t - prev.t) / 1000);
}

// history is oldest first and ends with cur, the counters read from samples.
function buildStandaloneNode(samples: readonly Sample[], history: readonly Counters[], id: string) {
  const cur = history[history.length - 1];
  const prev = history.slice(0, -1).reverse().find(h => cur.t - h.t >= MIN_WINDOW_MS);
  const finished = prev && delta(prev, cur, 'queryFinished');
  const latency = prev && delta(prev, cur, 'queryLatencySeconds');
  const errors = perSecond(prev, cur, 'errors');
  const paths = new Map<string, { used: number; total: number }>();
  for (const s of samples) {
    if (s.name !== 'banyandb_system_disk' || !s.labels.path) continue;
    const entry = paths.get(s.labels.path) ?? { used: 0, total: 0 };
    if (s.labels.kind === 'used') entry.used = s.value;
    if (s.labels.kind === 'total') entry.total = s.value;
    paths.set(s.labels.path, entry);
  }
  const cpuHistory: { t: number; v: number }[] = [];
  for (let i = 1; i < history.length; i++) {
    const v = perSecond(history[i - 1], history[i], 'cpuSeconds');
    if (v !== undefined) cpuHistory.push({ t: history[i].t, v });
  }
  return {
    id,
    role: 'standalone' as const,
    uptimeSeconds: sumOf(samples, 'banyandb_system_up_time'),
    cpuCores: sumOf(samples, 'banyandb_system_cpu_num'),
    cpuUsedCores: perSecond(prev, cur, 'cpuSeconds'),
    rssBytes: sumOf(samples, 'process_resident_memory_bytes'),
    memoryUsedRatio: sumOf(samples, 'banyandb_system_memory_state', { kind: 'used_percent' }),
    goroutines: sumOf(samples, 'go_goroutines'),
    heapInuseBytes: sumOf(samples, 'go_memstats_heap_inuse_bytes'),
    disk: [...paths].map(([path, d]) => ({ path, usedBytes: d.used, totalBytes: d.total })),
    write: { perSecond: perSecond(prev, cur, 'written') },
    query: {
      perSecond: perSecond(prev, cur, 'queryStarted'),
      avgLatencyMs: finished && latency !== undefined ? (latency / finished) * 1000 : undefined,
    },
    errorsPerMinute: errors === undefined ? undefined : errors * 60,
    cpuHistory,
  };
}

const PART_BYTES = {
  banyandb_measure_total_file_part_bytes: 'measure',
  banyandb_stream_tst_total_file_part_bytes: 'stream',
  banyandb_trace_tst_total_file_part_bytes: 'trace',
} as const;

function groupSizes(samples: readonly Sample[]) {
  const sizes = new Map<string, { name: string; catalog: 'measure' | 'stream' | 'trace'; bytes: number }>();
  for (const s of samples) {
    const catalog = PART_BYTES[s.name as keyof typeof PART_BYTES];
    if (!catalog || !s.labels.group) continue;
    const key = `${catalog}/${s.labels.group}`;
    const entry = sizes.get(key) ?? { name: s.labels.group, catalog, bytes: 0 };
    entry.bytes += s.value;
    sizes.set(key, entry);
  }
  return [...sizes.values()];
}

interface Failure {
  readonly status: number;
  readonly body: { readonly error: string; readonly message: string; readonly path: string };
}

const upstreamError = (message: string): Failure => ({ status: 502, body: { error: 'upstream_error', message, path: '/metrics' } });

async function scrape(config: Config): Promise<string | Failure> {
  try {
    await checkEndpointSafe(config.monitorTarget, config.blockRfc1918);
  } catch (err) {
    if (err instanceof SsrfError) return { status: 403, body: { error: 'ssrf_denied', message: err.message, path: '/metrics' } };
    throw err;
  }
  try {
    const res = await undiciRequest(`${config.monitorTarget}/metrics`, {
      method: 'GET',
      signal: AbortSignal.timeout(SCRAPE_DEADLINE_MS),
      reset: true,
    });
    if (res.statusCode !== 200) {
      await res.body.dump();
      return upstreamError(`GET /metrics returned ${res.statusCode}`);
    }
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of res.body) {
      const buf = Buffer.from(chunk as Uint8Array);
      size += buf.length;
      if (size > MAX_METRICS_BYTES) {
        res.body.destroy();
        return upstreamError(`GET /metrics returned more than ${MAX_METRICS_BYTES / 1024 / 1024} MiB`);
      }
      chunks.push(buf);
    }
    return Buffer.concat(chunks).toString('utf-8');
  } catch (err: unknown) {
    const code = (err as NodeJS.ErrnoException).code ?? (err as Error).name;
    return upstreamError(`GET /metrics failed with ${code}`);
  }
}

export async function registerMonitor(app: FastifyInstance, config: Config): Promise<void> {
  const history: Counters[] = [];
  const id = new URL(config.monitorTarget).host;
  let latest: { readonly at: number; readonly result: Promise<{ failure?: Failure; snapshot?: object }> } | undefined;

  const collect = async (): Promise<{ failure?: Failure; snapshot?: object }> => {
    const text = await scrape(config);
    if (typeof text !== 'string') return { failure: text };
    const samples = parsePrometheusText(text);
    // Every BanyanDB node registers its uptime, so a 200 without it is not a BanyanDB node.
    if (sumOf(samples, 'banyandb_system_up_time') === undefined) {
      return { failure: upstreamError('GET /metrics returned no BanyanDB metrics') };
    }
    const cur = readCounters(samples, Date.now());
    while (history.length && cur.t - history[0].t > HISTORY_MS) history.shift();
    const last = history[history.length - 1];
    if (!last || cur.t - last.t >= HISTORY_STEP_MS) history.push(cur);
    if (history.length > MAX_HISTORY) history.shift();
    return {
      snapshot: {
        mode: 'standalone' as const,
        source: id,
        scrapedAt: cur.t,
        nodes: [buildStandaloneNode(samples, history[history.length - 1] === cur ? history : [...history, cur], id)],
        groups: groupSizes(samples),
      },
    };
  };

  app.get('/monitor/snapshot', { preHandler: [app.canopyAuth.requireAuth, requireMonitorRole(config.monitorRoles)] }, async (_request, reply) => {
    // Concurrent and back-to-back viewers share the scrape that is running or just finished.
    if (!latest || Date.now() - latest.at > REUSE_MS) latest = { at: Date.now(), result: collect() };
    const { failure, snapshot } = await latest.result;
    return failure ? reply.status(failure.status).send(failure.body) : snapshot;
  });
}
