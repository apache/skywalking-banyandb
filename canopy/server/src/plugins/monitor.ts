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
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { checkEndpointSafe, SsrfError } from '../lib/ssrf.js';
import { parsePrometheusText, sumOf, type Sample } from '../lib/prometheus.js';
import type { Role } from '../lib/role.js';
import type { Config } from '../config.js';

// The response shape is MonitorSnapshot in canopy-shared; the server does not import shared code.
const SCRAPE_TIMEOUT_MS = 5_000;
const HISTORY_MS = 15 * 60_000;
// Rates over a shorter window are mostly noise, e.g. two tabs refreshing at once.
const MIN_WINDOW_MS = 1_000;

// The counters behind the rates, summed the same way as the Workload dashboard
// in docs/operation/observability/metrics.md. BanyanDB registers the write, query
// and error counters on first use, so a missing one counts as 0, like the
// dashboard's `or vector(0)`.
interface Counters {
  readonly t: number;
  readonly cpuSeconds?: number;
  readonly written?: number;
  readonly queryStarted?: number;
  readonly queryLatencySeconds?: number;
  readonly errors?: number;
}

function readCounters(samples: readonly Sample[], t: number): Counters {
  const total = (...names: string[]) => names.reduce((sum, name) => sum + (sumOf(samples, name) ?? 0), 0);
  return {
    t,
    cpuSeconds: sumOf(samples, 'process_cpu_seconds_total'),
    written: total('banyandb_measure_total_written', 'banyandb_stream_tst_total_written', 'banyandb_trace_tst_total_written'),
    queryStarted: sumOf(samples, 'banyandb_liaison_grpc_total_started', { method: 'query' }) ?? 0,
    queryLatencySeconds: sumOf(samples, 'banyandb_liaison_grpc_total_latency', { method: 'query' }) ?? 0,
    errors: total(
      'banyandb_liaison_grpc_total_err',
      'banyandb_liaison_grpc_total_registry_err',
      'banyandb_liaison_grpc_total_stream_msg_received_err',
      'banyandb_queue_pub_total_err',
    ),
  };
}

// A missing counter or one that went backwards (process restart) has no rate.
function delta(prev: Counters, cur: Counters, key: Exclude<keyof Counters, 't'>): number | undefined {
  const a = prev[key];
  const b = cur[key];
  return a === undefined || b === undefined || b < a ? undefined : b - a;
}

function perSecond(prev: Counters | undefined, cur: Counters, key: Exclude<keyof Counters, 't'>): number | undefined {
  if (!prev) return undefined;
  const d = delta(prev, cur, key);
  return d === undefined ? undefined : d / ((cur.t - prev.t) / 1000);
}

// history is oldest first and ends with the counters read from samples.
function buildStandaloneNode(samples: readonly Sample[], history: readonly Counters[], id: string) {
  const cur = history[history.length - 1];
  const prev = history.slice(0, -1).reverse().find(h => cur.t - h.t >= MIN_WINDOW_MS);
  const queries = prev && delta(prev, cur, 'queryStarted');
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
      avgLatencyMs: queries && latency !== undefined ? (latency / queries) * 1000 : undefined,
    },
    errorsPerMinute: errors === undefined ? undefined : errors * 60,
    cpuHistory,
  };
}

async function scrape(config: Config, reply: FastifyReply): Promise<string | undefined> {
  const fail = async (status: number, error: string, message: string): Promise<undefined> => {
    await reply.status(status).send({ error, message, path: '/metrics' });
    return undefined;
  };
  try {
    await checkEndpointSafe(config.monitorTarget, config.blockRfc1918);
  } catch (err) {
    if (err instanceof SsrfError) return fail(403, 'ssrf_denied', err.message);
    throw err;
  }
  try {
    const res = await undiciRequest(`${config.monitorTarget}/metrics`, {
      method: 'GET',
      headersTimeout: SCRAPE_TIMEOUT_MS,
      bodyTimeout: SCRAPE_TIMEOUT_MS,
      reset: true,
    });
    if (res.statusCode !== 200) {
      await res.body.dump();
      return fail(502, 'upstream_error', `GET /metrics returned ${res.statusCode}`);
    }
    return await res.body.text();
  } catch (err: unknown) {
    const code = (err as NodeJS.ErrnoException).code ?? 'error';
    return fail(502, 'upstream_error', `GET /metrics failed with ${code}`);
  }
}

export async function registerMonitor(app: FastifyInstance, config: Config): Promise<void> {
  const allowed: readonly Role[] = config.monitorRoles ?? ['admin'];
  const history: Counters[] = [];
  const id = new URL(config.monitorTarget).host;

  const requireMonitorRole = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
    const role = request.principal?.role;
    if (!role || !allowed.includes(role)) {
      await reply.status(403).send({
        error: 'forbidden',
        message: 'Monitor is limited to administrators. Set CANOPY_MONITOR_ROLES to open it to other roles.',
      });
    }
  };

  app.get('/monitor/snapshot', { preHandler: [app.canopyAuth.requireAuth, requireMonitorRole] }, async (_request, reply) => {
    const text = await scrape(config, reply);
    if (text === undefined) return;
    const samples = parsePrometheusText(text);
    const cur = readCounters(samples, Date.now());
    while (history.length && cur.t - history[0].t > HISTORY_MS) history.shift();
    history.push(cur);
    return {
      mode: 'standalone' as const,
      source: id,
      scrapedAt: cur.t,
      nodes: [buildStandaloneNode(samples, history, id)],
    };
  });
}
