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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { FastifyInstance } from 'fastify';
import { request } from 'undici';
import type { MonitorOverview, MonitorResult } from 'canopy-shared/monitor';
import type { Config } from '../config.js';
import { checkEndpointSafe } from '../lib/ssrf.js';
import { parseClusterState, parseStorageMetrics } from '../lib/monitor.js';

const MAX_BODY_BYTES = 4 * 1024 * 1024;

async function readUpstream(config: Config, target: string, path: string, database: boolean): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(config.upstreamTimeoutMs, 10000));
  try {
    await checkEndpointSafe(target, config.blockRfc1918);
    const headers: Record<string, string> = { accept: database ? 'application/json' : 'text/plain' };
    if (database && config.upstreamUsername && config.upstreamPassword) {
      headers.authorization = `Basic ${Buffer.from(`${config.upstreamUsername}:${config.upstreamPassword}`).toString('base64')}`;
    }
    const response = await request(`${target.replace(/\/$/, '')}${path}`, {
      headers,
      signal: controller.signal,
      reset: true,
    });
    try {
      if (response.statusCode !== 200) throw new Error('Unavailable upstream');
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of response.body) {
        const bytes = Buffer.from(chunk);
        size += bytes.length;
        if (size > MAX_BODY_BYTES) throw new Error('Oversized upstream response');
        chunks.push(bytes);
      }
      return Buffer.concat(chunks).toString('utf8');
    } finally {
      response.body.destroy();
    }
  } finally {
    clearTimeout(timer);
  }
}

async function observe<T>(load: () => Promise<T>, message: string): Promise<MonitorResult<T>> {
  try {
    return { status: 'ok', data: await load() };
  } catch {
    return { status: 'error', message };
  }
}

export async function registerMonitor(app: FastifyInstance, config: Config): Promise<void> {
  // Reserve every method on this exact path so /api/* cannot bypass the monitor role gate.
  app.all('/api/monitor/overview', {
    preHandler: async (req, reply) => {
      reply.header('Cache-Control', 'no-store');
      if (!req.session?.user) return reply.code(401).send({ error: 'unauthenticated', message: 'Login required' });
      if (req.session.role !== 'admin' && !(req.session.role === 'readonly' && config.monitor?.allowReadonly)) {
        return reply.code(403).send({ error: 'forbidden', message: 'Monitor access requires an administrator' });
      }
    },
  }, async (req, reply) => {
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      return reply.header('Allow', 'GET, HEAD').code(405).send({ error: 'method_not_allowed' });
    }
    const [cluster, storage] = await Promise.all([
      observe(async () => parseClusterState(JSON.parse(await readUpstream(config, config.banyandbTarget, '/api/v1/cluster/state', true))), 'Cluster state is unavailable'),
      observe(async () => parseStorageMetrics(await readUpstream(config, config.monitorTarget, '/metrics', false)), 'Storage metrics are unavailable'),
    ]);
    const overview: MonitorOverview = {
      observedAt: new Date().toISOString(),
      refreshIntervalMs: config.monitor?.refreshIntervalMs ?? 10000,
      experimentalSections: config.monitor?.experimentalSections ?? false,
      cluster,
      storage,
    };
    return overview;
  });
}
