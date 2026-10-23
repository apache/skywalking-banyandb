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

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import { createServer, type Server } from 'node:http';
import { getGlobalDispatcher, MockAgent, setGlobalDispatcher } from 'undici';
import { registerSession } from '../src/plugins/session.js';
import { registerMonitor } from '../src/routes/monitor.js';
import { registerProxy } from '../src/plugins/proxy.js';
import { parseClusterState, parseStorageMetrics } from '../src/lib/monitor.js';
import type { Config } from '../src/config.js';

const config: Config = {
  port: 4000, sessionSecret: 'test-secret-32-chars-minimum-xxxxx',
  banyandbTarget: 'http://127.0.0.1:17913', monitorTarget: 'http://127.0.0.1:2121',
  upstreamTimeoutMs: 500, users: [], devNoAuth: false, blockRfc1918: false,
  basePath: '/', baseHref: '/',
};
const clusterState = { routeTables: { tier2: {
  registered: [
    { metadata: { name: 'data-0' }, roles: ['ROLE_DATA', 'ROLE_META'], grpcAddress: 'data-0:17912' },
    { metadata: { name: 'data-1' }, roles: ['ROLE_DATA'] },
    { metadata: { name: 'data-2' } },
  ], active: ['data-0'], evictable: ['data-1'],
} } };
const diskMetrics = [
  'banyandb_system_disk{path="/data",kind="total",pod_name="data-0",container_name="data"} 100',
  'banyandb_system_disk{path="/data",kind="used",pod_name="data-0",container_name="data"} 25',
  'banyandb_system_disk{path="/data",kind="used",pod_name="data-1",container_name="data"} 60',
  '',
].join('\n');

describe('monitor data projections', () => {
  it('preserves route membership and reported active, evictable and registered state', () => {
    expect(parseClusterState(clusterState)).toEqual({ nodes: [
      { name: 'data-0', route: 'tier2', roles: ['ROLE_DATA', 'ROLE_META'], address: 'data-0:17912', state: 'active' },
      { name: 'data-1', route: 'tier2', roles: ['ROLE_DATA'], address: '', state: 'evictable' },
      { name: 'data-2', route: 'tier2', roles: [], address: '', state: 'registered' },
    ] });
    expect(parseClusterState({})).toEqual({ nodes: [] });
    expect(() => parseClusterState({ routeTables: { bad: { registered: [{}] } } })).toThrow();
  });

  it('projects reported API and file format versions without inventing an application version', () => {
    const node = { metadata: { name: 'data-0' }, version: { apiVersion: '0.11', fileFormatVersion: '1.5.0' } };
    expect(parseClusterState({ routeTables: { tier2: { registered: [node] } } }).nodes[0]).toMatchObject({
      name: 'data-0', apiVersion: '0.11', fileFormatVersion: '1.5.0',
    });
    expect(parseClusterState(clusterState).nodes[0]).not.toHaveProperty('apiVersion');
    const unversioned = parseClusterState({ routeTables: { tier2: { registered: [{ ...node, version: null }] } } }).nodes[0];
    expect(unversioned.name).toBe('data-0');
    expect(unversioned).not.toHaveProperty('apiVersion');
    expect(unversioned).not.toHaveProperty('fileFormatVersion');
  });

  it('pairs disk totals by node and path without inventing missing samples', () => {
    expect(parseStorageMetrics(diskMetrics).disks).toEqual([
      { node: 'data-0/data', path: '/data', totalBytes: 100, usedBytes: 25 },
      { node: 'data-1/data', path: '/data', totalBytes: null, usedBytes: 60 },
    ]);
    const direct = parseStorageMetrics('banyandb_system_disk{path="/data",kind="used"} 7\n');
    expect(direct.disks[0].node).toBe('Configured monitor target (direct scrape)');
  });

  it('reports uptime seconds with explicit FODC and direct scrape identities', () => {
    const parsed = parseStorageMetrics([
      'banyandb_system_up_time{pod_name="data-0",container_name="data"} 123.5',
      'banyandb_system_up_time 42',
      '',
    ].join('\n'));
    expect(parsed.uptimes).toEqual([
      { node: 'data-0/data', seconds: 123.5 },
      { node: 'Configured monitor target (direct scrape)', seconds: 42 },
    ]);
    expect(parsed.metrics).toEqual([]);
    expect(parseStorageMetrics(diskMetrics).uptimes).toEqual([]);
  });

  it.each([
    'banyandb_system_up_time -1\n',
    'banyandb_system_up_time NaN\n',
    'banyandb_system_up_time +Inf\n',
    'banyandb_system_up_time{pod_name="a",extra="1"} 1\nbanyandb_system_up_time{pod_name="a",extra="2"} 2\n',
  ])('rejects invalid or ambiguous uptime sample: %s', input => {
    expect(() => parseStorageMetrics(input)).toThrow();
  });

  it('decodes Prometheus escapes and preserves identity labels', () => {
    const parsed = parseStorageMetrics('banyandb_system_disk{path="C:\\\\data",kind="used",pod_name="a\\n\\"b"} 1e3\n');
    expect(parsed.metrics[0]).toEqual({
      name: 'banyandb_system_disk', labels: { path: 'C:\\data', kind: 'used', pod_name: 'a\n"b' }, value: 1000,
    });
  });

  it.each([
    'banyandb_system_disk{path="/data",kind="used"} 1',
    'banyandb_system_disk{broken} 1\n',
    'banyandb_system_disk{path="/data",kind="used"} NaN\n',
    'banyandb_system_disk{path="/data",kind="used"} -1\n',
    'banyandb_system_disk{path="/data",kind="used"} 1\nbanyandb_system_disk{kind="used",path="/data"} 2\n',
    '# HELP no_samples none\n',
  ])('rejects corrupt or incomplete metrics: %s', input => {
    expect(() => parseStorageMetrics(input)).toThrow();
  });

  it('rejects excessive samples rather than returning partial data', () => {
    const input = Array.from({ length: 10001 }, (_, i) => `unrelated{n="${i}"} 1\n`).join('');
    expect(() => parseStorageMetrics(input)).toThrow();
  });

  it('exposes storage gauges without query, queue or plugin counters', () => {
    const text = [
      'banyandb_measure_total_file_elements{group="metrics"} 10',
      'banyandb_stream_tst_total_mem_part_bytes{group="logs"} 20',
      'banyandb_trace_tst_total_file_parts{group="traces"} 2',
      'banyandb_trace_tst_pipeline_plugin_batches{group="traces"} 8',
      'banyandb_queue_pub_total_finished{group="logs"} 9',
      'banyandb_measure_query_total{group="metrics"} 4',
      '',
    ].join('\n');
    expect(parseStorageMetrics(text).metrics.map(sample => sample.name)).toEqual([
      'banyandb_measure_total_file_elements',
      'banyandb_stream_tst_total_mem_part_bytes',
      'banyandb_trace_tst_total_file_parts',
    ]);
  });
});

describe('monitor overview endpoint', () => {
  let app: FastifyInstance;
  let agent: MockAgent;
  let previous: ReturnType<typeof getGlobalDispatcher>;
  let upstream: Server | undefined;

  beforeEach(() => {
    previous = getGlobalDispatcher();
    agent = new MockAgent();
    agent.disableNetConnect();
    setGlobalDispatcher(agent);
  });
  afterEach(async () => {
    if (app) await app.close();
    if (upstream) {
      upstream.closeAllConnections();
      await new Promise<void>(resolve => upstream!.close(() => resolve()));
      upstream = undefined;
    }
    await agent.close();
    setGlobalDispatcher(previous);
  });

  async function build(overrides: Partial<Config> = {}) {
    const settings = { ...config, ...overrides };
    app = Fastify();
    await app.register(async scoped => {
      await registerSession(scoped, settings);
      scoped.post<{ Body: { role: 'admin' | 'readonly' } }>('/test-login', async req => {
        req.session.user = 'tester';
        req.session.role = req.body.role;
        return { ok: true };
      });
      await registerMonitor(scoped, settings);
      await registerProxy(scoped, settings);
    }, settings.basePath === '/' ? {} : { prefix: settings.basePath });
    return settings;
  }
  async function login(role: 'admin' | 'readonly', basePath = '') {
    const res = await app.inject({ method: 'POST', url: basePath + '/test-login', payload: { role } });
    return String(res.headers['set-cookie']).split(';')[0];
  }
  function mockSuccess() {
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(200, clusterState);
    agent.get(config.monitorTarget).intercept({ path: '/metrics' }).reply(200, diskMetrics);
  }

  it('requires a session and denies readonly users for all methods by default', async () => {
    await build();
    expect((await app.inject('/api/monitor/overview')).statusCode).toBe(401);
    const cookie = await login('readonly');
    for (const method of ['GET', 'HEAD', 'POST'] as const) {
      const response = await app.inject({ method, url: '/api/monitor/overview', headers: { cookie } });
      expect(response.statusCode).toBe(403);
      expect(response.headers['cache-control']).toBe('no-store');
    }
  });

  it('allows explicit readonly opt-in at a nested base path and prevents proxy method fallback', async () => {
    await build({ basePath: '/tools/canopy', baseHref: '/tools/canopy/', monitor: {
      allowReadonly: true, refreshIntervalMs: 30000, experimentalSections: true,
    } });
    const cookie = await login('readonly', '/tools/canopy');
    mockSuccess();
    const response = await app.inject({ url: '/tools/canopy/api/monitor/overview', headers: { cookie } });
    expect(response.statusCode).toBe(200);
    expect(response.headers['cache-control']).toBe('no-store');
    expect(response.json()).toMatchObject({ refreshIntervalMs: 30000, experimentalSections: true, cluster: { status: 'ok' }, storage: { status: 'ok' } });
    expect((await app.inject({ method: 'POST', url: '/tools/canopy/api/monitor/overview', headers: { cookie } })).statusCode).toBe(405);
    expect((await app.inject('/api/monitor/overview')).statusCode).toBe(404);
  });

  it('attaches configured database credentials without forwarding browser headers to either upstream', async () => {
    await build({ upstreamUsername: 'service', upstreamPassword: 'secret' });
    const cookie = await login('admin');
    const databaseHeaders: Record<string, string> = {};
    const monitorHeaders: Record<string, string> = {};
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(options => {
      Object.assign(databaseHeaders, options.headers);
      return { statusCode: 200, data: clusterState };
    });
    agent.get(config.monitorTarget).intercept({ path: '/metrics' }).reply(options => {
      Object.assign(monitorHeaders, options.headers);
      return { statusCode: 200, data: diskMetrics };
    });
    const response = await app.inject({ url: '/api/monitor/overview', headers: { cookie, authorization: 'Bearer browser-token', 'x-client-secret': 'private' } });
    expect(response.json().cluster.status).toBe('ok');
    expect(databaseHeaders.authorization).toBe('Basic ' + Buffer.from('service:secret').toString('base64'));
    expect(monitorHeaders.authorization).toBeUndefined();
    for (const headers of [databaseHeaders, monitorHeaders]) {
      expect(headers.cookie).toBeUndefined();
      expect(headers['x-client-secret']).toBeUndefined();
    }
  });

  it('preserves successful metrics when cluster fails, with generic error text', async () => {
    await build();
    const cookie = await login('admin');
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(500, 'secret diagnostic');
    agent.get(config.monitorTarget).intercept({ path: '/metrics' }).reply(200, diskMetrics);
    const response = await app.inject({ url: '/api/monitor/overview', headers: { cookie } });
    expect(response.json().cluster).toEqual({ status: 'error', message: 'Cluster state is unavailable' });
    expect(response.json().storage.status).toBe('ok');
    expect(response.body).not.toContain('secret diagnostic');
  });

  it.each([
    [200, 'bad metrics\n'],
    [302, ''],
    [200, 'x'.repeat(4 * 1024 * 1024 + 1)],
  ])('keeps cluster data when metrics fail (%s)', async (status, body) => {
    await build();
    const cookie = await login('admin');
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(200, clusterState);
    agent.get(config.monitorTarget).intercept({ path: '/metrics' }).reply(status, body, { headers: { location: 'http://169.254.169.254/' } });
    const response = await app.inject({ url: '/api/monitor/overview', headers: { cookie } });
    expect(response.json().cluster.status).toBe('ok');
    expect(response.json().storage.status).toBe('error');
  });

  it('checks the monitoring endpoint against the SSRF policy on every request', async () => {
    await build({ monitorTarget: 'http://169.254.169.254' });
    const cookie = await login('admin');
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(200, clusterState).times(2);
    for (let attempt = 0; attempt < 2; attempt++) {
      const response = await app.inject({ url: '/api/monitor/overview', headers: { cookie } });
      expect(response.json().cluster.status).toBe('ok');
      expect(response.json().storage).toEqual({ status: 'error', message: 'Storage metrics are unavailable' });
    }
  });

  it('applies a deadline to the whole response body, including a stalled stream', async () => {
    upstream = createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.write('banyandb_system_disk{path="/data",kind="used"} 1\n');
      // Leave the body open: a headers-only timeout would never complete.
    });
    await new Promise<void>(resolve => upstream!.listen(0, '127.0.0.1', resolve));
    const address = upstream.address();
    if (!address || typeof address === 'string') throw new Error('Missing test server address');
    agent.enableNetConnect('127.0.0.1');
    await build({ monitorTarget: `http://127.0.0.1:${address.port}`, upstreamTimeoutMs: 100 });
    const cookie = await login('admin');
    agent.get(config.banyandbTarget).intercept({ path: '/api/v1/cluster/state' }).reply(200, clusterState);
    const response = await app.inject({ url: '/api/monitor/overview', headers: { cookie } });
    expect(response.json().storage.status).toBe('error');
    expect(response.json().cluster.status).toBe('ok');
  });
});
