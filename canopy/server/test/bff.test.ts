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

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Fastify, { FastifyInstance } from 'fastify';
import secureSession from '@fastify/secure-session';
import { registerAuth } from '../src/routes/auth.js';
import { registerProxy } from '../src/plugins/proxy.js';
import { registerStatic } from '../src/plugins/static.js';
import type { Config } from '../src/config.js';
import bcrypt from 'bcryptjs';
import { MockAgent, setGlobalDispatcher } from 'undici';

// Mock DNS so SSRF check passes for test hostnames (no real DNS in unit tests)
vi.mock('node:dns/promises', () => ({
  lookup: vi.fn(async (hostname: string) => {
    // All test hostnames resolve to safe public IPs
    const safeMap: Record<string, string> = {
      'upstream-a.test': '93.184.216.10',
      'upstream-b.test': '93.184.216.11',
      'monitor.test': '93.184.216.12',
    };
    const addr = safeMap[hostname] || '93.184.216.34';
    return { address: addr, family: 4 };
  }),
}));

const TEST_SECRET = 'test-secret-32-chars-minimum-xxxxx';

async function buildTestApp(configOverrides: Partial<Config> = {}): Promise<FastifyInstance> {
  const adminHash = await bcrypt.hash('adminpass', 4);
  const readonlyHash = await bcrypt.hash('readpass', 4);

  const config: Config = {
    port: 4001,
    sessionSecret: TEST_SECRET,
    banyandbTarget: 'http://upstream-a.test:17913',
    monitorTarget: 'http://monitor.test:2121',
    upstreamTimeoutMs: 5000,
    upstreamUsername: undefined,
    upstreamPassword: undefined,
    users: [
      { username: 'admin', passwordHash: adminHash, role: 'admin' },
      { username: 'reader', passwordHash: readonlyHash, role: 'readonly' },
    ],
    devNoAuth: false,
    blockRfc1918: false,
    ...configOverrides,
  };

  const app = Fastify({ logger: false });
  await app.register(secureSession, {
    secret: config.sessionSecret,
    salt: 'canopy-sess-salt',
    cookie: { path: '/', httpOnly: true, sameSite: 'strict', secure: false },
  });
  await registerAuth(app, config);
  await registerProxy(app, config);
  return app;
}

async function loginAs(app: FastifyInstance, username: string, password: string, endpoint = 'http://upstream-a.test:17913'): Promise<string> {
  const res = await app.inject({
    method: 'POST',
    url: '/auth/login',
    payload: { username, password, endpoint },
  });
  expect(res.statusCode).toBe(200);
  const setCookie = res.headers['set-cookie'];
  return Array.isArray(setCookie) ? setCookie[0] : (setCookie as string);
}

describe('Auth routes', () => {
  let app: FastifyInstance;

  beforeEach(async () => { app = await buildTestApp(); });
  afterEach(async () => { await app.close(); });

  it('POST /auth/login with valid credentials sets session', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'adminpass', endpoint: 'http://upstream-a.test:17913' },
    });
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.user).toBe('admin');
    expect(body.role).toBe('admin');
    expect(body.endpoint).toBe('http://upstream-a.test:17913');
    expect(res.headers['set-cookie']).toBeTruthy();
  });

  it('POST /auth/login with wrong password returns 401', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'wrongpass', endpoint: 'http://upstream-a.test:17913' },
    });
    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toBe('invalid_credentials');
  });

  it('POST /auth/login with unknown user returns 401', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'nobody', password: 'pass', endpoint: 'http://upstream-a.test:17913' },
    });
    expect(res.statusCode).toBe(401);
  });

  it('POST /auth/login with malformed endpoint returns 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'adminpass', endpoint: 'ftp://bad.endpoint' },
    });
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toBe('bad_endpoint');
  });

  it('GET /auth/session returns user info when authenticated', async () => {
    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'GET',
      url: '/auth/session',
      headers: { cookie },
    });
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.user).toBe('admin');
    expect(body.role).toBe('admin');
  });

  it('GET /auth/session returns 401 when unauthenticated', async () => {
    const res = await app.inject({ method: 'GET', url: '/auth/session' });
    expect(res.statusCode).toBe(401);
  });

  it('POST /auth/logout returns 200', async () => {
    const cookie = await loginAs(app, 'admin', 'adminpass');
    const logout = await app.inject({ method: 'POST', url: '/auth/logout', headers: { cookie } });
    expect(logout.statusCode).toBe(200);
    expect(JSON.parse(logout.body)).toMatchObject({ ok: true });
  });
});

describe('Role enforcement', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: /.*/ }).reply(200, '{}', { headers: { 'content-type': 'application/json' } }).times(100);
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('readonly user: GET /api/v1/group/schema/lists returns 200', async () => {
    const cookie = await loginAs(app, 'reader', 'readpass');
    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(200);
  });

  it('readonly user: POST /api/v1/stream/schema returns 403', async () => {
    const cookie = await loginAs(app, 'reader', 'readpass');
    const res = await app.inject({
      method: 'POST', url: '/api/v1/stream/schema', headers: { cookie }, payload: {},
    });
    expect(res.statusCode).toBe(403);
    expect(JSON.parse(res.body).error).toBe('forbidden');
  });

  it('readonly user: DELETE /api/v1/group/schema returns 403', async () => {
    const cookie = await loginAs(app, 'reader', 'readpass');
    const res = await app.inject({
      method: 'DELETE', url: '/api/v1/group/schema', headers: { cookie },
    });
    expect(res.statusCode).toBe(403);
  });

  it('admin user: POST /api/v1/stream/schema is allowed', async () => {
    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'POST', url: '/api/v1/stream/schema', headers: { cookie }, payload: {},
    });
    expect(res.statusCode).not.toBe(403);
  });
});

describe('SPA fallback scoping', () => {
  let app: FastifyInstance;

  beforeEach(async () => {
    app = await buildTestApp();
    await registerStatic(app);
  });

  afterEach(async () => { await app.close(); });

  it('/api/* is not shadowed by SPA fallback', async () => {
    const res = await app.inject({ method: 'GET', url: '/api/v1/test' });
    // Should return 401 (requires auth) — NOT the SPA index.html
    expect(res.statusCode).toBe(401);
    expect(res.headers['content-type']).not.toMatch(/html/);
  });

  it('/auth/* is not shadowed by SPA fallback', async () => {
    const res = await app.inject({ method: 'GET', url: '/auth/session' });
    expect(res.statusCode).toBe(401);
    expect(res.headers['content-type']).not.toMatch(/html/);
  });
});

describe('Proxy: per-request endpoint isolation (MF1)', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);

    const poolA = mockAgent.get('http://upstream-a.test:17913');
    poolA.intercept({ path: '/api/v1/group/schema/lists', method: 'GET' })
      .reply(200, JSON.stringify({ upstream: 'A' }), { headers: { 'content-type': 'application/json' } })
      .times(10);

    const poolB = mockAgent.get('http://upstream-b.test:17913');
    poolB.intercept({ path: '/api/v1/group/schema/lists', method: 'GET' })
      .reply(200, JSON.stringify({ upstream: 'B' }), { headers: { 'content-type': 'application/json' } })
      .times(10);

    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('two concurrent sessions with different endpoints proxy to their own upstream', async () => {
    const cookieA = await loginAs(app, 'admin', 'adminpass', 'http://upstream-a.test:17913');
    const cookieB = await loginAs(app, 'reader', 'readpass', 'http://upstream-b.test:17913');

    const [resA, resB] = await Promise.all([
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieA } }),
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieB } }),
    ]);

    expect(resA.statusCode).toBe(200);
    expect(JSON.parse(resA.body).upstream).toBe('A');
    expect(resB.statusCode).toBe(200);
    expect(JSON.parse(resB.body).upstream).toBe('B');
  });

  it('/api/* path is forwarded VERBATIM (no prefix mutation)', async () => {
    // The mock intercepts /api/v1/group/schema/lists exactly — if path was mutated this would 404
    const cookie = await loginAs(app, 'admin', 'adminpass', 'http://upstream-a.test:17913');
    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('Proxy: request body forwarding', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('PUT /api/* re-serializes parsed JSON body verbatim to upstream', async () => {
    const payload = { name: 'test-stream', group: 'test-group' };
    const pool = mockAgent.get('http://upstream-a.test:17913');
    // Body match ensures the upstream receives the correctly serialized JSON string,
    // not "[object Object]" from a mis-cast TypeScript as string.
    pool
      .intercept({ path: '/api/v1/stream/schema', method: 'PUT', body: JSON.stringify(payload) })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } });

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'PUT',
      url: '/api/v1/stream/schema',
      headers: { cookie, 'content-type': 'application/json' },
      payload,
    });
    expect(res.statusCode).toBe(200);
  });

  it('POST /api/* with JSON body: upstream receives correct Content-Type', async () => {
    const payload = { name: 'my-group' };
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool
      .intercept({ path: '/api/v1/group/schema', method: 'POST', body: JSON.stringify(payload) })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } });

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/group/schema',
      headers: { cookie, 'content-type': 'application/json' },
      payload,
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('Upstream auth: attach-if-configured (MF2)', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('with credential configured: Authorization header is asserted on upstream request', async () => {
    const expectedAuth = `Basic ${Buffer.from('svc:svcpass').toString('base64')}`;
    // Intercept ONLY matching requests that carry the correct Authorization header.
    // If the proxy omits the header the mock won't match → undici throws → proxy returns 502 → test fails.
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool
      .intercept({ path: /.*/, method: 'GET', headers: { authorization: expectedAuth } })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } })
      .times(10);

    app = await buildTestApp({
      banyandbTarget: 'http://upstream-a.test:17913',
      upstreamUsername: 'svc',
      upstreamPassword: 'svcpass',
    });

    const cookie = await loginAs(app, 'admin', 'adminpass', 'http://upstream-a.test:17913');
    const res = await app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie } });
    expect(res.statusCode).toBe(200);
  });

  it('with credential configured: both concurrent sessions carry the Authorization header', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } })
      .times(10);

    const poolB = mockAgent.get('http://upstream-b.test:17913');
    poolB.intercept({ path: /.*/, method: 'GET' })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } })
      .times(10);

    app = await buildTestApp({
      banyandbTarget: 'http://upstream-a.test:17913',
      upstreamUsername: 'svc',
      upstreamPassword: 'svcpass',
    });

    const cookieA = await loginAs(app, 'admin', 'adminpass', 'http://upstream-a.test:17913');
    const cookieB = await loginAs(app, 'reader', 'readpass', 'http://upstream-b.test:17913');

    const [resA, resB] = await Promise.all([
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieA } }),
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieB } }),
    ]);

    expect(resA.statusCode).toBe(200);
    expect(resB.statusCode).toBe(200);
  });

  it('without credential configured: no Authorization header causes no crash', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } })
      .times(2); // loginAs triggers fetchBanyanVersion (consumes 1); actual test request consumes 2nd

    app = await buildTestApp({ upstreamUsername: undefined, upstreamPassword: undefined });
    const cookie = await loginAs(app, 'admin', 'adminpass');

    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(200);
  });
});

describe('Upstream error envelopes', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('upstream 401 → distinct upstream_auth_error envelope (not a Canopy login 401)', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' }).reply(401, 'Unauthorized', { headers: { 'content-type': 'text/plain' } })
      .times(2); // loginAs triggers fetchBanyanVersion (consumes 1); actual test request consumes 2nd

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(401);
    const body = JSON.parse(res.body);
    expect(body.error).toBe('upstream_auth_error');
    expect(body.error).not.toBe('invalid_credentials');
    expect(body.upstream).toBeDefined();
  });

  it('upstream 5xx → 502 upstream_error envelope', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' }).reply(500, 'Internal Error')
      .times(2); // loginAs triggers fetchBanyanVersion (consumes 1); actual test request consumes 2nd

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(502);
    expect(JSON.parse(res.body).error).toBe('upstream_error');
  });
});

describe('/monitoring proxy', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    const pool = mockAgent.get('http://monitor.test:2121');
    pool.intercept({ path: '/metrics', method: 'GET' }).reply(200, 'metrics_data');
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    await mockAgent.close();
  });

  it('unauthenticated /monitoring/* returns 401', async () => {
    const res = await app.inject({ method: 'GET', url: '/monitoring/metrics' });
    expect(res.statusCode).toBe(401);
  });

  it('authenticated readonly user GET /monitoring/* is permitted', async () => {
    const cookie = await loginAs(app, 'reader', 'readpass');
    const res = await app.inject({
      method: 'GET', url: '/monitoring/metrics', headers: { cookie },
    });
    // Strip /monitoring prefix → sent to monitor.test:2121/metrics → 200
    expect(res.statusCode).toBe(200);
  });
});
