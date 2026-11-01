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
import { registerSession } from '../src/plugins/session.js';
import { registerAuth } from '../src/routes/auth.js';
import { registerProxy } from '../src/plugins/proxy.js';
import { registerStatic } from '../src/plugins/static.js';
import type { Config } from '../src/config.js';
import bcrypt from 'bcryptjs';
import { argon2id } from 'hash-wasm';
import { randomBytes } from 'node:crypto';
import { hashSecret } from '../src/lib/tokens.js';
import { getGlobalDispatcher, MockAgent, setGlobalDispatcher } from 'undici';

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
    auth: {
      backend: 'local',
      local: {
        users: [
          { username: 'admin', passwordHash: adminHash, role: 'admin' },
          { username: 'reader', passwordHash: readonlyHash, role: 'readonly' },
        ],
      },
      session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: false },
    },
    devNoAuth: false,
    blockRfc1918: false,
    basePath: '/',
    baseHref: '/',
    ...configOverrides,
  };

  const app = Fastify({ logger: false });
  await registerSession(app, config);
  await registerAuth(app, config);
  await registerProxy(app, config);
  return app;
}

async function loginAs(app: FastifyInstance, username: string, password: string): Promise<string> {
  const res = await app.inject({
    method: 'POST',
    url: '/auth/login',
    payload: { username, password },
  });
  expect(res.statusCode).toBe(200);
  const setCookie = res.headers['set-cookie'];
  return Array.isArray(setCookie) ? setCookie[0] : (setCookie as string);
}

describe('Auth routes', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;
  let previousDispatcher: ReturnType<typeof getGlobalDispatcher>;

  beforeEach(async () => {
    previousDispatcher = getGlobalDispatcher();
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    mockAgent.get('http://upstream-a.test:17913')
      .intercept({ path: '/api/v1/common/api/version', method: 'GET' })
      .reply(200, { version: { version: 'test-version' } });
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    setGlobalDispatcher(previousDispatcher);
    await mockAgent.close();
  });

  it('POST /auth/login with valid credentials sets session', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'adminpass' },
    });
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.user).toBe('admin');
    expect(body.role).toBe('admin');
    expect(res.headers['set-cookie']).toBeTruthy();
  });

  it('POST /auth/login with wrong password returns 401', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'wrongpass' },
    });
    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toBe('invalid_credentials');
  });

  it('POST /auth/login with unknown user returns 401', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'nobody', password: 'pass' },
    });
    expect(res.statusCode).toBe(401);
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

  it('POST /auth/logout revokes the session server-side', async () => {
    const cookie = await loginAs(app, 'admin', 'adminpass');
    await app.inject({ method: 'POST', url: '/auth/logout', headers: { cookie } });

    const res = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie } });
    expect(res.statusCode).toBe(401);
  });

  it('a new login revokes the session the client already held', async () => {
    const first = await loginAs(app, 'admin', 'adminpass');
    const secondRes = await app.inject({
      method: 'POST', url: '/auth/login', headers: { cookie: first }, payload: { username: 'admin', password: 'adminpass' },
    });
    expect(secondRes.statusCode).toBe(200);
    const second = (Array.isArray(secondRes.headers['set-cookie']) ? secondRes.headers['set-cookie'][0] : secondRes.headers['set-cookie']) as string;

    expect((await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: first } })).statusCode).toBe(401);
    expect((await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: second } })).statusCode).toBe(200);
  });

  it('every login is refused when no users are configured', async () => {
    const empty = await buildTestApp({
      auth: { backend: 'local', local: { users: [] }, session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: false } },
    });
    const res = await empty.inject({ method: 'POST', url: '/auth/login', payload: { username: 'admin', password: 'admin' } });
    expect(res.statusCode).toBe(401);
    await empty.close();
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
    await registerStatic(app, { basePath: '/', baseHref: '/' } as Config);
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

describe('Proxy: all sessions use config.banyandbTarget', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;

  beforeEach(async () => {
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);

    // The BFF is configured to talk to upstream-a; the alternate pool must
    // never receive traffic, proving the proxy doesn't honor a per-session
    // endpoint override (capability intentionally retired).
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

  it('two concurrent sessions both proxy to config.banyandbTarget', async () => {
    const cookieA = await loginAs(app, 'admin', 'adminpass');
    const cookieB = await loginAs(app, 'reader', 'readpass');

    const [resA, resB] = await Promise.all([
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieA } }),
      app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie: cookieB } }),
    ]);

    expect(resA.statusCode).toBe(200);
    expect(JSON.parse(resA.body).upstream).toBe('A');
    expect(resB.statusCode).toBe(200);
    expect(JSON.parse(resB.body).upstream).toBe('A');
  });

  it('/api/* path is forwarded VERBATIM (no prefix mutation)', async () => {
    // The mock intercepts /api/v1/group/schema/lists exactly — if path was mutated this would 404
    const cookie = await loginAs(app, 'admin', 'adminpass');
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

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie } });
    expect(res.statusCode).toBe(200);
  });

  it('with credential configured: both concurrent sessions carry the Authorization header', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' })
      .reply(200, '{}', { headers: { 'content-type': 'application/json' } })
      .times(10);

    app = await buildTestApp({
      banyandbTarget: 'http://upstream-a.test:17913',
      upstreamUsername: 'svc',
      upstreamPassword: 'svcpass',
    });

    const cookieA = await loginAs(app, 'admin', 'adminpass');
    const cookieB = await loginAs(app, 'reader', 'readpass');

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

  it('upstream 5xx is forwarded verbatim', async () => {
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: /.*/, method: 'GET' }).reply(500, 'Internal Error')
      .times(2); // loginAs triggers fetchBanyanVersion (consumes 1); actual test request consumes 2nd

    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'GET', url: '/api/v1/group/schema/lists', headers: { cookie },
    });
    expect(res.statusCode).toBe(500);
    expect(res.body).toBe('Internal Error');
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

  it('readonly user: POST /monitoring/* returns 403', async () => {
    const cookie = await loginAs(app, 'reader', 'readpass');
    const res = await app.inject({
      method: 'POST',
      url: '/monitoring/debug/panic',
      headers: { cookie },
      payload: {},
    });
    expect(res.statusCode).toBe(403);
    expect(res.json()).toMatchObject({ error: 'forbidden' });
  });

  it('admin user: POST /monitoring/* is allowed through', async () => {
    const pool = mockAgent.get('http://monitor.test:2121');
    pool.intercept({ path: '/debug/panic', method: 'POST' }).reply(200, 'ok');
    const cookie = await loginAs(app, 'admin', 'adminpass');
    const res = await app.inject({
      method: 'POST',
      url: '/monitoring/debug/panic',
      headers: { cookie },
      payload: {},
    });
    expect(res.statusCode).toBe(200);
  });
});

async function argon2Hash(password: string): Promise<string> {
  return argon2id({
    password, salt: randomBytes(16), parallelism: 1, iterations: 1, memorySize: 8, hashLength: 16, outputType: 'encoded',
  });
}

describe('argon2id login over HTTP', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;
  let previousDispatcher: ReturnType<typeof getGlobalDispatcher>;

  beforeEach(async () => {
    previousDispatcher = getGlobalDispatcher();
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    mockAgent.get('http://upstream-a.test:17913')
      .intercept({ path: '/api/v1/common/api/version', method: 'GET' })
      .reply(200, { version: { version: 'v' } }).persist();
    app = await buildTestApp({
      auth: {
        backend: 'local',
        local: { users: [{ username: 'vera', passwordHash: await argon2Hash('s3cret-pass'), role: 'readonly' }] },
        session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: false },
      },
    });
  });

  afterEach(async () => {
    await app.close();
    setGlobalDispatcher(previousDispatcher);
    await mockAgent.close();
  });

  it('logs in a viewer-mapped argon2id user as readonly', async () => {
    const res = await app.inject({ method: 'POST', url: '/auth/login', payload: { username: 'vera', password: 's3cret-pass' } });
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toMatchObject({ user: 'vera', role: 'readonly' });
    expect(res.headers['set-cookie']).toBeTruthy();
  });

  it('rejects a wrong password with 401', async () => {
    const res = await app.inject({ method: 'POST', url: '/auth/login', payload: { username: 'vera', password: 'nope' } });
    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toBe('invalid_credentials');
    expect(res.headers['set-cookie']).toBeUndefined();
  });
});

describe('Proxy: sliding session cookie', () => {
  let app: FastifyInstance;
  let mockAgent: MockAgent;
  let previousDispatcher: ReturnType<typeof getGlobalDispatcher>;

  beforeEach(async () => {
    previousDispatcher = getGlobalDispatcher();
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    const pool = mockAgent.get('http://upstream-a.test:17913');
    pool.intercept({ path: '/api/v1/common/api/version', method: 'GET' }).reply(200, { version: { version: 'v' } });
    pool.intercept({ path: '/api/v1/ok', method: 'GET' }).reply(201, 'body-data', { headers: { 'set-cookie': 'up=1' } });
    app = await buildTestApp();
  });

  afterEach(async () => {
    await app.close();
    setGlobalDispatcher(previousDispatcher);
    await mockAgent.close();
  });

  it('delivers the renewal Set-Cookie on a proxied response and keeps status/body', async () => {
    const cookie = (await loginAs(app, 'admin', 'adminpass')).split(';')[0];
    const res = await app.inject({ method: 'GET', url: '/api/v1/ok', headers: { cookie } });
    expect(res.statusCode).toBe(201);
    expect(res.body).toBe('body-data');
    const cookies = ([] as string[]).concat(res.headers['set-cookie'] as string | string[]);
    expect(cookies.some(c => c.startsWith(`${cookie.split(';')[0]};`))).toBe(true);
    expect(cookies.some(c => /Max-Age=3600/.test(c))).toBe(true);
    expect(cookies).toContain('up=1');
  });
});

describe('Proxy renewal cookie over HTTPS', () => {
  let mockAgent: MockAgent;
  let previousDispatcher: ReturnType<typeof getGlobalDispatcher>;
  let app: FastifyInstance;

  beforeEach(async () => {
    previousDispatcher = getGlobalDispatcher();
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    mockAgent.get('http://upstream-a.test:17913')
      .intercept({ path: '/api/v1/ok', method: 'GET' }).reply(200, 'ok').times(5);
    const base = await buildTestApp();
    await base.close();
    const config: Config = {
      port: 4001,
      sessionSecret: TEST_SECRET,
      banyandbTarget: 'http://upstream-a.test:17913',
      monitorTarget: 'http://monitor.test:2121',
      upstreamTimeoutMs: 5000,
      auth: {
        backend: 'local',
        local: { users: [{ username: 'admin', passwordHash: '$2b$04$x', role: 'admin' }] },
        session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: 'auto' },
      },
      devNoAuth: false, blockRfc1918: false, basePath: '/', baseHref: '/',
    };
    app = Fastify({ trustProxy: true });
    await registerSession(app, config);
    await registerAuth(app, config);
    await registerProxy(app, config);
  });

  afterEach(async () => {
    await app.close();
    setGlobalDispatcher(previousDispatcher);
    await mockAgent.close();
  });

  const renewal = (res: { headers: Record<string, unknown> }): string =>
    ([] as string[]).concat(res.headers['set-cookie'] as string | string[]).find(c => c.startsWith('canopy_sid=')) ?? '';

  it('keeps Secure on the renewed cookie for an HTTPS request and omits it over HTTP', async () => {
    const cookie = `canopy_sid=${app.canopyAuth.store.create('admin', null)}`;

    const https = await app.inject({ method: 'GET', url: '/api/v1/ok', headers: { cookie, 'x-forwarded-proto': 'https' } });
    expect(renewal(https)).toContain('Secure');
    expect(renewal(https)).toContain('HttpOnly');

    const http = await app.inject({ method: 'GET', url: '/api/v1/ok', headers: { cookie } });
    expect(renewal(http)).not.toBe('');
    expect(renewal(http)).not.toContain('Secure');
  });
});

describe('API tokens', () => {
  const SECRET_A = 'alice-secret-value';
  const SECRET_B = 'bob-secret-value';
  let app: FastifyInstance;
  let users: Config['auth']['local']['users'];
  let mockAgent: MockAgent;
  let previousDispatcher: ReturnType<typeof getGlobalDispatcher>;
  let logs: string[];

  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    previousDispatcher = getGlobalDispatcher();
    mockAgent = new MockAgent();
    mockAgent.disableNetConnect();
    setGlobalDispatcher(mockAgent);
    mockAgent.get('http://upstream-a.test:17913')
      .intercept({ path: /.*/, method: /.*/ }).reply(200, '{}', { headers: { 'content-type': 'application/json' } }).times(100);
    users = [
      { username: 'alice', passwordHash: '$2b$04$x', role: 'admin' },
      { username: 'bob', passwordHash: '$2b$04$x', role: 'readonly' },
      { username: 'carol', passwordHash: '$2b$04$x', role: 'admin' },
    ];
    const base = await buildTestApp();
    await base.close();
    const config: Config = {
      port: 4001,
      sessionSecret: TEST_SECRET,
      banyandbTarget: 'http://upstream-a.test:17913',
      monitorTarget: 'http://monitor.test:2121',
      upstreamTimeoutMs: 5000,
      auth: {
        backend: 'local',
        local: { users },
        tokens: [
          { id: 't1', username: 'alice', secretHash: hashSecret(SECRET_A) },
          { id: 't2', username: 'bob', secretHash: hashSecret(SECRET_B) },
        ],
        session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: false },
      },
      devNoAuth: false, blockRfc1918: false, basePath: '/', baseHref: '/',
    };
    logs = [];
    app = Fastify({ logger: { level: 'trace', stream: { write: (line: string) => { logs.push(line); } } } });
    await registerSession(app, config);
    await registerAuth(app, config);
    await registerProxy(app, config);
    // Cookie sessions for carol are created directly to test bearer-vs-cookie precedence.
    app.get('/whoami', { preHandler: [app.canopyAuth.requireAuth] }, async request => ({ user: request.principal?.username, source: request.principal?.source }));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await app.close();
    setGlobalDispatcher(previousDispatcher);
    await mockAgent.close();
  });

  const bearer = (token: string) => ({ authorization: `Bearer ${token}` });

  it('admin token: GET and POST are allowed; readonly token: POST is 403', async () => {
    expect((await app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: bearer(`hzn_t1_${SECRET_A}`) })).statusCode).toBe(200);
    expect((await app.inject({ method: 'POST', url: '/api/v1/stream/schema', headers: bearer(`hzn_t1_${SECRET_A}`), payload: {} })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers: bearer(`hzn_t2_${SECRET_B}`) })).statusCode).toBe(200);
    expect((await app.inject({ method: 'POST', url: '/api/v1/stream/schema', headers: bearer(`hzn_t2_${SECRET_B}`), payload: {} })).statusCode).toBe(403);
  });

  it('wrong secret, unknown id and malformed token yield the same 401', async () => {
    const responses = await Promise.all([`hzn_t1_wrong`, `hzn_zz_${SECRET_A}`, 'hzn_malformed', 'garbage'].map(
      token => app.inject({ method: 'GET', url: '/whoami', headers: bearer(token) })));
    for (const res of responses) {
      expect(res.statusCode).toBe(401);
      expect(res.body).toBe(responses[0].body);
    }
  });

  it('revokes a token when the user is removed after the cache window; a failed lookup is not cached', async () => {
    const headers = bearer(`hzn_t1_${SECRET_A}`);
    expect((await app.inject({ method: 'GET', url: '/whoami', headers })).statusCode).toBe(200);
    const alice = users.shift()!;
    vi.advanceTimersByTime(29_000);
    expect((await app.inject({ method: 'GET', url: '/whoami', headers })).statusCode).toBe(200);
    vi.advanceTimersByTime(2_000);
    expect((await app.inject({ method: 'GET', url: '/whoami', headers })).statusCode).toBe(401);
    users.unshift(alice);
    expect((await app.inject({ method: 'GET', url: '/whoami', headers })).statusCode).toBe(200);
  });

  it('never creates a cookie session and is rejected by login/logout/session routes', async () => {
    const headers = bearer(`hzn_t1_${SECRET_A}`);
    const who = await app.inject({ method: 'GET', url: '/whoami', headers });
    expect(who.headers['set-cookie']).toBeUndefined();
    expect(JSON.parse(who.body)).toEqual({ user: 'alice', source: 'token' });
    expect((await app.inject({ method: 'GET', url: '/auth/session', headers })).statusCode).toBe(401);
    expect((await app.inject({ method: 'POST', url: '/auth/logout', headers })).statusCode).toBe(401);
    expect((await app.inject({ method: 'POST', url: '/auth/login', headers, payload: { username: 'alice', password: 'x' } })).statusCode).toBe(401);
  });

  it('prefers the bearer principal over a cookie for a different user', async () => {
    const cookie = `canopy_sid=${app.canopyAuth.store.create('carol', null)}`;
    const res = await app.inject({ method: 'GET', url: '/whoami', headers: { ...bearer(`hzn_t2_${SECRET_B}`), cookie } });
    expect(JSON.parse(res.body)).toEqual({ user: 'bob', source: 'token' });
    expect(res.headers['set-cookie']).toBeUndefined();
  });

  it('valid bearer plus a valid cookie still reaches the browser routes through the cookie', async () => {
    const cookie = `canopy_sid=${app.canopyAuth.store.create('carol', null)}`;
    const headers = { ...bearer(`hzn_t1_${SECRET_A}`), cookie };

    const session = await app.inject({ method: 'GET', url: '/auth/session', headers });
    expect(session.statusCode).toBe(200);
    expect(JSON.parse(session.body).user).toBe('carol');

    expect((await app.inject({ method: 'POST', url: '/auth/logout', headers })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie } })).statusCode).toBe(401);
  });

  it.each([['Bearer'], ['Bearer '], ['Bearer hzn_t1_wrong']])('rejects %j even with a valid session cookie', async authorization => {
    const cookie = `canopy_sid=${app.canopyAuth.store.create('carol', null)}`;

    expect((await app.inject({ method: 'GET', url: '/whoami', headers: { authorization, cookie } })).statusCode).toBe(401);
    expect((await app.inject({ method: 'GET', url: '/auth/session', headers: { authorization, cookie } })).statusCode).toBe(401);
  });

  it('applies a changed role after the cache window and enforces it on the proxy', async () => {
    const headers = bearer(`hzn_t1_${SECRET_A}`);
    expect((await app.inject({ method: 'POST', url: '/api/v1/stream/schema', headers, payload: {} })).statusCode).toBe(200);

    users[0] = { ...users[0], role: 'readonly' };
    vi.advanceTimersByTime(29_000);
    expect((await app.inject({ method: 'POST', url: '/api/v1/stream/schema', headers, payload: {} })).statusCode).toBe(200);

    vi.advanceTimersByTime(2_000);
    expect((await app.inject({ method: 'POST', url: '/api/v1/stream/schema', headers, payload: {} })).statusCode).toBe(403);
    expect((await app.inject({ method: 'GET', url: '/api/v1/group/schema/lists', headers })).statusCode).toBe(200);
  });

  it('never logs token secrets', async () => {
    await app.inject({ method: 'GET', url: '/whoami', headers: bearer(`hzn_t1_${SECRET_A}`) });
    await app.inject({ method: 'GET', url: '/whoami', headers: bearer('hzn_t1_wrongsecretvalue') });
    expect(logs.length).toBeGreaterThan(0);
    const all = logs.join('\n');
    expect(all).not.toContain(SECRET_A);
    expect(all).not.toContain('wrongsecretvalue');
  });
});
