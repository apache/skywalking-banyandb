/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Config } from '../src/config.js';
import { registerSession } from '../src/plugins/session.js';
import { SessionStore } from '../src/lib/sessions.js';
import { UserDirectory } from '../src/lib/users.js';

function makeConfig(overrides: Partial<Config> = {}): Config {
  return {
    port: 4000,
    sessionSecret: 'test-secret-32-chars-minimum-xxxxx',
    banyandbTarget: 'http://127.0.0.1:17913',
    monitorTarget: 'http://127.0.0.1:2121',
    upstreamTimeoutMs: 120000,
    auth: {
      backend: 'local',
      local: { users: [{ username: 'admin', passwordHash: '$2b$04$x', role: 'admin' }] },
      session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: 'auto' },
    },
    devNoAuth: false,
    blockRfc1918: false,
    basePath: '/',
    baseHref: '/',
    ...overrides,
  };
}

function cookieHeader(setCookie: string | string[] | undefined): string {
  if (Array.isArray(setCookie)) {
    return setCookie[0];
  }
  return setCookie ?? '';
}

describe('session cookies', () => {
  const apps: FastifyInstance[] = [];

  afterEach(async () => {
    await Promise.all(apps.splice(0).map(app => app.close()));
  });

  async function buildApp(config: Config = makeConfig()): Promise<FastifyInstance> {
    const app = Fastify({ trustProxy: true });
    apps.push(app);
    await registerSession(app, config);
    app.get('/login', async (_request, reply) => {
      app.canopyAuth.setSessionCookie(reply, app.canopyAuth.store.create('admin', null));
      return { ok: true };
    });
    app.get('/me', { preHandler: [app.canopyAuth.requireAuth] }, async request => ({ user: request.principal?.username, role: request.principal?.role }));
    app.get('/browser', { preHandler: [app.canopyAuth.requireBrowserSession] }, async () => ({ ok: true }));
    return app;
  }

  it('scopes the session cookie to the configured base path', async () => {
    const app = await buildApp(makeConfig({ basePath: '/canopy', baseHref: '/canopy/' }));

    const response = await app.inject({ method: 'GET', url: '/login' });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).toContain('Path=/canopy/');
  });

  it('sets an httpOnly strict cookie named canopy_sid with a random 256-bit id', async () => {
    const app = await buildApp();

    const header = cookieHeader((await app.inject({ method: 'GET', url: '/login' })).headers['set-cookie']);

    expect(header).toMatch(/^canopy_sid=[A-Za-z0-9_-]{43};/);
    expect(header).toContain('HttpOnly');
    expect(header).toContain('SameSite=Strict');
  });

  it('sets a Secure cookie when TLS is terminated by a proxy', async () => {
    const app = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: '/login',
      headers: { 'x-forwarded-proto': 'https' },
    });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).toContain('Secure');
  });

  it('does not set a Secure cookie over HTTP', async () => {
    const app = await buildApp();

    const response = await app.inject({ method: 'GET', url: '/login' });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).not.toContain('Secure');
  });

  it('honours an explicit cookieSecure=true over HTTP', async () => {
    const base = makeConfig();
    const app = await buildApp(makeConfig({ auth: { ...base.auth, session: { ...base.auth.session, cookieSecure: true } } }));

    const response = await app.inject({ method: 'GET', url: '/login' });

    expect(cookieHeader(response.headers['set-cookie'])).toContain('Secure');
  });

  it('rejects requests without a valid session and accepts the session cookie', async () => {
    const app = await buildApp();
    const login = await app.inject({ method: 'GET', url: '/login' });
    const cookie = cookieHeader(login.headers['set-cookie']).split(';')[0];

    expect((await app.inject({ method: 'GET', url: '/me' })).statusCode).toBe(401);
    expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie: 'canopy_sid=forged' } })).statusCode).toBe(401);
    const ok = await app.inject({ method: 'GET', url: '/me', headers: { cookie } });
    expect(ok.statusCode).toBe(200);
    expect(JSON.parse(ok.body)).toEqual({ user: 'admin', role: 'admin' });
    expect((await app.inject({ method: 'GET', url: '/browser', headers: { cookie } })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/browser' })).statusCode).toBe(401);
  });

  it('re-resolves the role from config and rejects removed users without re-login', async () => {
    const users = [{ username: 'admin', passwordHash: '$2b$04$x', role: 'admin' as const }];
    const base = makeConfig();
    const app = await buildApp(makeConfig({ auth: { ...base.auth, local: { users } } }));
    const cookie = cookieHeader((await app.inject({ method: 'GET', url: '/login' })).headers['set-cookie']).split(';')[0];

    users[0] = { ...users[0], role: 'readonly' as never };
    const demoted = await app.inject({ method: 'GET', url: '/me', headers: { cookie } });
    expect(JSON.parse(demoted.body).role).toBe('readonly');

    users.length = 0;
    expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie } })).statusCode).toBe(401);
  });
});

describe('session sliding expiry over HTTP', () => {
  const apps: FastifyInstance[] = [];

  afterEach(async () => {
    vi.useRealTimers();
    await Promise.all(apps.splice(0).map(app => app.close()));
  });

  async function setup(ttlMinutes: number): Promise<{ app: FastifyInstance; cookie: string }> {
    vi.useFakeTimers({ toFake: ['Date'] });
    const base = makeConfig();
    const app = Fastify();
    apps.push(app);
    await registerSession(app, makeConfig({ auth: { ...base.auth, session: { ...base.auth.session, ttlMinutes } } }));
    app.get('/login', async (_request, reply) => {
      app.canopyAuth.setSessionCookie(reply, app.canopyAuth.store.create('admin', null));
      return { ok: true };
    });
    app.get('/me', { preHandler: [app.canopyAuth.requireAuth] }, async () => ({ ok: true }));
    const login = await app.inject({ method: 'GET', url: '/login' });
    return { app, cookie: cookieHeader(login.headers['set-cookie']).split(';')[0] };
  }

  it('rejects a request after the session was idle past the TTL', async () => {
    const { app, cookie } = await setup(1);
    vi.advanceTimersByTime(61_000);
    expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie } })).statusCode).toBe(401);
  });

  it('keeps the session alive while requests arrive within the TTL', async () => {
    const { app, cookie } = await setup(1);
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(50_000);
      expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie } })).statusCode).toBe(200);
    }
  });

  it('passes the configured minutes to the store and cookie', async () => {
    const { app, cookie } = await setup(3);
    vi.advanceTimersByTime(170_000);
    const res = await app.inject({ method: 'GET', url: '/me', headers: { cookie } });
    expect(res.statusCode).toBe(200);
    expect(cookieHeader(res.headers['set-cookie'])).toContain('Max-Age=180');
    vi.advanceTimersByTime(181_000);
    expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie } })).statusCode).toBe(401);
  });
});

describe('SessionStore', () => {
  it('slides expiry on use and expires idle sessions', () => {
    let now = 1_000;
    const store = new SessionStore({ ttlMs: 100, now: () => now });
    const id = store.create('u', null);

    now += 60;
    expect(store.touch(id)?.username).toBe('u');
    now += 60;
    expect(store.touch(id)?.username).toBe('u');
    now += 101;
    expect(store.touch(id)).toBeUndefined();
    expect(store.size).toBe(0);
  });

  it('revokes a destroyed session and evicts the oldest past the cap', () => {
    const store = new SessionStore({ ttlMs: 1000, maxSessions: 2 });
    const a = store.create('a', null);
    const b = store.create('b', null);
    store.create('c', null);

    expect(store.touch(a)).toBeUndefined();
    expect(store.touch(b)?.username).toBe('b');
    store.destroy(b);
    expect(store.touch(b)).toBeUndefined();
  });

  it('promotes a touched session so the least recently used one is evicted', () => {
    const store = new SessionStore({ ttlMs: 1000, maxSessions: 2 });
    const a = store.create('a', null);
    const b = store.create('b', null);
    store.touch(a);
    store.create('c', null);

    expect(store.touch(b)).toBeUndefined();
    expect(store.touch(a)?.username).toBe('a');
  });

  it('sweep drops untouched expired sessions and keeps live ones', () => {
    let now = 0;
    const store = new SessionStore({ ttlMs: 100, now: () => now });
    store.create('old', null);
    now = 60;
    const live = store.create('live', null);
    now = 120;

    store.sweep();

    expect(store.size).toBe(1);
    expect(store.touch(live)?.username).toBe('live');
  });

  it('the periodic sweeper removes untouched expired sessions', async () => {
    vi.useFakeTimers();
    try {
      const app = Fastify();
      await registerSession(app, makeConfig());
      app.canopyAuth.store.create('idle', null);
      expect(app.canopyAuth.store.size).toBe(1);

      await vi.advanceTimersByTimeAsync(61 * 60_000);

      expect(app.canopyAuth.store.size).toBe(0);
      await app.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops the sweeper timer when the app closes', async () => {
    const spy = vi.spyOn(globalThis, 'clearInterval');
    const app = Fastify();
    await registerSession(app, makeConfig());
    await app.close();
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});

describe('UserDirectory', () => {
  const user = (role: 'admin' | 'readonly') => ({ username: 'alice', passwordHash: '$2b$04$x', role });

  it('uses the static list when nothing can be reloaded', () => {
    const users = [user('admin')];
    const directory = new UserDirectory({ users });

    users.length = 0;

    expect(directory.find('alice')).toBeUndefined();
  });

  it('re-reads the configured source at most every few seconds and keeps the last good list on error', () => {
    let now = 0;
    let source: ReturnType<typeof user>[] | Error = [user('admin')];
    const reload = vi.fn(() => {
      if (source instanceof Error) throw source;
      return source;
    });
    const errors: unknown[] = [];
    const directory = new UserDirectory({ users: [user('admin')], reload }, err => errors.push(err), () => now);

    source = [user('readonly')];
    now = 1_000;
    expect(directory.find('alice')?.role).toBe('admin');
    expect(reload).not.toHaveBeenCalled();

    now = 6_000;
    expect(directory.find('alice')?.role).toBe('readonly');
    expect(directory.find('alice')?.role).toBe('readonly');
    expect(reload).toHaveBeenCalledTimes(1);

    source = new Error('mid-write');
    now = 12_000;
    expect(directory.find('alice')?.role).toBe('readonly');
    expect(errors).toHaveLength(1);

    source = [];
    now = 18_000;
    expect(directory.find('alice')).toBeUndefined();
  });
});
