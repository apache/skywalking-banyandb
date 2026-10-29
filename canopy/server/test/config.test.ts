/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under
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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadConfig } from '../src/config.js';

const TEST_SECRET = 'test-secret-32-chars-minimum-xxxxx';
let originalEnv: NodeJS.ProcessEnv;

describe('CANOPY_BASE_PATH', () => {
  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.NODE_ENV = 'test';
    process.env.SESSION_SECRET = TEST_SECRET;
    process.env.CANOPY_DEV_NOAUTH = 'true';
    delete process.env.CANOPY_BASE_PATH;
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it.each([
    [undefined, '/', '/'],
    ['/', '/', '/'],
    ['canopy', '/canopy', '/canopy/'],
    ['/canopy', '/canopy', '/canopy/'],
    ['/canopy/', '/canopy', '/canopy/'],
    ['/tools/banyandb/canopy', '/tools/banyandb/canopy', '/tools/banyandb/canopy/'],
  ])('normalizes %s', (configuredPath, expectedPath, expectedHref) => {
    if (configuredPath !== undefined) {
      process.env.CANOPY_BASE_PATH = configuredPath;
    }

    const config = loadConfig();

    expect(config.basePath).toBe(expectedPath);
    expect(config.baseHref).toBe(expectedHref);
  });

  it.each([
    'http://example.com/canopy',
    '//example.com/canopy',
    '/canopy?mode=admin',
    '/canopy#settings',
    '/canopy/../admin',
    '/canopy\\admin',
    '/%2e%2e/admin',
  ])('rejects invalid value %j', configuredPath => {
    process.env.CANOPY_BASE_PATH = configuredPath;

    expect(() => loadConfig()).toThrow(/CANOPY_BASE_PATH/);
  });
});

describe('auth config', () => {
  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.NODE_ENV = 'test';
    process.env.SESSION_SECRET = TEST_SECRET;
    delete process.env.CANOPY_DEV_NOAUTH;
    delete process.env.CANOPY_USERS;
    delete process.env.CANOPY_AUTH_CONFIG;
    delete process.env.CANOPY_AUTH_BACKEND;
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it('accepts backend local with an empty users list and creates no default admin', () => {
    process.env.CANOPY_AUTH_BACKEND = 'local';

    expect(() => loadConfig()).not.toThrow();
    const config = loadConfig();

    expect(config.auth.backend).toBe('local');
    expect(config.auth.local.users).toEqual([]);
  });

  it('refuses the sso backend until it is implemented', () => {
    process.env.CANOPY_AUTH_BACKEND = 'sso';

    expect(() => loadConfig()).toThrow(/not yet supported/);
  });

  it('rejects an unknown backend', () => {
    process.env.CANOPY_AUTH_BACKEND = 'ldap';

    expect(() => loadConfig()).toThrow(/auth\.backend/);
  });

  it('maps Horizon roles and warns once per bcrypt user', () => {
    const dir = mkdtempSync(join(tmpdir(), 'canopy-auth-'));
    const file = join(dir, 'auth.json');
    writeFileSync(file, JSON.stringify({
      auth: {
        backend: 'local',
        local: {
          users: [
            { username: 'v', passwordHash: '$argon2id$v=19$m=8,t=1,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA', role: 'viewer' },
            { username: 'm', passwordHash: '$2b$04$abcdefghijklmnopqrstuuabcdefghijklmnopqrstuvwxyz01234', role: 'maintainer' },
          ],
        },
      },
    }));
    process.env.CANOPY_AUTH_CONFIG = file;

    const config = loadConfig();

    expect(config.auth.local.users.map(u => u.role)).toEqual(['readonly', 'admin']);
    const warns = vi.mocked(console.warn).mock.calls.map(c => String(c[0])).filter(m => m.includes('bcrypt'));
    expect(warns).toHaveLength(1);
    expect(warns[0]).toContain('"m"');
  });

  it('rejects a user with an unknown role or a duplicate username', () => {
    const dir = mkdtempSync(join(tmpdir(), 'canopy-auth-'));
    const file = join(dir, 'users.json');
    process.env.CANOPY_USERS = file;
    const hash = '$argon2id$v=19$m=8,t=1,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA';

    writeFileSync(file, JSON.stringify([{ username: 'a', passwordHash: hash, role: 'root' }]));
    expect(() => loadConfig()).toThrow(/role/);

    writeFileSync(file, JSON.stringify([
      { username: 'a', passwordHash: hash, role: 'admin' },
      { username: 'a', passwordHash: hash, role: 'admin' },
    ]));
    expect(() => loadConfig()).toThrow(/duplicate/);
  });
});

describe('auth config hardening', () => {
  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.NODE_ENV = 'test';
    process.env.SESSION_SECRET = TEST_SECRET;
    delete process.env.CANOPY_DEV_NOAUTH;
    delete process.env.CANOPY_AUTH_CONFIG;
    delete process.env.CANOPY_USERS;
    delete process.env.CANOPY_AUTH_BACKEND;
    delete process.env.CANOPY_AUTH_SESSION_TTL_MINUTES;
    delete process.env.CANOPY_AUTH_SESSION_COOKIE_NAME;
    delete process.env.CANOPY_AUTH_SESSION_COOKIE_SECURE;
    delete process.env.CANOPY_AUTH_TOKENS_FILE;
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it('dev no-auth boots even when CANOPY_USERS points at a missing file', () => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.CANOPY_USERS = join(tmpdir(), 'canopy-does-not-exist', 'users.yaml');

    const config = loadConfig();

    expect(config.auth.local.users.map(u => u.username)).toEqual(['admin']);
  });

  it.each(['1.001', '1.5', '0', '-3', 'abc'])('rejects ttlMinutes %s', ttl => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.CANOPY_AUTH_SESSION_TTL_MINUTES = ttl;

    expect(() => loadConfig()).toThrow(/ttlMinutes must be a positive whole number/);
  });

  it('defaults the session settings to canopy_sid, 60 minutes and auto-secure', () => {
    process.env.CANOPY_DEV_NOAUTH = 'true';

    expect(loadConfig().auth.session).toEqual({ cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: 'auto' });
  });

  it('reads session settings from the auth file and lets CANOPY_AUTH_SESSION_* override them', () => {
    const dir = mkdtempSync(join(tmpdir(), 'canopy-session-'));
    const file = join(dir, 'auth.json');
    writeFileSync(file, JSON.stringify({ auth: { backend: 'local', session: { cookieName: 'file_sid', ttlMinutes: 10, cookieSecure: true } } }));
    process.env.CANOPY_AUTH_CONFIG = file;

    expect(loadConfig().auth.session).toEqual({ cookieName: 'file_sid', ttlMinutes: 10, cookieSecure: true });

    process.env.CANOPY_AUTH_SESSION_COOKIE_NAME = 'env_sid';
    process.env.CANOPY_AUTH_SESSION_TTL_MINUTES = '5';
    process.env.CANOPY_AUTH_SESSION_COOKIE_SECURE = 'false';
    expect(loadConfig().auth.session).toEqual({ cookieName: 'env_sid', ttlMinutes: 5, cookieSecure: false });
  });

  it.each(['bad name', 'a;b', 'x=y'])('rejects cookie name %j', name => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.CANOPY_AUTH_SESSION_COOKIE_NAME = name;

    expect(() => loadConfig()).toThrow(/cookieName/);
  });

  it('rejects an invalid cookieSecure value', () => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.CANOPY_AUTH_SESSION_COOKIE_SECURE = 'sometimes';

    expect(() => loadConfig()).toThrow(/CANOPY_AUTH_SESSION_COOKIE_SECURE must be true, false or auto/);
  });

  it('accepts a whole-number ttlMinutes', () => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.CANOPY_AUTH_SESSION_TTL_MINUTES = '15';

    expect(loadConfig().auth.session.ttlMinutes).toBe(15);
  });

  it('loads and validates auth.tokensFile', () => {
    const dir = mkdtempSync(join(tmpdir(), 'canopy-tokens-'));
    const tokens = join(dir, 'tokens.json');
    const authFile = join(dir, 'auth.json');
    writeFileSync(authFile, JSON.stringify({ auth: { backend: 'local', tokensFile: tokens } }));
    process.env.CANOPY_AUTH_CONFIG = authFile;
    const hash = 'a'.repeat(64);

    writeFileSync(tokens, JSON.stringify([{ id: 't1', username: 'alice', secretHash: hash, description: 'ci' }]));
    expect(loadConfig().auth.tokens).toEqual([{ id: 't1', username: 'alice', secretHash: hash, description: 'ci' }]);

    writeFileSync(tokens, JSON.stringify([{ id: 't1', username: 'alice', secretHash: 'short' }]));
    expect(() => loadConfig()).toThrow(/secretHash/);
    writeFileSync(tokens, JSON.stringify([{ id: 'a_b', username: 'alice', secretHash: hash }]));
    expect(() => loadConfig()).toThrow(/id/);
  });

  describe('user and token sources', () => {
    const argonHash = '$argon2id$v=19$m=8,t=1,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA';
    const dir = (): string => mkdtempSync(join(tmpdir(), 'canopy-src-'));

    it('CANOPY_USERS alone implies the local backend and loads the users', () => {
      const file = join(dir(), 'users.json');
      writeFileSync(file, JSON.stringify([{ username: 'alice', passwordHash: argonHash, role: 'maintainer' }]));
      process.env.CANOPY_USERS = file;

      const config = loadConfig();

      expect(config.auth.backend).toBe('local');
      expect(config.auth.local.users).toEqual([{ username: 'alice', passwordHash: argonHash, role: 'admin' }]);
    });

    it('rejects users configured in both CANOPY_USERS and auth.local.users', () => {
      const d = dir();
      const users = join(d, 'users.json');
      const auth = join(d, 'auth.json');
      const entry = { username: 'alice', passwordHash: argonHash, role: 'admin' };
      writeFileSync(users, JSON.stringify([entry]));
      writeFileSync(auth, JSON.stringify({ auth: { backend: 'local', local: { users: [{ ...entry, username: 'bob' }] } } }));
      process.env.CANOPY_USERS = users;
      process.env.CANOPY_AUTH_CONFIG = auth;

      expect(() => loadConfig()).toThrow(/both set/);
    });

    it('lets CANOPY_AUTH_TOKENS_FILE take precedence over auth.tokensFile', () => {
      const d = dir();
      const fromFile = join(d, 'file-tokens.json');
      const fromEnv = join(d, 'env-tokens.json');
      const auth = join(d, 'auth.json');
      const hash = 'a'.repeat(64);
      writeFileSync(fromFile, JSON.stringify([{ id: 'filetok', username: 'alice', secretHash: hash }]));
      writeFileSync(fromEnv, JSON.stringify([{ id: 'envtok', username: 'alice', secretHash: hash }]));
      writeFileSync(auth, JSON.stringify({ auth: { backend: 'local', tokensFile: fromFile } }));
      process.env.CANOPY_AUTH_CONFIG = auth;

      expect(loadConfig().auth.tokens?.map(t => t.id)).toEqual(['filetok']);

      process.env.CANOPY_AUTH_TOKENS_FILE = fromEnv;
      expect(loadConfig().auth.tokens?.map(t => t.id)).toEqual(['envtok']);
    });

    it('exposes a reload that re-reads the users file', () => {
      const file = join(dir(), 'users.json');
      writeFileSync(file, JSON.stringify([{ username: 'alice', passwordHash: argonHash, role: 'admin' }]));
      process.env.CANOPY_USERS = file;
      const config = loadConfig();

      writeFileSync(file, JSON.stringify([{ username: 'alice', passwordHash: argonHash, role: 'viewer' }]));

      expect(config.auth.local.reload?.().map(u => u.role)).toEqual(['readonly']);
    });
  });
});
