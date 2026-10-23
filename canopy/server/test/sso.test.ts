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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import { createHash, createSign, generateKeyPairSync, randomBytes, type KeyObject } from 'node:crypto';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Writable } from 'node:stream';
import { argon2id } from 'hash-wasm';

import type { AuthPolicy, Config, SsoProviderConfig, SsoRolesConfig } from '../src/config.js';
import type { SsoService } from '../src/lib/sso.js';
import { registerAuth } from '../src/routes/auth.js';
import { registerSession } from '../src/plugins/session.js';

const CLIENT_ID = 'canopy-client';
const BREAK_GLASS_PASSWORD = 'break-glass-pw';

interface TokenOptions {
  readonly alg?: string;
  readonly kid?: string;
  // Signs with this key instead of the provider's own, to model a token from an unknown signer.
  readonly signWith?: KeyObject;
}

interface IssuedCode {
  readonly claims: Record<string, unknown>;
  readonly challenge: string;
  readonly options?: TokenOptions;
}

// An in-process identity provider: OIDC discovery, JWKS, token, userinfo and a GitHub-style
// emails endpoint. It verifies PKCE, single-use codes and the client secret like a real provider.
class MockIdp {
  readonly server: Server = createServer((req, res) => this.handle(req, res));
  keys = generateKeyPairSync('rsa', { modulusLength: 2048 });
  readonly ecKeys = generateKeyPairSync('ec', { namedCurve: 'P-256' });
  readonly codes = new Map<string, IssuedCode>();
  readonly accessTokens = new Map<string, string>();
  readonly hits: Record<string, number> = {};
  advertisedAuthMethods: string[] | undefined;
  lastTokenAuth: 'basic' | 'post' | undefined;
  hang = false;
  tokenMode: 'normal' | 'huge' | 'stall' = 'normal';
  tokenGetStatus = 404;
  emails: unknown = [];
  userinfo: Record<string, unknown> = {};
  url = '';

  async start(): Promise<void> {
    await new Promise<void>(resolve => this.server.listen(0, '127.0.0.1', resolve));
    this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
  }

  async stop(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>(resolve => this.server.close(() => resolve()));
  }

  /** Replaces the RSA signing key while keeping its kid, as a provider does when it rotates keys. */
  rotateKey(): void {
    this.keys = generateKeyPairSync('rsa', { modulusLength: 2048 });
  }

  /** Registers an authorization code for the login the test just started. */
  issueCode(authorizeUrl: URL, claims: Record<string, unknown>, options?: TokenOptions): string {
    const code = randomBytes(8).toString('hex');
    this.codes.set(code, { claims, challenge: authorizeUrl.searchParams.get('code_challenge') ?? '', options });
    return code;
  }

  idToken(claims: Record<string, unknown>, options: TokenOptions = {}): string {
    const alg = options.alg ?? 'RS256';
    const kid = options.kid ?? (alg === 'ES256' ? 'e1' : 'k1');
    const header = Buffer.from(JSON.stringify({ alg, kid, typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify(claims)).toString('base64url');
    const data = `${header}.${payload}`;
    let signature: string;
    if (alg === 'ES256') {
      signature = createSign('sha256').update(data).sign({ key: options.signWith ?? this.ecKeys.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url');
    } else if (alg === 'RS256') {
      signature = createSign('sha256').update(data).sign(options.signWith ?? this.keys.privateKey).toString('base64url');
    } else {
      signature = Buffer.from('not-a-real-signature').toString('base64url');
    }
    return `${data}.${signature}`;
  }

  private json(res: ServerResponse, status: number, body: unknown): void {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  }

  private handle(req: IncomingMessage, res: ServerResponse): void {
    if (this.hang) return;
    const url = new URL(req.url ?? '/', this.url);
    this.hits[url.pathname] = (this.hits[url.pathname] ?? 0) + 1;
    if (url.pathname === '/.well-known/openid-configuration') {
      return this.json(res, 200, {
        issuer: this.url,
        authorization_endpoint: `${this.url}/authorize`,
        token_endpoint: `${this.url}/token`,
        jwks_uri: `${this.url}/jwks`,
        userinfo_endpoint: `${this.url}/userinfo`,
        ...(this.advertisedAuthMethods ? { token_endpoint_auth_methods_supported: this.advertisedAuthMethods } : {}),
      });
    }
    if (url.pathname === '/jwks') {
      return this.json(res, 200, {
        keys: [
          { ...this.keys.publicKey.export({ format: 'jwk' }), kid: 'k1', use: 'sig', alg: 'RS256' },
          { ...this.ecKeys.publicKey.export({ format: 'jwk' }), kid: 'e1', use: 'sig', alg: 'ES256' },
        ],
      });
    }
    if (url.pathname === '/token' && req.method === 'GET') {
      return this.json(res, this.tokenGetStatus, {});
    }
    if (url.pathname === '/token' && req.method === 'POST') {
      let raw = '';
      req.on('data', chunk => { raw += String(chunk); });
      req.on('end', () => {
        const form = new URLSearchParams(raw);
        const issued = this.codes.get(form.get('code') ?? '');
        this.codes.delete(form.get('code') ?? '');
        const challenge = createHash('sha256').update(form.get('code_verifier') ?? '').digest('base64url');
        const basic = /^Basic (.+)$/.exec(String(req.headers.authorization ?? ''))?.[1];
        this.lastTokenAuth = basic ? 'basic' : 'post';
        const secret = basic ? decodeURIComponent(Buffer.from(basic, 'base64').toString().split(':')[1] ?? '') : form.get('client_secret');
        const methodAllowed = !this.advertisedAuthMethods
          || this.advertisedAuthMethods.includes(basic ? 'client_secret_basic' : 'client_secret_post');
        if (!issued || issued.challenge !== challenge || secret !== 'shh' || !methodAllowed) {
          return this.json(res, 400, { error: 'invalid_grant' });
        }
        if (this.tokenMode === 'huge') {
          res.writeHead(200, { 'content-type': 'application/json' });
          return res.end('x'.repeat(2 * 1024 * 1024));
        }
        if (this.tokenMode === 'stall') {
          // Headers and the start of a body arrive, then the provider goes silent.
          res.writeHead(200, { 'content-type': 'application/json' });
          return void res.write('{"access_token":');
        }
        const accessToken = randomBytes(8).toString('hex');
        this.accessTokens.set(accessToken, 'ok');
        return this.json(res, 200, { access_token: accessToken, token_type: 'bearer', id_token: this.idToken(issued.claims, issued.options) });
      });
      return;
    }
    const bearer = /^Bearer (.+)$/.exec(String(req.headers.authorization ?? ''))?.[1];
    if (url.pathname === '/userinfo') {
      return bearer && this.accessTokens.has(bearer) ? this.json(res, 200, this.userinfo) : this.json(res, 401, { error: 'unauthorized' });
    }
    if (url.pathname === '/emails') {
      return bearer && this.accessTokens.has(bearer) ? this.json(res, 200, this.emails) : this.json(res, 401, { error: 'unauthorized' });
    }
    return this.json(res, 404, {});
  }
}

const ROLES: SsoRolesConfig = {
  defaultRoles: [],
  roleByEmail: { 'alice@x.com': ['admin'] },
  roleByDomain: { 'x.com': ['readonly'] },
};

describe('SSO backend', () => {
  let idp: MockIdp;
  const apps: FastifyInstance[] = [];
  let logLines: Array<Record<string, unknown>>;
  let breakGlassHash: string;

  beforeEach(async () => {
    idp = new MockIdp();
    await idp.start();
    logLines = [];
    breakGlassHash = await argon2id({
      password: BREAK_GLASS_PASSWORD,
      salt: randomBytes(16),
      parallelism: 1,
      iterations: 3,
      memorySize: 19456,
      hashLength: 32,
      outputType: 'encoded',
    });
  });

  afterEach(async () => {
    await Promise.all(apps.splice(0).map(app => app.close()));
    await idp.stop();
  });

  function oidcProvider(from: MockIdp = idp, id = 'corp'): SsoProviderConfig {
    return { id, name: 'Corp SSO', type: 'oidc', clientId: CLIENT_ID, clientSecret: 'shh', issuer: from.url, scopes: ['openid', 'email'] };
  }

  function oauth2Provider(from: MockIdp = idp, id = 'github'): SsoProviderConfig {
    return {
      id,
      name: 'GitHub',
      type: 'oauth2',
      clientId: CLIENT_ID,
      clientSecret: 'shh',
      authorizeUrl: `${from.url}/authorize`,
      tokenUrl: `${from.url}/token`,
      userinfoUrl: `${from.url}/userinfo`,
      emailsUrl: `${from.url}/emails`,
      scopes: ['read:user', 'user:email'],
    };
  }

  function makeConfig(overrides: { roles?: SsoRolesConfig; breakGlass?: boolean; providers?: SsoProviderConfig[]; timeoutMs?: number; reloadPolicy?: () => AuthPolicy } = {}): Config {
    return {
      port: 4000,
      sessionSecret: 'test-secret-32-chars-minimum-xxxxx',
      banyandbTarget: 'http://127.0.0.1:1',
      monitorTarget: 'http://127.0.0.1:2121',
      upstreamTimeoutMs: 120000,
      auth: {
        backend: 'sso',
        sso: { providers: overrides.providers ?? [oidcProvider()], roles: overrides.roles ?? ROLES, timeoutMs: overrides.timeoutMs ?? 2000 },
        breakGlass: overrides.breakGlass
          ? { enabled: true, users: [{ username: 'ops', passwordHash: breakGlassHash, role: 'admin' }] }
          : undefined,
        reloadPolicy: overrides.reloadPolicy,
        local: { users: [] },
        session: { cookieName: 'canopy_sid', ttlMinutes: 60, cookieSecure: 'auto' },
      },
      devNoAuth: false,
      blockRfc1918: false,
      basePath: '/',
      baseHref: '/',
    };
  }

  async function buildApp(config: Config): Promise<FastifyInstance> {
    const sink = new Writable({
      write(chunk, _encoding, done) {
        for (const line of String(chunk).split('\n').filter(Boolean)) logLines.push(JSON.parse(line) as Record<string, unknown>);
        done();
      },
    });
    const app = Fastify({ trustProxy: true, logger: { level: 'info', stream: sink } });
    apps.push(app);
    await registerSession(app, config);
    await registerAuth(app, config, { ssoHealthTtlMs: 0 });
    app.get('/me', { preHandler: [app.canopyAuth.requireAuth] }, async request => ({
      user: request.principal?.username,
      role: request.principal?.role,
      source: request.principal?.source,
    }));
    return app;
  }

  function cookies(setCookie: string | string[] | undefined): Record<string, string> {
    const list = Array.isArray(setCookie) ? setCookie : setCookie ? [setCookie] : [];
    const jar: Record<string, string> = {};
    for (const entry of list) {
      const [pair] = entry.split(';');
      const eq = pair.indexOf('=');
      jar[pair.slice(0, eq)] = pair.slice(eq + 1);
    }
    return jar;
  }

  const cookieHeader = (jar: Record<string, string>): string => Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ');

  interface Started {
    readonly authorizeUrl: URL;
    readonly jar: Record<string, string>;
  }

  async function start(app: FastifyInstance, provider = 'corp'): Promise<Started> {
    const res = await app.inject({ method: 'GET', url: `/auth/sso/${provider}/start`, headers: { host: 'canopy.test' } });
    expect(res.statusCode).toBe(302);
    return { authorizeUrl: new URL(String(res.headers.location)), jar: cookies(res.headers['set-cookie']) };
  }

  function claimsFor(started: Started, email: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
      iss: idp.url,
      aud: CLIENT_ID,
      sub: `sub-${email}`,
      email,
      email_verified: true,
      nonce: started.authorizeUrl.searchParams.get('nonce'),
      exp: Math.floor(Date.now() / 1000) + 300,
      ...overrides,
    };
  }

  async function callback(app: FastifyInstance, started: Started, code: string, state = started.authorizeUrl.searchParams.get('state') ?? '', provider = 'corp') {
    return app.inject({
      method: 'GET',
      url: `/auth/sso/${provider}/callback?code=${encodeURIComponent(code)}&state=${encodeURIComponent(state)}`,
      headers: { cookie: cookieHeader(started.jar), host: 'canopy.test' },
    });
  }

  async function sessionOf(app: FastifyInstance, response: { headers: Record<string, unknown> }) {
    const jar = cookies(response.headers['set-cookie'] as string | string[] | undefined);
    return app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: cookieHeader({ canopy_sid: jar.canopy_sid ?? '' }) } });
  }

  async function loginAs(app: FastifyInstance, email: string) {
    const started = await start(app);
    const code = idp.issueCode(started.authorizeUrl, claimsFor(started, email));
    return callback(app, started, code);
  }

  describe('OIDC login', () => {
    it('redirects to the authorize endpoint with PKCE, state and nonce', async () => {
      const app = await buildApp(makeConfig());

      const { authorizeUrl, jar } = await start(app);

      expect(`${authorizeUrl.origin}${authorizeUrl.pathname}`).toBe(`${idp.url}/authorize`);
      expect(authorizeUrl.searchParams.get('response_type')).toBe('code');
      expect(authorizeUrl.searchParams.get('client_id')).toBe(CLIENT_ID);
      expect(authorizeUrl.searchParams.get('redirect_uri')).toBe('http://canopy.test/auth/sso/corp/callback');
      expect(authorizeUrl.searchParams.get('code_challenge_method')).toBe('S256');
      expect(authorizeUrl.searchParams.get('code_challenge')).toMatch(/^[A-Za-z0-9_-]{43}$/);
      expect(authorizeUrl.searchParams.get('state')).toBeTruthy();
      expect(authorizeUrl.searchParams.get('nonce')).toBeTruthy();
      expect(jar.canopy_sid_sso).toBe(authorizeUrl.searchParams.get('state'));
    });

    it('maps role by email and domain and refuses unmapped users', async () => {
      const app = await buildApp(makeConfig());

      const alice = await loginAs(app, 'Alice@X.com');
      expect(alice.statusCode).toBe(302);
      expect(alice.headers.location).toBe('/');
      const aliceSession = await sessionOf(app, alice);
      expect(aliceSession.statusCode).toBe(200);
      expect(aliceSession.json()).toMatchObject({ user: 'alice@x.com', role: 'admin' });

      const carol = await loginAs(app, 'carol@x.com');
      expect(carol.statusCode).toBe(302);
      expect((await sessionOf(app, carol)).json()).toMatchObject({ user: 'carol@x.com', role: 'readonly' });

      const eve = await loginAs(app, 'eve@y.com');
      expect(eve.statusCode).toBe(403);
      expect(eve.json()).toMatchObject({ error: 'forbidden' });
      expect(cookies(eve.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it('gives an otherwise unmapped user the default roles', async () => {
      const app = await buildApp(makeConfig({ roles: { ...ROLES, defaultRoles: ['readonly'] } }));

      const eve = await loginAs(app, 'eve@y.com');

      expect(eve.statusCode).toBe(302);
      expect((await sessionOf(app, eve)).json()).toMatchObject({ user: 'eve@y.com', role: 'readonly' });
    });

    it('rejects a wrong state and a replayed login without creating a session', async () => {
      const app = await buildApp(makeConfig());

      const wrong = await start(app);
      const wrongCode = idp.issueCode(wrong.authorizeUrl, claimsFor(wrong, 'alice@x.com'));
      const wrongState = await callback(app, wrong, wrongCode, 'not-the-state');
      expect(wrongState.statusCode).toBe(400);
      expect(cookies(wrongState.headers['set-cookie']).canopy_sid).toBeUndefined();

      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com'));
      expect((await callback(app, started, code)).statusCode).toBe(302);
      const replay = await callback(app, started, code);
      expect(replay.statusCode).toBe(400);
      expect(cookies(replay.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it('rejects a callback whose state is not bound to the browser cookie', async () => {
      const app = await buildApp(makeConfig());
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com'));

      const res = await callback(app, { ...started, jar: {} }, code);

      expect(res.statusCode).toBe(400);
    });

    it.each([
      ['nonce', { nonce: 'attacker-nonce' }],
      ['audience', { aud: 'another-client' }],
      ['issuer', { iss: 'https://evil.example' }],
      ['expiry', { exp: Math.floor(Date.now() / 1000) - 3600 }],
    ])('refuses an ID token with a wrong %s', async (_name, override) => {
      const app = await buildApp(makeConfig());
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com', override));

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(401);
      expect(res.json()).toMatchObject({ error: 'invalid_id_token' });
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it('answers 404 for an unknown provider', async () => {
      const app = await buildApp(makeConfig());

      const res = await app.inject({ method: 'GET', url: '/auth/sso/nope/start' });

      expect(res.statusCode).toBe(404);
    });
  });

  describe('OAuth2 login', () => {
    it('resolves the email from the emails endpoint and maps the role', async () => {
      const app = await buildApp(makeConfig({ providers: [oauth2Provider()] }));
      idp.userinfo = { login: 'alice-gh', email: null };
      idp.emails = [
        { email: 'unverified@x.com', primary: false, verified: false },
        { email: 'alice@x.com', primary: true, verified: true },
      ];
      const started = await start(app, 'github');
      const code = idp.issueCode(started.authorizeUrl, {});

      const res = await callback(app, started, code, undefined, 'github');

      expect(res.statusCode).toBe(302);
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'alice@x.com', role: 'admin' });
    });

    it('uses the userinfo email when present', async () => {
      const app = await buildApp(makeConfig({ providers: [oauth2Provider()] }));
      idp.userinfo = { login: 'carol-gh', email: 'carol@x.com' };
      const started = await start(app, 'github');
      const code = idp.issueCode(started.authorizeUrl, {});

      const res = await callback(app, started, code, undefined, 'github');

      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'carol@x.com', role: 'readonly' });
    });
  });

  describe('break-glass', () => {
    const attempt = (app: FastifyInstance, password = BREAK_GLASS_PASSWORD) => app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'ops', password },
      remoteAddress: '10.1.2.3',
    });

    it('refuses the break-glass password while SSO is healthy', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true }));

      const res = await attempt(app);

      expect(res.statusCode).toBe(401);
      expect(res.json()).toMatchObject({ error: 'invalid_credentials' });
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it('accepts the break-glass password while SSO is unhealthy and logs one WARN with the client IP', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true }));
      await idp.stop();

      const res = await attempt(app);

      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({ user: 'ops', role: 'admin' });
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'ops', role: 'admin' });
      const warns = logLines.filter(line => line.level === 40);
      expect(warns).toHaveLength(1);
      expect(warns[0]).toMatchObject({ username: 'ops', ip: '10.1.2.3' });
    });

    it('treats an identity provider that never answers as unhealthy within the timeout', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true, timeoutMs: 300 }));
      idp.hang = true;

      const res = await attempt(app);

      expect(res.statusCode).toBe(200);
    });

    it('logs a failure at INFO without the password, even while SSO is unhealthy', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true }));
      await idp.stop();

      const res = await attempt(app, 'wrong-password');

      expect(res.statusCode).toBe(401);
      expect(res.json()).toMatchObject({ error: 'invalid_credentials' });
      expect(logLines.filter(line => line.level === 40)).toHaveLength(0);
      expect(logLines.filter(line => line.level === 30 && line.username === 'ops')).toHaveLength(1);
      expect(JSON.stringify(logLines)).not.toContain('wrong-password');
    });

    it('is off unless auth.breakGlass is enabled', async () => {
      const app = await buildApp(makeConfig());
      await idp.stop();

      const res = await attempt(app);

      expect(res.statusCode).toBe(401);
    });
  });

  describe('meta', () => {
    it('lists the providers, SSO health and break-glass availability for the login page', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true }));

      const healthy = await app.inject({ method: 'GET', url: '/api/meta' });
      expect(healthy.json().auth).toEqual({
        backend: 'sso',
        providers: [{ id: 'corp', name: 'Corp SSO', type: 'oidc' }],
        ssoHealthy: true,
        breakGlass: true,
      });

      idp.hang = true;
      const unhealthy = await app.inject({ method: 'GET', url: '/api/meta' });
      expect(unhealthy.json().auth).toMatchObject({ ssoHealthy: false, breakGlass: true });
    }, 15000);
  });

  describe('ID token validation', () => {
    it('accepts an ES256 token signed with the provider EC key', async () => {
      const app = await buildApp(makeConfig());
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com'), { alg: 'ES256' });

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(302);
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'alice@x.com', role: 'admin' });
    });

    it.each([
      ['an unsupported algorithm (HS256)', { alg: 'HS256' }, {}],
      ['the none algorithm', { alg: 'none' }, {}],
      ['a signature from an unknown key', { signWith: generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey }, {}],
      ['a not-yet-valid nbf', {}, { nbf: Math.floor(Date.now() / 1000) + 3600 }],
      ['a missing subject', {}, { sub: undefined }],
      ['an authorized party that is not this client', {}, { azp: 'another-client' }],
      ['several audiences without an authorized party', {}, { aud: [CLIENT_ID, 'another-client'] }],
    ])('refuses an ID token with %s', async (_name, options, override) => {
      const app = await buildApp(makeConfig());
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com', override), options);

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(401);
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it('accepts several audiences when the authorized party is this client', async () => {
      const app = await buildApp(makeConfig());
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com', { aud: [CLIENT_ID, 'other'], azp: CLIENT_ID }));

      expect((await callback(app, started, code)).statusCode).toBe(302);
    });
  });

  describe('OIDC userinfo fallback', () => {
    const noEmail = { email: undefined, email_verified: undefined };

    it('reads the email from userinfo when the ID token has none', async () => {
      const app = await buildApp(makeConfig());
      idp.userinfo = { sub: 'sub-alice@x.com', email: 'alice@x.com', email_verified: true };
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com', noEmail));

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(302);
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'alice@x.com', role: 'admin' });
    });

    it('ignores a userinfo answer for a different subject', async () => {
      const app = await buildApp(makeConfig());
      idp.userinfo = { sub: 'someone-else', email: 'alice@x.com', email_verified: true };
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'bob@y.com', noEmail));

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(403);
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
    });

    it.each([
      ['userinfo', noEmail, { email: 'alice@x.com', email_verified: false }],
      ['the ID token', { email: 'alice@x.com', email_verified: false }, {}],
    ])('never uses an email that %s marks unverified', async (_name, claimOverride, userinfo) => {
      const app = await buildApp(makeConfig());
      idp.userinfo = { sub: 'sub-alice@x.com', ...userinfo };
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com', claimOverride));

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(403);
    });
  });

  describe('OAuth2 email verification', () => {
    it('falls back to the verified address list when userinfo marks its email unverified', async () => {
      const app = await buildApp(makeConfig({ providers: [oauth2Provider()] }));
      idp.userinfo = { login: 'mallory', email: 'alice@x.com', email_verified: false };
      idp.emails = [{ email: 'mallory@y.com', primary: true, verified: true }];
      const started = await start(app, 'github');
      const code = idp.issueCode(started.authorizeUrl, {});

      const res = await callback(app, started, code, undefined, 'github');

      expect(res.statusCode).toBe(403);
      expect(idp.hits['/emails']).toBe(1);
    });
  });

  describe('token endpoint client authentication', () => {
    it.each([
      ['advertises only client_secret_basic', ['client_secret_basic'], 'basic'],
      ['advertises only client_secret_post', ['client_secret_post'], 'post'],
      ['advertises both', ['client_secret_basic', 'client_secret_post'], 'post'],
      ['advertises nothing (the OIDC default is basic)', undefined, 'basic'],
    ])('uses the supported method when the provider %s', async (_name, methods, expected) => {
      idp.advertisedAuthMethods = methods;
      const app = await buildApp(makeConfig());

      const res = await loginAs(app, 'alice@x.com');

      expect(res.statusCode).toBe(302);
      expect(idp.lastTokenAuth).toBe(expected);
    });
  });

  describe('signing keys', () => {
    it('refetches the JWKS after a signature fails against a rotated key with the same kid', async () => {
      const app = await buildApp(makeConfig());
      expect((await loginAs(app, 'alice@x.com')).statusCode).toBe(302);
      expect(idp.hits['/jwks']).toBe(1);

      idp.rotateKey();
      const rotated = await loginAs(app, 'alice@x.com');

      expect(rotated.statusCode).toBe(302);
      expect(idp.hits['/jwks']).toBe(2);
    });
  });

  describe('identity provider bearer tokens', () => {
    const bearer = (app: FastifyInstance, token: string, url = '/me') => app.inject({ method: 'GET', url, headers: { authorization: `Bearer ${token}` } });

    beforeEach(() => {
      idp.accessTokens.set('idp-token', 'ok');
      idp.userinfo = { sub: 's-1', email: 'alice@x.com', email_verified: true };
    });

    it('authenticates a provider access token, caches it for 30 s and creates no session', async () => {
      const app = await buildApp(makeConfig());

      const first = await bearer(app, 'idp-token');
      const second = await bearer(app, 'idp-token');

      expect(first.statusCode).toBe(200);
      expect(first.json()).toEqual({ user: 'alice@x.com', role: 'admin', source: 'token' });
      expect(second.statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(1);
      expect(first.headers['set-cookie']).toBeUndefined();
      expect((await bearer(app, 'idp-token', '/auth/session')).statusCode).toBe(401);
    });

    it('refuses a token the provider does not recognise, and does not cache the failure', async () => {
      const app = await buildApp(makeConfig());

      expect((await bearer(app, 'forged')).statusCode).toBe(401);
      expect((await bearer(app, 'forged')).statusCode).toBe(401);
      expect(idp.hits['/userinfo']).toBe(2);
    });

    it('refuses a token whose owner has no role', async () => {
      const app = await buildApp(makeConfig());
      idp.userinfo = { sub: 's-2', email: 'eve@y.com', email_verified: true };

      expect((await bearer(app, 'idp-token')).statusCode).toBe(401);
    });

    it('never sends a Canopy API token (hzn_) to the provider', async () => {
      const app = await buildApp(makeConfig());

      const res = await bearer(app, 'hzn_t1_secret');

      expect(res.statusCode).toBe(401);
      expect(idp.hits['/userinfo']).toBeUndefined();
    });
  });

  describe('identity provider bearer lifecycle', () => {
    const SECONDS = 1000;
    const bearer = (app: FastifyInstance, token: string, extra: Record<string, string> = {}, url = '/me') =>
      app.inject({ method: 'GET', url, headers: { authorization: `Bearer ${token}`, ...extra } });
    let policy: AuthPolicy;

    beforeEach(() => {
      policy = { roles: ROLES, breakGlass: undefined };
      idp.accessTokens.set('idp-token', 'ok');
      idp.userinfo = { sub: 's-1', email: 'alice@x.com', email_verified: true };
      vi.useFakeTimers({ toFake: ['Date'] });
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('falls back to the cookie session on a browser-only route when a valid provider token rides along', async () => {
      const app = await buildApp(makeConfig());
      const login = await loginAs(app, 'carol@x.com');
      const jar = cookies(login.headers['set-cookie']);

      const both = await bearer(app, 'idp-token', { cookie: cookieHeader({ canopy_sid: jar.canopy_sid ?? '' }) }, '/auth/session');
      const cookieOnly = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: cookieHeader({ canopy_sid: jar.canopy_sid ?? '' }) } });

      expect(cookieOnly.statusCode).toBe(200);
      expect(both.statusCode).toBe(200);
      // The principal is the browser session, never the provider token's own identity.
      expect(both.json()).toMatchObject({ user: 'carol@x.com', role: 'readonly' });
    });

    it('still refuses an invalid provider token without a cookie and never treats a valid one as a browser session', async () => {
      const app = await buildApp(makeConfig());

      expect((await bearer(app, 'forged', {}, '/auth/session')).statusCode).toBe(401);
      expect((await bearer(app, 'idp-token', {}, '/auth/session')).statusCode).toBe(401);
    });

    it('does not cache a role-denied token: every denied request asks the provider again', async () => {
      const app = await buildApp(makeConfig());
      idp.userinfo = { sub: 's-2', email: 'eve@y.com', email_verified: true };

      expect((await bearer(app, 'idp-token')).statusCode).toBe(401);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(401);

      expect(idp.hits['/userinfo']).toBe(2);
    });

    it('drops a cached identity as soon as its role is refused', async () => {
      const app = await buildApp(makeConfig({ reloadPolicy: () => policy }));
      expect((await bearer(app, 'idp-token')).statusCode).toBe(200);

      policy = { roles: { defaultRoles: [], roleByEmail: {}, roleByDomain: {} }, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(401);

      policy = { roles: ROLES, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(200);
      // The refused request evicted the entry, so the recovered token was identified again.
      expect(idp.hits['/userinfo']).toBe(2);
    });

    it('resolves the live role of a cached identity: a downgrade applies and a removal refuses the next request', async () => {
      const app = await buildApp(makeConfig({ reloadPolicy: () => policy }));
      expect((await bearer(app, 'idp-token')).json()).toMatchObject({ role: 'admin' });

      policy = { roles: { ...ROLES, roleByEmail: { 'alice@x.com': ['readonly'] } }, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      const downgraded = await bearer(app, 'idp-token');
      expect(downgraded.json()).toMatchObject({ user: 'alice@x.com', role: 'readonly' });

      policy = { roles: { defaultRoles: [], roleByEmail: {}, roleByDomain: {} }, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(401);
      expect(idp.hits['/userinfo']).toBe(1);
    });

    it('expires a cached identity after 30 s', async () => {
      const app = await buildApp(makeConfig());

      expect((await bearer(app, 'idp-token')).statusCode).toBe(200);
      vi.setSystemTime(Date.now() + 29 * SECONDS);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(1);

      vi.setSystemTime(Date.now() + 2 * SECONDS);
      expect((await bearer(app, 'idp-token')).statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(2);
    });

    it('evicts the oldest identity once the cache holds 1000 entries', async () => {
      const app = await buildApp(makeConfig());
      for (let i = 0; i < 1001; i += 1) idp.accessTokens.set(`t-${i}`, 'ok');
      for (let i = 0; i < 1001; i += 1) expect((await bearer(app, `t-${i}`)).statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(1001);

      // The newest entries are still cached; the oldest was evicted and is identified again.
      expect((await bearer(app, 't-1000')).statusCode).toBe(200);
      expect((await bearer(app, 't-1')).statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(1001);
      expect((await bearer(app, 't-0')).statusCode).toBe(200);
      expect(idp.hits['/userinfo']).toBe(1002);
    });

    it('identifies a token through an OAuth2 provider userinfo endpoint, with the verified-address fallback', async () => {
      const app = await buildApp(makeConfig({ providers: [oauth2Provider()] }));
      idp.userinfo = { login: 'alice-gh', email: null };
      idp.emails = [{ email: 'alice@x.com', primary: true, verified: true }];

      const res = await bearer(app, 'idp-token');

      expect(res.json()).toEqual({ user: 'alice@x.com', role: 'admin', source: 'token' });
      expect(idp.hits['/emails']).toBe(1);
    });

    it('uses the OIDC userinfo endpoint and a provider-scoped name when the token owner has no email', async () => {
      const app = await buildApp(makeConfig({ roles: { ...ROLES, defaultRoles: ['readonly'] } }));
      idp.userinfo = { sub: 's-9', preferred_username: 'nomail' };

      const res = await bearer(app, 'idp-token');

      expect(res.json()).toEqual({ user: 'corp:nomail', role: 'readonly', source: 'token' });
    });

    it('tries the next provider when the first does not accept the token', async () => {
      const other = new MockIdp();
      await other.start();
      try {
        const app = await buildApp(makeConfig({ providers: [oidcProvider(other, 'other'), oauth2Provider()] }));
        idp.userinfo = { login: 'carol-gh', email: 'carol@x.com' };

        const res = await bearer(app, 'idp-token');

        expect(res.json()).toMatchObject({ user: 'carol@x.com', role: 'readonly' });
        expect(other.hits['/userinfo']).toBe(1);
      } finally {
        await other.stop();
      }
    });
  });

  describe('login without an email', () => {
    const SECONDS = 1000;
    let policy: AuthPolicy;

    beforeEach(() => {
      policy = { roles: { defaultRoles: ['readonly'], roleByEmail: {}, roleByDomain: {} }, breakGlass: undefined };
      vi.useFakeTimers({ toFake: ['Date'] });
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('names the session <provider>:<sub>, takes the role from defaultRoles and re-resolves it', async () => {
      const app = await buildApp(makeConfig({ roles: policy.roles as SsoRolesConfig, reloadPolicy: () => policy }));
      idp.userinfo = {};
      const started = await start(app);
      const claims = claimsFor(started, 'ignored@x.com', { sub: 'abc-123', email: undefined, email_verified: undefined });
      const code = idp.issueCode(started.authorizeUrl, claims);

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(302);
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'corp:abc-123', role: 'readonly' });

      policy = { roles: { defaultRoles: ['admin'], roleByEmail: {}, roleByDomain: {} }, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      expect((await sessionOf(app, res)).json()).toMatchObject({ user: 'corp:abc-123', role: 'admin' });

      policy = { roles: { defaultRoles: [], roleByEmail: {}, roleByDomain: {} }, breakGlass: undefined };
      vi.setSystemTime(Date.now() + 6 * SECONDS);
      expect((await sessionOf(app, res)).statusCode).toBe(401);
    });

    it('refuses an OAuth2 response that carries no usable identity', async () => {
      const app = await buildApp(makeConfig({ providers: [oauth2Provider()], roles: policy.roles as SsoRolesConfig }));
      idp.userinfo = { email: null, name: 'Anonymous' };
      idp.emails = [];
      const started = await start(app, 'github');
      const code = idp.issueCode(started.authorizeUrl, {});

      const res = await callback(app, started, code, undefined, 'github');

      expect(res.statusCode).toBe(401);
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
    });
  });

  describe('live policy', () => {
    const SECONDS = 1000;
    let policy: AuthPolicy;

    beforeEach(() => {
      policy = {
        roles: ROLES,
        breakGlass: { enabled: true, users: [{ username: 'ops', passwordHash: breakGlassHash, role: 'admin' }] },
      };
      vi.useFakeTimers({ toFake: ['Date'] });
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('revokes an SSO session when the role mapping stops granting access', async () => {
      const app = await buildApp(makeConfig({ reloadPolicy: () => policy }));
      const login = await loginAs(app, 'alice@x.com');
      expect((await sessionOf(app, login)).statusCode).toBe(200);

      policy = { ...policy, roles: { defaultRoles: [], roleByEmail: {}, roleByDomain: {} } };
      vi.setSystemTime(Date.now() + 6 * SECONDS);

      expect((await sessionOf(app, login)).statusCode).toBe(401);
    });

    it('applies a role change to an SSO session without a new login', async () => {
      const app = await buildApp(makeConfig({ reloadPolicy: () => policy }));
      const login = await loginAs(app, 'alice@x.com');

      policy = { ...policy, roles: { ...ROLES, roleByEmail: { 'alice@x.com': ['readonly'] } } };
      vi.setSystemTime(Date.now() + 6 * SECONDS);

      expect((await sessionOf(app, login)).json()).toMatchObject({ role: 'readonly' });
    });

    it('revokes break-glass sessions and refuses new break-glass logins once it is disabled', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true, reloadPolicy: () => policy }));
      await idp.stop();
      const login = await app.inject({ method: 'POST', url: '/auth/login', payload: { username: 'ops', password: BREAK_GLASS_PASSWORD } });
      expect(login.statusCode).toBe(200);
      expect((await sessionOf(app, login)).statusCode).toBe(200);

      policy = { ...policy, breakGlass: { enabled: false, users: policy.breakGlass?.users ?? [] } };
      vi.setSystemTime(Date.now() + 6 * SECONDS);

      expect((await sessionOf(app, login)).statusCode).toBe(401);
      const again = await app.inject({ method: 'POST', url: '/auth/login', payload: { username: 'ops', password: BREAK_GLASS_PASSWORD } });
      expect(again.statusCode).toBe(401);
    });

    it('keeps the previous policy when a reload fails', async () => {
      let broken = false;
      const app = await buildApp(makeConfig({
        reloadPolicy: () => {
          if (broken) throw new Error('half-written file');
          return policy;
        },
      }));
      const login = await loginAs(app, 'alice@x.com');

      broken = true;
      vi.setSystemTime(Date.now() + 6 * SECONDS);

      expect((await sessionOf(app, login)).statusCode).toBe(200);
    });
  });

  describe('state and health lifecycles', () => {
    let clock: number;
    const MINUTE = 60_000;

    // The service runs with its production cache policy and a clock the test moves.
    // Imported lazily so the rest of this file loads against a tree without the SSO module.
    const service = async (): Promise<SsoService> => {
      const { SsoService: Service } = await import('../src/lib/sso.js');
      return new Service([oidcProvider()], 2000, undefined, () => clock);
    };

    beforeEach(() => {
      clock = Date.now();
    });

    async function loginThrough(svc: SsoService): Promise<void> {
      const { url, state } = await svc.begin('corp', 'http://canopy.test/cb');
      const authorizeUrl = new URL(url);
      const code = idp.issueCode(authorizeUrl, {
        iss: idp.url,
        aud: CLIENT_ID,
        sub: 's',
        email: 'alice@x.com',
        nonce: authorizeUrl.searchParams.get('nonce'),
        exp: Math.floor(clock / 1000) + 300,
      });
      await svc.complete('corp', state, state, code);
    }

    it('refuses a login that sat unfinished past its lifetime', async () => {
      const svc = await service();
      const { state } = await svc.begin('corp', 'http://canopy.test/cb');

      clock += 11 * MINUTE;

      await expect(svc.complete('corp', state, state, 'code')).rejects.toMatchObject({ status: 400, code: 'invalid_state' });
    });

    it('evicts the oldest pending login when the map is full', async () => {
      const svc = await service();
      const states: string[] = [];
      for (let i = 0; i < 1001; i += 1) states.push((await svc.begin('corp', 'http://canopy.test/cb')).state);

      await expect(svc.complete('corp', states[0], states[0], 'code')).rejects.toMatchObject({ status: 400, code: 'invalid_state' });
      // The newest login is still pending: it passes the state check and fails later, at the token endpoint.
      await expect(svc.complete('corp', states[1000], states[1000], 'code')).rejects.toMatchObject({ status: 401, code: 'invalid_code' });
    });

    it('reuses a health result for the cache lifetime and probes again after it', async () => {
      const svc = await service();

      expect(await svc.healthy()).toBe(true);
      expect(await svc.healthy()).toBe(true);
      expect(idp.hits['/.well-known/openid-configuration']).toBe(1);

      clock += 6000;
      expect(await svc.healthy()).toBe(true);
      expect(idp.hits['/.well-known/openid-configuration']).toBe(2);
    });

    it('coalesces concurrent health probes into one request', async () => {
      const svc = await service();

      const results = await Promise.all([svc.healthy(), svc.healthy(), svc.healthy(), svc.healthy()]);

      expect(results).toEqual([true, true, true, true]);
      expect(idp.hits['/.well-known/openid-configuration']).toBe(1);
    });

    it('stops trusting a key the provider no longer publishes once the cached JWKS ages out', async () => {
      const svc = await service();
      await loginThrough(svc);
      const retired = idp.keys.privateKey;
      idp.rotateKey();
      clock += 11 * MINUTE;
      const { url, state } = await svc.begin('corp', 'http://canopy.test/cb');
      const authorizeUrl = new URL(url);
      const code = idp.issueCode(authorizeUrl, {
        iss: idp.url,
        aud: CLIENT_ID,
        sub: 's',
        email: 'alice@x.com',
        nonce: authorizeUrl.searchParams.get('nonce'),
        exp: Math.floor(clock / 1000) + 300,
      }, { signWith: retired });

      await expect(svc.complete('corp', state, state, code)).rejects.toMatchObject({ status: 401, code: 'invalid_id_token' });
    });

    it('reuses the JWKS until it ages out', async () => {
      const svc = await service();

      await loginThrough(svc);
      await loginThrough(svc);
      expect(idp.hits['/jwks']).toBe(1);

      clock += 11 * MINUTE;
      await loginThrough(svc);
      expect(idp.hits['/jwks']).toBe(2);
    });
  });

  describe('provider response limits', () => {
    it.each([
      ['a body larger than the size cap', 'huge'],
      ['a body that stalls after the headers', 'stall'],
    ] as const)('refuses a login whose token response is %s, and rejects its replay', async (_name, mode) => {
      const app = await buildApp(makeConfig({ timeoutMs: 400 }));
      const started = await start(app);
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com'));
      idp.tokenMode = mode;

      const res = await callback(app, started, code);

      expect(res.statusCode).toBe(502);
      expect(res.json()).toMatchObject({ error: 'sso_unavailable' });
      expect(cookies(res.headers['set-cookie']).canopy_sid).toBeUndefined();
      const replay = await callback(app, started, code);
      expect(replay.statusCode).toBe(400);
      expect(cookies(replay.headers['set-cookie']).canopy_sid).toBeUndefined();
    });
  });

  describe('re-authentication', () => {
    it('revokes the session the browser held when it started the SSO login', async () => {
      const app = await buildApp(makeConfig());
      const first = await loginAs(app, 'alice@x.com');
      const firstJar = cookies(first.headers['set-cookie']);
      expect(firstJar.canopy_sid).toBeTruthy();

      // Start from a browser that holds the first session, then return through the cross-site
      // callback, which carries only the SameSite=Lax state cookie.
      const startRes = await app.inject({ method: 'GET', url: '/auth/sso/corp/start', headers: { host: 'canopy.test', cookie: `canopy_sid=${firstJar.canopy_sid}` } });
      const started: Started = { authorizeUrl: new URL(String(startRes.headers.location)), jar: cookies(startRes.headers['set-cookie']) };
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'carol@x.com'));
      const second = await callback(app, started, code);

      expect(second.statusCode).toBe(302);
      const secondJar = cookies(second.headers['set-cookie']);
      expect(secondJar.canopy_sid).toBeTruthy();
      expect(secondJar.canopy_sid).not.toBe(firstJar.canopy_sid);
      const old = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: `canopy_sid=${firstJar.canopy_sid}` } });
      expect(old.statusCode).toBe(401);
      const current = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: `canopy_sid=${secondJar.canopy_sid}` } });
      expect(current.json()).toMatchObject({ user: 'carol@x.com' });
      await app.inject({ method: 'POST', url: '/auth/logout', headers: { cookie: `canopy_sid=${secondJar.canopy_sid}` } });
      const after = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: `canopy_sid=${secondJar.canopy_sid}` } });
      expect(after.statusCode).toBe(401);
    });
  });

  describe('break-glass with several or OAuth2 providers', () => {
    let second: MockIdp;

    beforeEach(async () => {
      second = new MockIdp();
      await second.start();
    });

    afterEach(async () => {
      await second.stop();
    });

    const attempt = (app: FastifyInstance) => app.inject({ method: 'POST', url: '/auth/login', payload: { username: 'ops', password: BREAK_GLASS_PASSWORD } });

    it.each([
      ['a reachable token endpoint that answers 404', { tokenGetStatus: 404 }, 401],
      ['a token endpoint that answers 400', { tokenGetStatus: 400 }, 401],
      ['a token endpoint that answers 500', { tokenGetStatus: 500 }, 200],
      ['a token endpoint that never answers', { hang: true }, 200],
    ] as const)('classifies an OAuth2 provider with %s', async (_name, state, expected) => {
      Object.assign(idp, state);
      const app = await buildApp(makeConfig({ breakGlass: true, providers: [oauth2Provider()], timeoutMs: 300 }));

      expect((await attempt(app)).statusCode).toBe(expected);
    });

    it('counts a provider that has stopped as unhealthy', async () => {
      const app = await buildApp(makeConfig({ breakGlass: true, providers: [oauth2Provider()], timeoutMs: 300 }));
      await idp.stop();

      expect((await attempt(app)).statusCode).toBe(200);
    });

    it.each([
      ['the first provider is healthy and the second is down', true, false, 401],
      ['the first provider is down and the second is healthy', false, true, 401],
      ['both providers are down', false, false, 200],
    ] as const)('with two providers, break-glass is gated when %s', async (_name, firstUp, secondUp, expected) => {
      const app = await buildApp(makeConfig({ breakGlass: true, providers: [oidcProvider(idp, 'one'), oauth2Provider(second, 'two')], timeoutMs: 300 }));
      if (!firstUp) await idp.stop();
      if (!secondUp) await second.stop();

      expect((await attempt(app)).statusCode).toBe(expected);
    });
  });

  describe('empty bearer credentials', () => {
    it.each([['a bare Bearer', 'Bearer'], ['a whitespace-only Bearer', 'Bearer   ']])('rejects %s instead of falling back to a valid session cookie', async (_name, header) => {
      const app = await buildApp(makeConfig());
      const login = await loginAs(app, 'alice@x.com');
      const jar = cookies(login.headers['set-cookie']);
      const cookie = `canopy_sid=${jar.canopy_sid}`;

      expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie } })).statusCode).toBe(200);
      expect((await app.inject({ method: 'GET', url: '/me', headers: { cookie, authorization: header } })).statusCode).toBe(401);
      expect((await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie, authorization: header } })).statusCode).toBe(401);
    });
  });

  describe('behind a mount prefix and an HTTPS proxy', () => {
    const PREFIX = '/canopy';

    async function buildPrefixed(config: Config): Promise<FastifyInstance> {
      const app = Fastify({ trustProxy: true });
      apps.push(app);
      await app.register(async scoped => {
        await registerSession(scoped, config);
        await registerAuth(scoped, config, { ssoHealthTtlMs: 0 });
      }, { prefix: PREFIX });
      return app;
    }

    const prefixedConfig = (providers?: SsoProviderConfig[]): Config => ({ ...makeConfig({ providers }), basePath: PREFIX, baseHref: `${PREFIX}/` });
    const proxied = { host: 'canopy.test', 'x-forwarded-proto': 'https' };

    it('derives the callback URL from the forwarded protocol, host and mount path, and scopes the state cookie to it', async () => {
      const app = await buildPrefixed(prefixedConfig());

      const res = await app.inject({ method: 'GET', url: `${PREFIX}/auth/sso/corp/start`, headers: proxied });

      expect(res.statusCode).toBe(302);
      const authorizeUrl = new URL(String(res.headers.location));
      expect(authorizeUrl.searchParams.get('redirect_uri')).toBe('https://canopy.test/canopy/auth/sso/corp/callback');
      const stateCookie = String(([] as string[]).concat(res.headers['set-cookie'] ?? []).find(c => c.startsWith('canopy_sid_sso=')));
      expect(stateCookie).toContain('Path=/canopy/');
      expect(stateCookie).toContain('Secure');
      expect(stateCookie).toContain('HttpOnly');
      expect(stateCookie).toContain('SameSite=Lax');
    });

    it('completes the login through the prefix and sets a prefix-scoped session cookie', async () => {
      const app = await buildPrefixed(prefixedConfig());
      const startRes = await app.inject({ method: 'GET', url: `${PREFIX}/auth/sso/corp/start`, headers: proxied });
      const started: Started = { authorizeUrl: new URL(String(startRes.headers.location)), jar: cookies(startRes.headers['set-cookie']) };
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'alice@x.com'));

      const res = await app.inject({
        method: 'GET',
        url: `${PREFIX}/auth/sso/corp/callback?code=${code}&state=${started.authorizeUrl.searchParams.get('state')}`,
        headers: { ...proxied, cookie: cookieHeader(started.jar) },
      });

      expect(res.statusCode).toBe(302);
      expect(res.headers.location).toBe('/canopy/');
      const sessionCookie = String(([] as string[]).concat(res.headers['set-cookie'] ?? []).find(c => c.startsWith('canopy_sid=')));
      expect(sessionCookie).toContain('Path=/canopy/');
      expect(sessionCookie).toContain('Secure');
    });

    it('uses an explicit redirectUri instead of deriving one', async () => {
      const explicit = 'https://public.example/tools/canopy/auth/sso/corp/callback';
      const app = await buildPrefixed(prefixedConfig([{ ...oidcProvider(), redirectUri: explicit }]));

      const res = await app.inject({ method: 'GET', url: `${PREFIX}/auth/sso/corp/start`, headers: proxied });

      expect(new URL(String(res.headers.location)).searchParams.get('redirect_uri')).toBe(explicit);
    });
  });

  describe('a login the provider denied', () => {
    it('consumes the pending state, clears the state cookie, keeps the existing session and allows a new login', async () => {
      const app = await buildApp(makeConfig());
      const first = await loginAs(app, 'alice@x.com');
      const sessionCookie = `canopy_sid=${cookies(first.headers['set-cookie']).canopy_sid}`;

      const startRes = await app.inject({ method: 'GET', url: '/auth/sso/corp/start', headers: { host: 'canopy.test', cookie: sessionCookie } });
      const started: Started = { authorizeUrl: new URL(String(startRes.headers.location)), jar: cookies(startRes.headers['set-cookie']) };
      const state = started.authorizeUrl.searchParams.get('state') ?? '';
      const denied = await app.inject({
        method: 'GET',
        url: `/auth/sso/corp/callback?error=access_denied&state=${encodeURIComponent(state)}`,
        headers: { host: 'canopy.test', cookie: `${sessionCookie}; ${cookieHeader(started.jar)}` },
      });

      expect(denied.statusCode).toBe(400);
      expect(denied.json()).toMatchObject({ error: 'sso_denied' });
      const cleared = String(([] as string[]).concat(denied.headers['set-cookie'] ?? []).find(c => c.startsWith('canopy_sid_sso=')));
      expect(cleared).toMatch(/canopy_sid_sso=;|Expires=Thu, 01 Jan 1970/);
      expect(cleared).not.toContain(state);
      // The session the browser already held is untouched.
      const still = await app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: sessionCookie } });
      expect(still.statusCode).toBe(200);
      // The abandoned state cannot be replayed, even with a code that would have worked.
      const code = idp.issueCode(started.authorizeUrl, claimsFor(started, 'carol@x.com'));
      const replay = await callback(app, started, code);
      expect(replay.statusCode).toBe(400);
      expect(replay.json()).toMatchObject({ error: 'invalid_state' });
      // A fresh login still works afterwards.
      const retry = await loginAs(app, 'carol@x.com');
      expect(retry.statusCode).toBe(302);
      expect((await sessionOf(app, retry)).json()).toMatchObject({ user: 'carol@x.com' });
    });
  });
});
