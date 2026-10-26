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

import { createHash, createPublicKey, randomBytes, timingSafeEqual, verify as cryptoVerify } from 'node:crypto';
import type { JsonWebKey } from 'node:crypto';
import { request as undiciRequest } from 'undici';

import type { SsoProviderConfig, SsoRolesConfig } from '../config.js';
import type { Role } from './role.js';

/** A failed SSO step, carrying the HTTP status and machine-readable code the route answers with. */
export class SsoError extends Error {
  constructor(readonly status: 400 | 401 | 403 | 502, readonly code: string, message: string) {
    super(message);
    this.name = 'SsoError';
  }
}

/** The principal an identity provider vouched for. */
export interface SsoIdentity {
  readonly providerId: string;
  readonly username: string;
  readonly email?: string;
}

/** A completed login, plus the session id the browser held when it started (to revoke on success). */
export interface SsoLogin extends SsoIdentity {
  readonly previousSessionId?: string;
}

/** The provider fields the login page may show. */
export interface SsoProviderInfo {
  readonly id: string;
  readonly name: string;
  readonly type: SsoProviderConfig['type'];
}

/**
 * Resolves the Canopy role of an SSO login: an exact (case-insensitive) email entry wins over a
 * domain entry, which wins over defaultRoles. No match gives undefined and the login is refused.
 * When a match lists several roles, admin wins.
 */
export function resolveSsoRole(roles: SsoRolesConfig, email: string | undefined): Role | undefined {
  const address = email?.trim().toLowerCase();
  let matched: readonly Role[] | undefined;
  if (address) {
    if (Object.hasOwn(roles.roleByEmail, address)) {
      matched = roles.roleByEmail[address];
    } else if (address.includes('@')) {
      const domain = address.slice(address.lastIndexOf('@') + 1);
      if (Object.hasOwn(roles.roleByDomain, domain)) matched = roles.roleByDomain[domain];
    }
  }
  matched ??= roles.defaultRoles;
  if (matched.length === 0) return undefined;
  return matched.includes('admin') ? 'admin' : 'readonly';
}

const MAX_BODY_BYTES = 1024 * 1024;
const PENDING_TTL_MS = 10 * 60_000;
const MAX_PENDING = 1000;
const METADATA_TTL_MS = 10 * 60_000;
const CLOCK_SKEW_SECONDS = 60;
const JWKS_TTL_MS = 10 * 60_000;
const USER_AGENT = 'canopy-sso';

type Json = Record<string, unknown>;

function isJson(value: unknown): value is Json {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

interface HttpResult {
  readonly status: number;
  readonly body: unknown;
}

// Every identity-provider call is bounded: the signal also covers reading the body, so a
// server that stalls mid-response cannot hold a login request open.
async function httpJson(
  url: string,
  timeoutMs: number,
  init: { method?: 'GET' | 'POST'; headers?: Record<string, string>; form?: Record<string, string> } = {},
): Promise<HttpResult> {
  const headers: Record<string, string> = { accept: 'application/json', 'user-agent': USER_AGENT, ...init.headers };
  let body: string | undefined;
  if (init.form) {
    body = new URLSearchParams(init.form).toString();
    headers['content-type'] = 'application/x-www-form-urlencoded';
  }
  const res = await undiciRequest(url, { method: init.method ?? 'GET', headers, body, signal: AbortSignal.timeout(timeoutMs) });
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of res.body) {
    const buf = Buffer.from(chunk as Uint8Array);
    size += buf.length;
    if (size > MAX_BODY_BYTES) {
      res.body.destroy();
      throw new Error('response body too large');
    }
    chunks.push(buf);
  }
  const text = Buffer.concat(chunks).toString('utf-8');
  let parsed: unknown;
  try {
    parsed = text === '' ? undefined : JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  return { status: res.statusCode, body: parsed };
}

interface OidcMetadata {
  readonly issuer: string;
  readonly authorizationEndpoint: string;
  readonly tokenEndpoint: string;
  readonly jwksUri: string;
  readonly userinfoEndpoint?: string;
  // How to present the client secret at the token endpoint; client_secret_basic is the OIDC default.
  readonly tokenAuth: 'basic' | 'post';
}

interface Pending {
  readonly providerId: string;
  readonly nonce: string;
  readonly verifier: string;
  readonly redirectUri: string;
  readonly previousSessionId?: string;
  readonly expiresAt: number;
}

const b64url = (buf: Buffer): string => buf.toString('base64url');

function safeEqual(a: string, b: string): boolean {
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value !== '' ? value : undefined;
}

/**
 * Runs the authorization-code flow (PKCE, state, nonce) against the configured OIDC and OAuth2
 * providers and probes their health. In-flight logins live in a bounded in-memory map that is
 * swept on insert, so the service owns no timer; a restart drops pending logins.
 */
export class SsoService {
  private readonly byId: ReadonlyMap<string, SsoProviderConfig>;
  private readonly pending = new Map<string, Pending>();
  private readonly metadata = new Map<string, { value: OidcMetadata; fetchedAt: number }>();
  private readonly jwks = new Map<string, { keys: readonly JsonWebKey[]; fetchedAt: number }>();
  private health: { healthy: boolean; checkedAt: number } | undefined;
  private healthInflight: Promise<boolean> | undefined;

  constructor(
    private readonly providers: readonly SsoProviderConfig[],
    private readonly timeoutMs: number,
    private readonly healthTtlMs = 5000,
    private readonly now: () => number = Date.now,
  ) {
    this.byId = new Map(providers.map(p => [p.id, p]));
  }

  list(): SsoProviderInfo[] {
    return this.providers.map(({ id, name, type }) => ({ id, name, type }));
  }

  has(id: string): boolean {
    return this.byId.has(id);
  }

  redirectUriFor(id: string): string | undefined {
    return this.byId.get(id)?.redirectUri;
  }

  /** Starts a login: returns the provider's authorize URL and the state that binds the callback to this browser. */
  async begin(id: string, redirectUri: string, previousSessionId?: string): Promise<{ url: string; state: string }> {
    const provider = this.provider(id);
    const authorizeUrl = provider.type === 'oidc' ? (await this.discover(provider)).authorizationEndpoint : provider.authorizeUrl as string;
    this.sweepPending();
    while (this.pending.size >= MAX_PENDING) {
      const oldest = this.pending.keys().next();
      if (oldest.done) break;
      this.pending.delete(oldest.value);
    }
    const state = b64url(randomBytes(24));
    const nonce = b64url(randomBytes(24));
    const verifier = b64url(randomBytes(32));
    this.pending.set(state, { providerId: id, nonce, verifier, redirectUri, previousSessionId, expiresAt: this.now() + PENDING_TTL_MS });

    const url = new URL(authorizeUrl);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('client_id', provider.clientId);
    url.searchParams.set('redirect_uri', redirectUri);
    url.searchParams.set('scope', provider.scopes.join(' '));
    url.searchParams.set('state', state);
    if (provider.type === 'oidc') url.searchParams.set('nonce', nonce);
    url.searchParams.set('code_challenge', b64url(createHash('sha256').update(verifier).digest()));
    url.searchParams.set('code_challenge_method', 'S256');
    return { url: url.toString(), state };
  }

  /** Drops a pending login without completing it (the identity provider reported an error). */
  abandon(state: string | undefined): void {
    if (state) this.pending.delete(state);
  }

  /**
   * Completes a login. The state is single-use and must match the one the browser holds, so a
   * wrong or replayed state (and therefore a replayed code) is refused before any provider call.
   */
  async complete(id: string, state: string | undefined, browserState: string | undefined, code: string | undefined): Promise<SsoLogin> {
    const provider = this.provider(id);
    const pending = state ? this.pending.get(state) : undefined;
    if (state) this.pending.delete(state);
    if (!state || !pending || pending.expiresAt <= this.now() || pending.providerId !== id || !browserState || !safeEqual(state, browserState)) {
      throw new SsoError(400, 'invalid_state', 'The login attempt is unknown, expired or was already used. Start again.');
    }
    if (!code) {
      throw new SsoError(400, 'bad_request', 'The authorization code is missing');
    }
    try {
      const identity = provider.type === 'oidc' ? await this.completeOidc(provider, pending, code) : await this.completeOauth2(provider, pending, code);
      return { ...identity, previousSessionId: pending.previousSessionId };
    } catch (err) {
      if (err instanceof SsoError) throw err;
      throw new SsoError(502, 'sso_unavailable', 'The identity provider could not be reached');
    }
  }

  /** True while at least one provider answers its discovery or token endpoint; probes are coalesced and cached briefly. */
  async healthy(): Promise<boolean> {
    if (this.health && this.now() - this.health.checkedAt < this.healthTtlMs) return this.health.healthy;
    if (!this.healthInflight) {
      this.healthInflight = Promise.all(this.providers.map(provider => this.probe(provider)))
        .then(results => {
          const healthy = results.some(Boolean);
          this.health = { healthy, checkedAt: this.now() };
          return healthy;
        })
        .finally(() => {
          this.healthInflight = undefined;
        });
    }
    return this.healthInflight;
  }

  private provider(id: string): SsoProviderConfig {
    const provider = this.byId.get(id);
    if (!provider) throw new SsoError(400, 'unknown_provider', `Unknown SSO provider "${id}"`);
    return provider;
  }

  private sweepPending(): void {
    const now = this.now();
    for (const [state, entry] of this.pending) {
      if (entry.expiresAt <= now) this.pending.delete(state);
    }
  }

  private async probe(provider: SsoProviderConfig): Promise<boolean> {
    try {
      if (provider.type === 'oidc') {
        await this.discover(provider, true);
        return true;
      }
      // Any answer below 500 (even a 400/404 for a bare GET) shows the token endpoint is up.
      const res = await httpJson(provider.tokenUrl as string, this.timeoutMs);
      return res.status < 500;
    } catch {
      return false;
    }
  }

  private async discover(provider: SsoProviderConfig, force = false): Promise<OidcMetadata> {
    const cached = this.metadata.get(provider.id);
    if (!force && cached && this.now() - cached.fetchedAt < METADATA_TTL_MS) return cached.value;
    const issuer = provider.issuer as string;
    const res = await httpJson(`${issuer.replace(/\/+$/, '')}/.well-known/openid-configuration`, this.timeoutMs);
    const doc = res.body;
    if (res.status !== 200 || !isJson(doc)) {
      throw new SsoError(502, 'sso_unavailable', 'OIDC discovery failed');
    }
    const advertised = str(doc.issuer);
    const authorizationEndpoint = str(doc.authorization_endpoint);
    const tokenEndpoint = str(doc.token_endpoint);
    const jwksUri = str(doc.jwks_uri);
    if (!advertised || advertised.replace(/\/+$/, '') !== issuer.replace(/\/+$/, '') || !authorizationEndpoint || !tokenEndpoint || !jwksUri) {
      throw new SsoError(502, 'sso_unavailable', 'The OIDC discovery document does not match the configured issuer');
    }
    const methods = Array.isArray(doc.token_endpoint_auth_methods_supported) ? doc.token_endpoint_auth_methods_supported : ['client_secret_basic'];
    const tokenAuth = methods.includes('client_secret_post') ? 'post' : 'basic';
    const value: OidcMetadata = { issuer: advertised, authorizationEndpoint, tokenEndpoint, jwksUri, userinfoEndpoint: str(doc.userinfo_endpoint), tokenAuth };
    this.metadata.set(provider.id, { value, fetchedAt: this.now() });
    return value;
  }

  private async exchangeCode(provider: SsoProviderConfig, tokenEndpoint: string, tokenAuth: 'basic' | 'post', pending: Pending, code: string): Promise<Json> {
    const form: Record<string, string> = {
      grant_type: 'authorization_code',
      code,
      redirect_uri: pending.redirectUri,
      client_id: provider.clientId,
      code_verifier: pending.verifier,
    };
    const headers: Record<string, string> = {};
    if (tokenAuth === 'basic') {
      // RFC 6749 §2.3.1: the id and secret are form-urlencoded before the Basic encoding.
      headers.authorization = `Basic ${Buffer.from(`${encodeURIComponent(provider.clientId)}:${encodeURIComponent(provider.clientSecret)}`).toString('base64')}`;
    } else {
      form.client_secret = provider.clientSecret;
    }
    const res = await httpJson(tokenEndpoint, this.timeoutMs, { method: 'POST', form, headers });
    if (res.status >= 500) {
      throw new SsoError(502, 'sso_unavailable', 'The identity provider token endpoint failed');
    }
    if (res.status !== 200 || !isJson(res.body) || res.body.error !== undefined) {
      throw new SsoError(401, 'invalid_code', 'The identity provider rejected the authorization code');
    }
    return res.body;
  }

  private async completeOidc(provider: SsoProviderConfig, pending: Pending, code: string): Promise<SsoIdentity> {
    const meta = await this.discover(provider);
    const tokens = await this.exchangeCode(provider, meta.tokenEndpoint, meta.tokenAuth, pending, code);
    const idToken = str(tokens.id_token);
    if (!idToken) {
      throw new SsoError(401, 'invalid_id_token', 'The identity provider returned no ID token');
    }
    const claims = await this.verifyIdToken(provider, meta, idToken, pending.nonce);
    const subject = claims.sub as string;
    let email = claims.email_verified === false ? undefined : str(claims.email);
    let login = str(claims.preferred_username);
    const accessToken = str(tokens.access_token);
    if (!email && accessToken && meta.userinfoEndpoint) {
      const info = await httpJson(meta.userinfoEndpoint, this.timeoutMs, { headers: { authorization: `Bearer ${accessToken}` } });
      if (info.status === 200 && isJson(info.body) && info.body.sub === subject) {
        email = info.body.email_verified === false ? undefined : str(info.body.email);
        login ??= str(info.body.preferred_username);
      }
    }
    return this.identity(provider, email, login ?? subject);
  }

  private async completeOauth2(provider: SsoProviderConfig, pending: Pending, code: string): Promise<SsoIdentity> {
    const tokens = await this.exchangeCode(provider, provider.tokenUrl as string, 'post', pending, code);
    const accessToken = str(tokens.access_token);
    if (!accessToken) {
      throw new SsoError(401, 'invalid_code', 'The identity provider returned no access token');
    }
    const headers = { authorization: `Bearer ${accessToken}` };
    const info = await httpJson(provider.userinfoUrl as string, this.timeoutMs, { headers });
    if (info.status >= 500) {
      throw new SsoError(502, 'sso_unavailable', 'The identity provider userinfo endpoint failed');
    }
    if (info.status !== 200 || !isJson(info.body)) {
      throw new SsoError(401, 'invalid_code', 'The identity provider rejected the access token');
    }
    const { email, login } = await this.oauth2Profile(provider, info.body, headers);
    return this.identity(provider, email, login);
  }

  // An email the provider marks unverified is never used; the verified-address list is the fallback.
  private async oauth2Profile(provider: SsoProviderConfig, info: Json, headers: Record<string, string>): Promise<{ email?: string; login?: string }> {
    let email = info.email_verified === false ? undefined : str(info.email);
    if (!email && provider.emailsUrl) {
      const emails = await httpJson(provider.emailsUrl, this.timeoutMs, { headers });
      if (emails.status === 200 && Array.isArray(emails.body)) {
        const entries = emails.body.filter(isJson).filter(entry => entry.verified === true && str(entry.email));
        email = str((entries.find(entry => entry.primary === true) ?? entries[0])?.email);
      }
    }
    const login = str(info.login) ?? str(info.preferred_username) ?? str(info.sub) ?? (typeof info.id === 'number' ? String(info.id) : undefined);
    return { email, login };
  }

  /**
   * Identifies the holder of an access token issued by one of the providers by presenting it to
   * the provider's userinfo endpoint. Returns undefined when no provider accepts it.
   */
  async identifyBearer(token: string): Promise<SsoIdentity | undefined> {
    for (const provider of this.providers) {
      try {
        const meta = provider.type === 'oidc' ? await this.discover(provider) : undefined;
        const userinfoUrl = meta ? meta.userinfoEndpoint : provider.userinfoUrl;
        if (!userinfoUrl) continue;
        const headers = { authorization: `Bearer ${token}` };
        const info = await httpJson(userinfoUrl, this.timeoutMs, { headers });
        if (info.status !== 200 || !isJson(info.body)) continue;
        if (provider.type === 'oidc') {
          const email = info.body.email_verified === false ? undefined : str(info.body.email);
          const subject = str(info.body.sub);
          if (!subject) continue;
          return this.identity(provider, email, str(info.body.preferred_username) ?? subject);
        }
        const { email, login } = await this.oauth2Profile(provider, info.body, headers);
        return this.identity(provider, email, login);
      } catch {
        // Try the next provider; an unreachable provider simply cannot vouch for the token.
      }
    }
    return undefined;
  }

  // The username is the email when the provider gave one; otherwise a provider-scoped login.
  private identity(provider: SsoProviderConfig, email: string | undefined, login: string | undefined): SsoIdentity {
    const address = email?.trim().toLowerCase();
    const username = address ?? (login ? `${provider.id}:${login}` : undefined);
    if (!username) {
      throw new SsoError(401, 'no_identity', 'The identity provider returned no usable identity');
    }
    return address ? { providerId: provider.id, username, email: address } : { providerId: provider.id, username };
  }

  private async keys(provider: SsoProviderConfig, meta: OidcMetadata, refresh: boolean): Promise<{ keys: readonly JsonWebKey[]; fresh: boolean }> {
    const cached = this.jwks.get(provider.id);
    if (cached && !refresh && this.now() - cached.fetchedAt < JWKS_TTL_MS) return { keys: cached.keys, fresh: false };
    const res = await httpJson(meta.jwksUri, this.timeoutMs);
    if (res.status !== 200 || !isJson(res.body) || !Array.isArray(res.body.keys)) {
      throw new SsoError(502, 'sso_unavailable', 'The identity provider signing keys could not be fetched');
    }
    const keys = res.body.keys.filter(isJson) as JsonWebKey[];
    this.jwks.set(provider.id, { keys, fetchedAt: this.now() });
    return { keys, fresh: true };
  }

  // Verifies the ID token's signature (RS256 or ES256 against the provider's JWKS) and the
  // iss, aud, azp, exp, nbf, nonce and sub claims.
  private async verifyIdToken(provider: SsoProviderConfig, meta: OidcMetadata, token: string, nonce: string): Promise<Json> {
    const bad = (message: string): SsoError => new SsoError(401, 'invalid_id_token', `Invalid ID token: ${message}`);
    const parts = token.split('.');
    if (parts.length !== 3) throw bad('malformed');
    let header: unknown;
    let claims: unknown;
    try {
      header = JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf-8'));
      claims = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf-8'));
    } catch {
      throw bad('malformed');
    }
    if (!isJson(header) || !isJson(claims)) throw bad('malformed');
    const alg = header.alg;
    if (alg !== 'RS256' && alg !== 'ES256') throw bad('unsupported signing algorithm');
    const kid = str(header.kid);
    const signed = Buffer.from(`${parts[0]}.${parts[1]}`);
    const signature = Buffer.from(parts[2], 'base64url');

    const matches = (keys: readonly JsonWebKey[]): JsonWebKey[] => keys.filter(key => {
      const jwk = key as Json;
      const kty = alg === 'RS256' ? 'RSA' : 'EC';
      return jwk.kty === kty && (alg === 'RS256' || jwk.crv === 'P-256') && (jwk.use === undefined || jwk.use === 'sig') && (!kid || jwk.kid === kid);
    });
    const signatureOk = (keys: readonly JsonWebKey[]): boolean => matches(keys).some(key => {
      try {
        const publicKey = createPublicKey({ key, format: 'jwk' });
        return alg === 'RS256'
          ? cryptoVerify('sha256', signed, publicKey, signature)
          : cryptoVerify('sha256', signed, { key: publicKey, dsaEncoding: 'ieee-p1363' }, signature);
      } catch {
        return false;
      }
    });
    // Keys age out after JWKS_TTL_MS. A signature that does not verify against cached keys
    // triggers one refetch, so a rotated or replaced key is picked up without a restart.
    let current = await this.keys(provider, meta, false);
    let verified = signatureOk(current.keys);
    if (!verified && !current.fresh) {
      current = await this.keys(provider, meta, true);
      verified = signatureOk(current.keys);
    }
    if (!verified) throw bad('signature verification failed');

    if (claims.iss !== meta.issuer) throw bad('issuer mismatch');
    const audience = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    if (!audience.includes(provider.clientId)) throw bad('audience mismatch');
    if ((audience.length > 1 || claims.azp !== undefined) && claims.azp !== provider.clientId) throw bad('authorized party mismatch');
    const nowSeconds = this.now() / 1000;
    if (typeof claims.exp !== 'number' || claims.exp + CLOCK_SKEW_SECONDS <= nowSeconds) throw bad('expired');
    if (claims.nbf !== undefined && (typeof claims.nbf !== 'number' || claims.nbf - CLOCK_SKEW_SECONDS > nowSeconds)) throw bad('not yet valid');
    if (typeof claims.nonce !== 'string' || !safeEqual(claims.nonce, nonce)) throw bad('nonce mismatch');
    if (!str(claims.sub)) throw bad('subject missing');
    return claims;
  }
}
