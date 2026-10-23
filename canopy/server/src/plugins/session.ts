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

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { createHash } from 'node:crypto';
import cookie from '@fastify/cookie';
import type { CanopyUser, Config } from '../config.js';
import { AuthPolicyDirectory } from '../lib/policy.js';
import { resolveSsoRole, SsoService } from '../lib/sso.js';
import type { Role } from '../lib/role.js';
import { SessionStore, type SessionRecord } from '../lib/sessions.js';
import { UserDirectory } from '../lib/users.js';
import { bearerCredential, TOKEN_PREFIX, TokenVerifier } from '../lib/tokens.js';

export interface Principal {
  readonly username: string;
  readonly role: Role;
  // How the principal was authenticated: a browser session cookie or an API token.
  readonly source: 'session' | 'token';
}

/** Returned by a resolver that recognised a credential but refused it; no later resolver may run. */
const REJECT = 'reject';

/** Resolves a principal from a request, or returns undefined to let the next resolver try. */
export type PrincipalResolver = (request: FastifyRequest) => Principal | typeof REJECT | undefined | Promise<Principal | typeof REJECT | undefined>;

export interface AuthContext {
  readonly store: SessionStore;
  readonly cookieName: string;
  /** The live SSO role mapping and break-glass accounts (re-read from the auth file every few seconds). */
  readonly policy: AuthPolicyDirectory;
  /** Looks a user up in the live user list (re-read from its files periodically). */
  findUser(username: string): CanopyUser | undefined;
  /** Rejects with 401 unless some resolver yields a principal. */
  requireAuth(request: FastifyRequest, reply: FastifyReply): Promise<void>;
  /** Like requireAuth but only accepts a browser session cookie. */
  requireBrowserSession(request: FastifyRequest, reply: FastifyReply): Promise<void>;
  /** For routes usable without a session (login/logout): rejects callers that present a bearer credential but no browser session. */
  rejectBearerOnly(request: FastifyRequest, reply: FastifyReply): Promise<void>;
  /**
   * The serialized Set-Cookie value that renews the request's session, or undefined when the
   * request is not session-authenticated. For responses written straight to reply.raw, where
   * Fastify's deferred cookie flush (onSend) never runs.
   */
  renewalCookie(request: FastifyRequest): string | undefined;
  /** Sets the session cookie for a new or refreshed session. */
  setSessionCookie(reply: FastifyReply, id: string): void;
  clearSessionCookie(reply: FastifyReply): void;
}

declare module 'fastify' {
  interface FastifyInstance {
    canopyAuth: AuthContext;
  }
  interface FastifyRequest {
    principal: Principal | null;
  }
}

const SWEEP_INTERVAL_MS = 60_000;
const BEARER_ROLE_CACHE_MS = 30_000;
const IDP_CACHE_MAX = 1000;

async function unauthenticated(reply: FastifyReply): Promise<void> {
  await reply.status(401).send({ error: 'unauthenticated', message: 'Login required' });
}

export async function registerSession(app: FastifyInstance, config: Config): Promise<void> {
  const { cookieName, ttlMinutes, cookieSecure } = config.auth.session;
  const store = new SessionStore({ ttlMs: ttlMinutes * 60_000 });
  const policy = new AuthPolicyDirectory(config.auth, err => app.log.warn({ err }, 'could not reload the SSO policy; keeping the previous one'));
  const directory = new UserDirectory(config.auth.local, err => app.log.warn({ err }, 'could not reload the users file; keeping the previous users'));
  await app.register(cookie);
  app.decorateRequest('principal', null);

  const cookieOptions = {
    path: config.baseHref,
    httpOnly: true,
    sameSite: 'strict' as const,
    secure: cookieSecure,
    maxAge: ttlMinutes * 60,
  };

  // The role is re-resolved from the current config on every request, so removing
  // a user or changing a role takes effect without a re-login.
  const sessionRole = (record: SessionRecord): Role | undefined => {
    switch (record.origin?.kind) {
      case 'sso': {
        const roles = policy.roles();
        return roles ? resolveSsoRole(roles, record.origin.email) : undefined;
      }
      case 'breakglass':
        return policy.breakGlass()?.users.find(u => u.username === record.username)?.role;
      default:
        return directory.find(record.username)?.role;
    }
  };
  const resolveSessionCookie: PrincipalResolver = request => {
    const record = store.touch(request.cookies[cookieName]);
    if (!record) return undefined;
    const role = sessionRole(record);
    if (!role) {
      store.destroy(request.cookies[cookieName]);
      return undefined;
    }
    return { username: record.username, role, source: 'session' };
  };
  // An access token issued by an SSO provider: the provider's userinfo endpoint vouches for it,
  // and the role comes from the live SSO mapping. Cached 30 s on success, never on failure.
  const ssoConfig = config.auth.backend === 'sso' ? config.auth.sso : undefined;
  const bearerSso = ssoConfig ? new SsoService(ssoConfig.providers, ssoConfig.timeoutMs) : undefined;
  // The token names a username only; its role is re-resolved from the live config and
  // cached for 30 s on success. Failures are never cached, so a re-added user works at once.
  const tokens = new TokenVerifier(config.auth.tokens ?? []);
  const bearerCache = new Map<string, { principal: Principal; expiresAt: number }>();
  const resolveBearer: PrincipalResolver = request => {
    const credential = bearerCredential(request.headers.authorization);
    if (credential === undefined) return undefined;
    // With SSO on, a credential without the API-token prefix was already judged by resolveIdpBearer
    // (it either authenticated or rejected the chain); let browser-only routes fall back to the cookie.
    // An empty credential is never judged by resolveIdpBearer, so it falls through to the token check and is rejected.
    if (bearerSso && credential !== '' && !credential.startsWith(TOKEN_PREFIX)) return undefined;
    const entry = tokens.verify(credential);
    if (!entry) return REJECT;
    const now = Date.now();
    const cached = bearerCache.get(entry.id);
    if (cached && cached.expiresAt > now && cached.principal.username === entry.username) return cached.principal;
    bearerCache.delete(entry.id);
    const user = directory.find(entry.username);
    if (!user) return REJECT;
    const principal: Principal = { username: user.username, role: user.role, source: 'token' };
    bearerCache.set(entry.id, { principal, expiresAt: now + BEARER_ROLE_CACHE_MS });
    return principal;
  };
  const idpCache = new Map<string, { username: string; email: string | undefined; expiresAt: number }>();
  const resolveIdpBearer: PrincipalResolver = async request => {
    const credential = bearerCredential(request.headers.authorization);
    if (!bearerSso || !credential || credential.startsWith(TOKEN_PREFIX)) return undefined;
    const key = createHash('sha256').update(credential).digest('hex');
    const now = Date.now();
    const cached = idpCache.get(key);
    let entry = cached && cached.expiresAt > now ? cached : undefined;
    if (!entry) idpCache.delete(key);
    // The role is looked up in the live mapping on every request, so a mapping change applies at once.
    // The identity is cached only once its role is authorized; a denied credential is never kept.
    const roleOf = (email: string | undefined): Role | undefined => {
      const roles = policy.roles();
      return roles ? resolveSsoRole(roles, email) : undefined;
    };
    if (entry) {
      const role = roleOf(entry.email);
      if (!role) {
        idpCache.delete(key);
        return REJECT;
      }
      return { username: entry.username, role, source: 'token' };
    }
    const identity = await bearerSso.identifyBearer(credential);
    if (!identity) return REJECT;
    const role = roleOf(identity.email);
    if (!role) return REJECT;
    entry = { username: identity.username, email: identity.email, expiresAt: now + BEARER_ROLE_CACHE_MS };
    while (idpCache.size >= IDP_CACHE_MAX) {
      const oldest = idpCache.keys().next();
      if (oldest.done) break;
      idpCache.delete(oldest.value);
    }
    idpCache.set(key, entry);
    return { username: entry.username, role, source: 'token' };
  };
  // Horizon's order: IdP bearer, then API token, then cookie session.
  const resolvers: PrincipalResolver[] = [resolveIdpBearer, resolveBearer, resolveSessionCookie];

  const authenticate = async (request: FastifyRequest, reply: FastifyReply, accepted: readonly Principal['source'][]): Promise<void> => {
    for (const resolve of resolvers) {
      const principal = await resolve(request);
      if (principal === REJECT) break;
      if (principal && accepted.includes(principal.source)) {
        request.principal = principal;
        if (principal.source === 'session') {
          const id = request.cookies[cookieName];
          if (id) auth.setSessionCookie(reply, id);
        }
        return;
      }
    }
    await unauthenticated(reply);
  };

  const auth: AuthContext = {
    store,
    cookieName,
    policy,
    findUser: username => directory.find(username),
    requireAuth: (request, reply) => authenticate(request, reply, ['session', 'token']),
    requireBrowserSession: (request, reply) => authenticate(request, reply, ['session']),
    rejectBearerOnly: async (request, reply) => {
      if (bearerCredential(request.headers.authorization) !== undefined) {
        await authenticate(request, reply, ['session']);
      }
    },
    renewalCookie: request => {
      const id = request.cookies[cookieName];
      if (request.principal?.source !== 'session' || !id) return undefined;
      const secure = cookieSecure === 'auto' ? request.protocol === 'https' : cookieSecure;
      return app.serializeCookie(cookieName, id, { ...cookieOptions, secure });
    },
    setSessionCookie: (reply, id) => {
      void reply.setCookie(cookieName, id, cookieOptions);
    },
    clearSessionCookie: reply => {
      void reply.clearCookie(cookieName, { path: config.baseHref, httpOnly: true, sameSite: 'strict', secure: cookieSecure });
    },
  };
  app.decorate('canopyAuth', auth);

  const sweeper = setInterval(() => store.sweep(), SWEEP_INTERVAL_MS);
  sweeper.unref();
  app.addHook('onClose', async () => {
    clearInterval(sweeper);
  });
}
