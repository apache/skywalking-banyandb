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

import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { request as undiciRequest } from 'undici';

import type { Config } from '../config.js';
import { verifyPassword } from '../lib/password.js';
import { resolveSsoRole, SsoError, SsoService } from '../lib/sso.js';

interface LoginBody {
  username: string;
  password: string;
}

async function fetchBanyanVersion(target: string): Promise<string | null> {
  try {
    const res = await undiciRequest(`${target}/api/v1/common/api/version`, {
      method: 'GET',
      headersTimeout: 3000,
      bodyTimeout: 3000,
    });
    if (res.statusCode === 200) {
      const data = await res.body.json() as { version?: { version?: string } };
      return data?.version?.version ?? null;
    }
    await res.body.dump();
  } catch {
    // best-effort
  }
  return null;
}

export interface RegisterAuthOptions {
  /** How long an SSO health probe result is reused; the default is a few seconds. */
  readonly ssoHealthTtlMs?: number;
}

const SSO_STATE_MAX_AGE_SECONDS = 600;

export async function registerAuth(app: FastifyInstance, config: Config, options: RegisterAuthOptions = {}): Promise<void> {
  const ssoConfig = config.auth.backend === 'sso' ? config.auth.sso : undefined;
  const sso = ssoConfig ? new SsoService(ssoConfig.providers, ssoConfig.timeoutMs, options.ssoHealthTtlMs) : undefined;
  const ssoStateCookie = `${config.auth.session.cookieName}_sso`;
  const ssoStateCookieOptions = (request: FastifyRequest) => ({
    path: config.baseHref,
    httpOnly: true,
    // Lax, not Strict: the callback is a cross-site top-level navigation from the identity provider.
    sameSite: 'lax' as const,
    secure: config.auth.session.cookieSecure === 'auto' ? request.protocol === 'https' : config.auth.session.cookieSecure,
  });

  // Unauthenticated — exposes the BFF's configured BanyanDB target, its
  // version, and reachability so the login page and the Sidebar can render
  // operator-relevant context before credentials are known. Registered
  // before the authenticated /api/* proxy so Fastify matches this route first.
  app.get('/api/meta', async (_request: FastifyRequest, reply: FastifyReply) => {
    // fetchBanyanVersion returns null on any failure (timeout, non-200, bad
    // body). Distinguish "we got an answer, the version was empty" from "we
    // couldn't reach the server" by probing a lightweight endpoint.
    let reachable = false;
    try {
      const res = await undiciRequest(`${config.banyandbTarget}/api/v1/common/api/version`, {
        method: 'GET',
        headersTimeout: 3000,
        bodyTimeout: 3000,
      });
      reachable = res.statusCode > 0 && res.statusCode < 500;
      await res.body.dump();
    } catch {
      reachable = false;
    }
    const banyanVersion = reachable ? await fetchBanyanVersion(config.banyandbTarget) : null;
    // Tells the login page which sign-in options to offer; the break-glass form is only
    // meant to show while the identity provider is unhealthy.
    const authMeta = sso
      ? { backend: 'sso' as const, providers: sso.list(), ssoHealthy: await sso.healthy(), breakGlass: app.canopyAuth.policy.breakGlass() !== undefined }
      : { backend: 'local' as const };
    return reply.send({ banyanVersion, reachable, banyandbTarget: config.banyandbTarget, auth: authMeta });
  });

  if (sso) {
    app.get<{ Params: { provider: string } }>('/auth/sso/:provider/start', async (request, reply) => {
      const id = request.params.provider;
      if (!sso.has(id)) {
        return reply.status(404).send({ error: 'not_found', message: 'Unknown SSO provider' });
      }
      const redirectUri = sso.redirectUriFor(id) ?? `${request.protocol}://${request.host}${config.baseHref}auth/sso/${encodeURIComponent(id)}/callback`;
      try {
        // The session cookie is SameSite=Strict, so the callback (a cross-site return) will not carry it:
        // remember which session this browser holds now, from this same-site request.
        const { url, state } = await sso.begin(id, redirectUri, request.cookies[app.canopyAuth.cookieName]);
        void reply.setCookie(ssoStateCookie, state, { ...ssoStateCookieOptions(request), maxAge: SSO_STATE_MAX_AGE_SECONDS });
        return reply.redirect(url);
      } catch (err) {
        request.log.warn({ err, provider: id }, 'SSO start failed');
        return reply.status(502).send({ error: 'sso_unavailable', message: 'The identity provider could not be reached' });
      }
    });

    app.get<{ Params: { provider: string }; Querystring: { code?: string; state?: string; error?: string } }>(
      '/auth/sso/:provider/callback',
      async (request, reply) => {
        const id = request.params.provider;
        if (!sso.has(id)) {
          return reply.status(404).send({ error: 'not_found', message: 'Unknown SSO provider' });
        }
        const { code, state, error } = request.query;
        const browserState = request.cookies[ssoStateCookie];
        void reply.clearCookie(ssoStateCookie, ssoStateCookieOptions(request));
        if (error) {
          sso.abandon(state);
          request.log.info({ provider: id, error }, 'SSO login denied by the identity provider');
          return reply.status(400).send({ error: 'sso_denied', message: 'The identity provider refused the login' });
        }
        try {
          const identity = await sso.complete(id, state, browserState, code);
          const roles = app.canopyAuth.policy.roles();
          const role = roles ? resolveSsoRole(roles, identity.email) : undefined;
          if (!role) {
            request.log.info({ provider: id, username: identity.username }, 'SSO login refused: no role matches');
            return reply.status(403).send({ error: 'forbidden', message: 'Your account has no access to Canopy. Ask an administrator to grant a role.' });
          }
          const banyanVersion = await fetchBanyanVersion(config.banyandbTarget);
          app.canopyAuth.store.destroy(request.cookies[app.canopyAuth.cookieName]);
          app.canopyAuth.store.destroy(identity.previousSessionId);
          const sessionId = app.canopyAuth.store.create(identity.username, banyanVersion, { kind: 'sso', email: identity.email });
          app.canopyAuth.setSessionCookie(reply, sessionId);
          request.log.info({ provider: id, username: identity.username, role }, 'SSO login accepted');
          return reply.redirect(config.baseHref);
        } catch (err) {
          if (err instanceof SsoError) {
            request.log.info({ provider: id, code: err.code }, 'SSO login failed');
            return reply.status(err.status).send({ error: err.code, message: err.message });
          }
          throw err;
        }
      },
    );
  }

  // With the sso backend POST /auth/login only serves break-glass users, and only while the
  // identity provider is unhealthy. The password is verified either way so that a healthy
  // provider, an unknown user and a wrong password cost the same and answer the same 401.
  const loginBreakGlass = async (request: FastifyRequest, reply: FastifyReply, username: string, password: string): Promise<FastifyReply> => {
    const breakGlass = app.canopyAuth.policy.breakGlass();
    const candidate = breakGlass?.users.find(u => u.username === username);
    const passwordOk = await verifyPassword(candidate?.passwordHash, password);
    const ssoUnhealthy = breakGlass ? !(await sso!.healthy()) : false;
    if (!breakGlass || !candidate || !passwordOk || !ssoUnhealthy) {
      request.log.info({ username, ip: request.ip }, 'break-glass login failed');
      return reply.status(401).send({ error: 'invalid_credentials', message: 'Invalid username or password' });
    }
    request.log.warn({ username, ip: request.ip }, 'break-glass login accepted while SSO is unhealthy');
    const banyanVersion = await fetchBanyanVersion(config.banyandbTarget);
    app.canopyAuth.store.destroy(request.cookies[app.canopyAuth.cookieName]);
    const sessionId = app.canopyAuth.store.create(candidate.username, banyanVersion, { kind: 'breakglass' });
    app.canopyAuth.setSessionCookie(reply, sessionId);
    return reply.send({ user: candidate.username, role: candidate.role, banyanVersion });
  };

  app.post<{ Body: LoginBody }>('/auth/login', { preHandler: [app.canopyAuth.rejectBearerOnly] }, async (request: FastifyRequest, reply: FastifyReply) => {
    const { username, password } = request.body as LoginBody;

    if (!username || !password) {
      return reply.status(400).send({ error: 'bad_request', message: 'username and password are required' });
    }

    if (config.auth.backend === 'sso') {
      return loginBreakGlass(request, reply, username, password);
    }

    // An unknown user still runs one argon2id verification (against a dummy
    // hash) so the response time does not reveal which usernames exist.
    const user = app.canopyAuth.findUser(username);
    const passwordOk = await verifyPassword(user?.passwordHash, password);

    if (!user || !passwordOk) {
      return reply.status(401).send({ error: 'invalid_credentials', message: 'Invalid username or password' });
    }

    // Cache the BanyanDB version on the session so the brand badge can
    // render instantly without re-probing on every page load. Upstream
    // reachability is NOT a login barrier — a stale BANYANDB_TARGET will
    // surface as a 502 on the first /api/* call, not as a login failure.
    const banyanVersion = await fetchBanyanVersion(config.banyandbTarget);

    // Drop any session the client already holds so a login never reuses an id.
    app.canopyAuth.store.destroy(request.cookies[app.canopyAuth.cookieName]);
    const sessionId = app.canopyAuth.store.create(user.username, banyanVersion);
    app.canopyAuth.setSessionCookie(reply, sessionId);

    return reply.send({ user: user.username, role: user.role, banyanVersion });
  });

  app.post('/auth/logout', { preHandler: [app.canopyAuth.rejectBearerOnly] }, async (request: FastifyRequest, reply: FastifyReply) => {
    app.canopyAuth.store.destroy(request.cookies[app.canopyAuth.cookieName]);
    app.canopyAuth.clearSessionCookie(reply);
    return reply.send({ ok: true });
  });

  app.get('/auth/session', { preHandler: [app.canopyAuth.requireBrowserSession] }, async (request: FastifyRequest, reply: FastifyReply) => {
    const principal = request.principal;
    const record = app.canopyAuth.store.touch(request.cookies[app.canopyAuth.cookieName]);
    return reply.send({ user: principal?.username, role: principal?.role, banyanVersion: record?.banyanVersion ?? null });
  });
}
