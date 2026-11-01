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

export async function registerAuth(app: FastifyInstance, config: Config): Promise<void> {
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
    return reply.send({ banyanVersion, reachable, banyandbTarget: config.banyandbTarget });
  });

  app.post<{ Body: LoginBody }>('/auth/login', { preHandler: [app.canopyAuth.rejectBearerOnly] }, async (request: FastifyRequest, reply: FastifyReply) => {
    const { username, password } = request.body as LoginBody;

    if (!username || !password) {
      return reply.status(400).send({ error: 'bad_request', message: 'username and password are required' });
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
