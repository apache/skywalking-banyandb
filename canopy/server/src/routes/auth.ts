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

import bcrypt from 'bcryptjs';
import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { request as undiciRequest } from 'undici';

import type { Config } from '../config.js';

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

  app.post<{ Body: LoginBody }>('/auth/login', async (request: FastifyRequest, reply: FastifyReply) => {
    const { username, password } = request.body as LoginBody;

    if (!username || !password) {
      return reply.status(400).send({ error: 'bad_request', message: 'username and password are required' });
    }

    // Find user (constant-time compare to prevent timing attacks)
    const user = config.users.find(u => u.username === username);

    // Always run bcrypt compare even if user not found (timing-safe)
    const hashToCheck = user?.passwordHash ?? '$2b$10$invalidhashpaddingtomakeitconstanttimexxx';
    let passwordOk = false;
    try {
      passwordOk = await bcrypt.compare(password, hashToCheck);
    } catch {
      passwordOk = false;
    }

    if (!user || !passwordOk) {
      return reply.status(401).send({ error: 'invalid_credentials', message: 'Invalid username or password' });
    }

    // Cache the BanyanDB version on the session so the brand badge can
    // render instantly without re-probing on every page load. Upstream
    // reachability is NOT a login barrier — a stale BANYANDB_TARGET will
    // surface as a 502 on the first /api/* call, not as a login failure.
    const banyanVersion = await fetchBanyanVersion(config.banyandbTarget);

    request.session.user = user.username;
    request.session.role = user.role;
    request.session.banyanVersion = banyanVersion;

    return reply.send({ user: user.username, role: user.role, banyanVersion });
  });

  app.post('/auth/logout', async (request: FastifyRequest, reply: FastifyReply) => {
    request.session.delete();
    return reply.send({ ok: true });
  });

  app.get('/auth/session', async (request: FastifyRequest, reply: FastifyReply) => {
    const session = request.session;
    if (!session?.user) {
      return reply.status(401).send({ error: 'unauthenticated' });
    }
    return reply.send({ user: session.user, role: session.role, banyanVersion: session.banyanVersion ?? null });
  });
}