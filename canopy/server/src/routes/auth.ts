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
import { checkEndpointSafe, SsrfError } from '../lib/ssrf.js';

interface LoginBody {
  username: string;
  password: string;
  endpoint: string;
}

function normalizeEndpoint(raw: string): string {
  // Accept bare host:port — normalize to http:// first so URL can parse it.
  // Leave any other scheme unchanged so isValidHttpUrl rejects it.
  const withScheme = /^https?:\/\//i.test(raw) ? raw
    : /^[a-z][a-z0-9+\-.]*:\/\//i.test(raw) ? raw
    : `http://${raw}`;
  try {
    // Canonicalize to origin — strips path, query, fragment, and userinfo.
    return new URL(withScheme).origin;
  } catch {
    return withScheme;
  }
}

function isValidHttpUrl(s: string): boolean {
  try {
    const u = new URL(s);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return false;
    // Reject embedded credentials (e.g. http://user:pass@host)
    if (u.username !== '' || u.password !== '') return false;
    return true;
  } catch {
    return false;
  }
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
  // Unauthenticated — exposes BanyanDB version for the login page before credentials are known.
  // Registered before the authenticated /api/* proxy so Fastify matches this route first.
  app.get('/api/meta', async (_request: FastifyRequest, reply: FastifyReply) => {
    const banyanVersion = await fetchBanyanVersion(config.banyandbTarget);
    return reply.send({ banyanVersion });
  });

  app.post<{ Body: LoginBody }>('/auth/login', async (request: FastifyRequest, reply: FastifyReply) => {
    const { username, password, endpoint } = request.body as LoginBody;

    if (!username || !password || !endpoint) {
      return reply.status(400).send({ error: 'bad_request', message: 'username, password, and endpoint are required' });
    }

    const normalizedEndpoint = normalizeEndpoint(endpoint);
    if (!isValidHttpUrl(normalizedEndpoint)) {
      return reply.status(400).send({ error: 'bad_endpoint', message: 'Endpoint must be a valid http(s) URL or host:port' });
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

    // SSRF check before contacting the user-supplied endpoint
    try {
      await checkEndpointSafe(normalizedEndpoint, false);
    } catch (err) {
      if (err instanceof SsrfError) {
        return reply.status(400).send({ error: 'bad_endpoint', message: err.message });
      }
      throw err;
    }

    const banyanVersion = await fetchBanyanVersion(normalizedEndpoint);

    request.session.user = user.username;
    request.session.role = user.role;
    request.session.endpoint = normalizedEndpoint;
    request.session.banyanVersion = banyanVersion;

    return reply.send({ user: user.username, role: user.role, endpoint: normalizedEndpoint, banyanVersion });
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
    return reply.send({ user: session.user, role: session.role, endpoint: session.endpoint, banyanVersion: session.banyanVersion ?? null });
  });
}
