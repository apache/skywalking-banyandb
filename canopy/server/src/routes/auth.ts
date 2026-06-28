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
  endpoint: string;
}

function normalizeEndpoint(raw: string): string {
  // Accept bare host:port and normalize to http://
  // Leave any other scheme (ftp://, etc.) unchanged so isValidHttpUrl rejects it.
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^[a-z][a-z0-9+\-.]*:\/\//i.test(raw)) return raw;
  return `http://${raw}`;
}

function isValidHttpUrl(s: string): boolean {
  try {
    const u = new URL(s);
    return u.protocol === 'http:' || u.protocol === 'https:';
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
  // Unauthenticated — exposes BanyanDB version + reachability for the login page
  // before credentials are known. Registered before the authenticated /api/*
  // proxy so Fastify matches this route first.
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
    return reply.send({ banyanVersion, reachable });
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

    const banyanVersion = await fetchBanyanVersion(normalizedEndpoint);
    // Refuse to log in against an unreachable endpoint — otherwise the user
    // gets a successful response, the sidebar shows "Connected <endpoint>",
    // but every subsequent /api/* request returns 502 upstream_error. Tests
    // that don't spin up a real upstream can disable this via config.
    if (config.requireUpstreamOnLogin && banyanVersion === null) {
      return reply.status(502).send({
        error: 'upstream_unreachable',
        message: `Cannot reach BanyanDB at ${normalizedEndpoint}. Check that the endpoint is correct and the server is up.`,
      });
    }

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
