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

import { request as undiciRequest, type Dispatcher } from 'undici';
import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { checkEndpointSafe, SsrfError } from '../lib/ssrf.js';
import { enforceRole } from '../lib/role.js';
import type { Config } from '../config.js';

function buildUpstreamAuth(config: Config): string | undefined {
  if (config.upstreamUsername && config.upstreamPassword) {
    const encoded = Buffer.from(`${config.upstreamUsername}:${config.upstreamPassword}`).toString('base64');
    return `Basic ${encoded}`;
  }
  return undefined;
}

function getSessionEndpoint(request: FastifyRequest, config: Config): string {
  return request.session?.endpoint || config.banyandbTarget;
}

async function forwardRequest(
  upstreamBase: string,
  upstreamPath: string,
  request: FastifyRequest,
  reply: FastifyReply,
  config: Config,
  upstreamAuth: string | undefined,
): Promise<void> {
  const targetUrl = `${upstreamBase}${upstreamPath}`;

  // Re-check SSRF on every request (per-request DNS re-resolution)
  try {
    await checkEndpointSafe(upstreamBase, config.blockRfc1918);
  } catch (err) {
    if (err instanceof SsrfError) {
      await reply.status(403).send({ error: 'ssrf_denied', message: err.message });
      return;
    }
    throw err;
  }

  const headers: Record<string, string> = {};

  // Forward relevant headers from client; skip hop-by-hop and let undici manage content-length
  for (const [k, v] of Object.entries(request.headers)) {
    const lower = k.toLowerCase();
    if (lower === 'host' || lower === 'connection' || lower === 'transfer-encoding' || lower === 'content-length') continue;
    if (typeof v === 'string') headers[k] = v;
  }

  // Attach upstream service credential if configured (attach-if-configured, global)
  if (upstreamAuth) {
    headers['authorization'] = upstreamAuth;
  }

  // Re-serialize body: Fastify parses application/json into a JS object; undici requires string/Buffer.
  // Skipping for bodyless methods avoids sending empty payloads on GET/HEAD/DELETE/OPTIONS.
  let upstreamBody: string | undefined;
  if (!['GET', 'HEAD', 'DELETE', 'OPTIONS'].includes(request.method) && request.body != null) {
    upstreamBody = typeof request.body === 'string' ? request.body : JSON.stringify(request.body);
    if (!headers['content-type']) headers['content-type'] = 'application/json';
    headers['content-length'] = String(Buffer.byteLength(upstreamBody));
  }

  let upstreamRes: Dispatcher.ResponseData;
  try {
    upstreamRes = await undiciRequest(targetUrl, {
      method: request.method as Dispatcher.HttpMethod,
      headers,
      body: upstreamBody,
      bodyTimeout: config.upstreamTimeoutMs,
      headersTimeout: config.upstreamTimeoutMs,
      // undici v7 request() does not follow redirects by default (no RedirectAgent/RedirectHandler
      // configured), so 3xx responses are forwarded verbatim — the SSRF deny-narrow contract holds.
      reset: true, // send Connection: close (no keep-alive pooling per proxy request)
    });
  } catch (err: unknown) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ECONNREFUSED' || code === 'ENOTFOUND') {
      await reply.status(502).send({
        error: 'upstream_error',
        message: `Cannot connect to upstream: ${upstreamBase}`,
      });
      return;
    }
    if (code === 'UND_ERR_HEADERS_TIMEOUT' || code === 'UND_ERR_BODY_TIMEOUT') {
      await reply.status(504).send({
        error: 'upstream_timeout',
        message: `Upstream request timed out after ${config.upstreamTimeoutMs}ms`,
      });
      return;
    }
    throw err;
  }

  // Handle upstream 401 distinctly — wrap in a specific error envelope
  if (upstreamRes.statusCode === 401) {
    // Drain the body
    await upstreamRes.body.dump();
    await reply.status(401).send({
      error: 'upstream_auth_error',
      message: 'The target cluster rejected the configured service credential. Check BANYANDB_UPSTREAM_USERNAME/PASSWORD or the endpoint.',
      upstream: upstreamBase,
    });
    return;
  }

  // Upstream 5xx → wrap as 502
  if (upstreamRes.statusCode >= 500) {
    await upstreamRes.body.dump();
    await reply.status(502).send({
      error: 'upstream_error',
      message: `Upstream returned ${upstreamRes.statusCode}`,
    });
    return;
  }

  // Forward the response
  const forwardHeaders: Record<string, string | string[]> = {};
  for (const [k, v] of Object.entries(upstreamRes.headers)) {
    const lower = k.toLowerCase();
    if (lower === 'transfer-encoding' || lower === 'connection') continue;
    if (v !== undefined) forwardHeaders[k] = v as string | string[];
  }
  reply.raw.writeHead(upstreamRes.statusCode, forwardHeaders);
  await upstreamRes.body.pipe(reply.raw);
}

export async function registerProxy(app: FastifyInstance, config: Config): Promise<void> {
  const upstreamAuth = buildUpstreamAuth(config);

  // Auth middleware for all proxy routes
  async function requireAuth(request: FastifyRequest, reply: FastifyReply) {
    if (!request.session?.user) {
      await reply.status(401).send({ error: 'unauthenticated', message: 'Login required' });
    }
  }

  // /api/* — proxied VERBATIM to session.endpoint (no prefix add/strip)
  app.all('/api/*', { preHandler: [requireAuth, enforceRole] }, async (request, reply) => {
    const sessionEndpoint = getSessionEndpoint(request, config);
    const upstreamPath = request.url; // VERBATIM — includes /api/v1/... prefix
    await forwardRequest(sessionEndpoint, upstreamPath, request, reply, config, upstreamAuth);
  });

  // /monitoring/* — forwarded to MONITOR_TARGET with /monitoring prefix STRIPPED
  app.all('/monitoring/*', { preHandler: [requireAuth] }, async (request, reply) => {
    const stripped = request.url.replace(/^\/monitoring/, '');
    await forwardRequest(config.monitorTarget, stripped, request, reply, config, undefined);
  });
}
