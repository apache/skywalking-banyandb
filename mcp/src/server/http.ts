/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createServer, IncomingMessage } from 'node:http';

import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { BanyanDBClient } from '../client/index.js';
import {
  BANYANDB_ADDRESS,
  MAX_ERROR_MESSAGE_LENGTH,
  MCP_AUTH_TOKEN,
  MCP_HOST,
  MCP_MAX_BODY_BYTES,
  MCP_PORT,
  MCP_RATE_LIMIT_MAX_CLIENTS,
  MCP_RATE_LIMIT_MAX_REQUESTS,
  MCP_RATE_LIMIT_WINDOW_MS,
} from '../config.js';
import { createMcpServer, truncateText } from './mcp.js';
import { log } from '../utils/logger.js';

type RateLimitEntry = {
  count: number;
  windowStart: number;
};

const rateLimitByClient = new Map<string, RateLimitEntry>();

function getAuthorizationToken(req: IncomingMessage): string {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return '';
  }

  const bearerPrefix = 'Bearer ';
  return authHeader.startsWith(bearerPrefix) ? authHeader.slice(bearerPrefix.length).trim() : '';
}

export function isLoopbackHost(host: string): boolean {
  return host === '127.0.0.1' || host === 'localhost' || host === '::1';
}

function getClientIdentifier(req: IncomingMessage): string {
  const forwardedFor = req.headers['x-forwarded-for'];
  if (typeof forwardedFor === 'string' && forwardedFor.trim()) {
    return forwardedFor.split(',')[0].trim();
  }

  return req.socket.remoteAddress || 'unknown';
}

function pruneExpiredRateLimitEntries(now: number): void {
  for (const [clientId, currentEntry] of rateLimitByClient.entries()) {
    if (now - currentEntry.windowStart >= MCP_RATE_LIMIT_WINDOW_MS) {
      rateLimitByClient.delete(clientId);
    }
  }
}

function evictOldestRateLimitEntry(): void {
  const oldestEntry = rateLimitByClient.entries().next().value;
  if (oldestEntry) {
    const [clientId] = oldestEntry;
    rateLimitByClient.delete(clientId);
  }
}

function isRateLimited(clientId: string, now: number): boolean {
  pruneExpiredRateLimitEntries(now);

  const currentEntry = rateLimitByClient.get(clientId);
  if (!currentEntry || now - currentEntry.windowStart >= MCP_RATE_LIMIT_WINDOW_MS) {
    if (!currentEntry && rateLimitByClient.size >= MCP_RATE_LIMIT_MAX_CLIENTS) {
      evictOldestRateLimitEntry();
    }
    rateLimitByClient.set(clientId, { count: 1, windowStart: now });
    return false;
  }

  if (currentEntry.count >= MCP_RATE_LIMIT_MAX_REQUESTS) {
    return true;
  }

  rateLimitByClient.delete(clientId);
  currentEntry.count += 1;
  rateLimitByClient.set(clientId, currentEntry);
  return false;
}

export function startHttpServer(banyandbClient: BanyanDBClient): void {
  const httpServer = createServer((req, res) => {
    const { pathname } = new URL(req.url ?? '', 'http://localhost');
    if (pathname !== '/mcp') {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }

    if (req.method !== 'POST') {
      res.writeHead(405, {
        'Content-Type': 'application/json',
        Allow: 'POST',
      });
      res.end(JSON.stringify({ error: 'Method not allowed' }));
      return;
    }

    if (MCP_AUTH_TOKEN && getAuthorizationToken(req) !== MCP_AUTH_TOKEN) {
      res.writeHead(401, {
        'Content-Type': 'application/json',
        'WWW-Authenticate': 'Bearer',
      });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    const clientId = getClientIdentifier(req);
    if (isRateLimited(clientId, Date.now())) {
      res.writeHead(429, {
        'Content-Type': 'application/json',
        'Retry-After': `${Math.ceil(MCP_RATE_LIMIT_WINDOW_MS / 1000)}`,
      });
      res.end(JSON.stringify({ error: 'Too many requests' }));
      return;
    }

    const bodyChunks: Buffer[] = [];
    let receivedBytes = 0;
    let requestTooLarge = false;
    req.on('data', (chunk: Buffer) => {
      receivedBytes += chunk.length;
      if (receivedBytes > MCP_MAX_BODY_BYTES) {
        requestTooLarge = true;
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `Request body too large. Limit is ${MCP_MAX_BODY_BYTES} bytes.` }));
        req.destroy();
        return;
      }

      bodyChunks.push(chunk);
    });
    req.on('end', async () => {
      if (requestTooLarge) {
        return;
      }

      let parsedBody: unknown;
      if (receivedBytes > 0) {
        try {
          parsedBody = JSON.parse(Buffer.concat(bodyChunks, receivedBytes).toString('utf-8'));
        } catch {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid JSON in request body' }));
          return;
        }
      }

      try {
        const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
        const mcpServer = createMcpServer(banyandbClient);
        await mcpServer.connect(transport);
        await transport.handleRequest(req, res, parsedBody);
      } catch (error) {
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(
            JSON.stringify({
              error: truncateText(error instanceof Error ? error.message : String(error), MAX_ERROR_MESSAGE_LENGTH),
            }),
          );
        }
      }
    });
  });

  httpServer.listen(MCP_PORT, MCP_HOST, () => {
    log.info(`BanyanDB MCP HTTP server listening on ${MCP_HOST}:${MCP_PORT}/mcp`);
    log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
    log.info(`HTTP auth ${MCP_AUTH_TOKEN ? 'enabled' : 'disabled'}`);
    log.info(`HTTP rate limit ${MCP_RATE_LIMIT_MAX_REQUESTS} requests per ${MCP_RATE_LIMIT_WINDOW_MS}ms`);
  });
}
