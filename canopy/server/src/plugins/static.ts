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

import { join } from 'node:path';
import { existsSync } from 'node:fs';
import type { FastifyInstance } from 'fastify';
import fastifyStatic from '@fastify/static';

// Anchor to process.cwd() (always the server package dir) rather than __dirname so
// the path is correct both in ts-node (src/plugins/) and compiled (dist/src/plugins/).
const WEB_DIST = join(process.cwd(), '..', 'web', 'dist');

export async function registerStatic(app: FastifyInstance): Promise<void> {
  if (!existsSync(WEB_DIST)) {
    app.log.warn(`[static] web/dist not found at ${WEB_DIST} — SPA serving disabled (run npm run build in web/)`);
    return;
  }

  await app.register(fastifyStatic, {
    root: WEB_DIST,
    prefix: '/',
    // SPA assets must always revalidate. Without these headers the browser
    // caches the index.html + the hash-named bundle, so a `npm run -w web build`
    // that swaps the bundle filename does NOT clear the browser's view of the
    // page — the cached HTML still points at the old hash. Sending
    // no-cache on the HTML and a short max-age on the assets keeps the dev
    // loop tight while still letting the browser avoid refetching on every
    // request within a session.
    cacheControl: false,
    setHeaders: (res, path) => {
      if (path.endsWith('index.html')) {
        res.setHeader('Cache-Control', 'no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
      } else {
        res.setHeader('Cache-Control', 'no-cache, must-revalidate');
      }
    },
    // Do NOT use wildcard here — we handle the SPA fallback manually below
    // to ensure /api, /auth, /monitoring, /healthz are NOT shadowed.
  });

  // SPA fallback: serve index.html for any path not matching API/auth/monitoring/healthz
  app.setNotFoundHandler(async (request, reply) => {
    const path = request.url;
    if (
      path.startsWith('/api/') ||
      path.startsWith('/auth/') ||
      path.startsWith('/monitoring/') ||
      path === '/healthz'
    ) {
      // Let these fall through to 404 naturally (they have real route handlers)
      await reply.status(404).send({ error: 'not_found' });
      return;
    }
    // SPA fallback — always re-validate the HTML.
    reply.header('Cache-Control', 'no-store, must-revalidate');
    reply.header('Pragma', 'no-cache');
    await reply.sendFile('index.html', WEB_DIST);
  });
}
