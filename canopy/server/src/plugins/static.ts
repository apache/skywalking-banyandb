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
    // SPA fallback
    await reply.sendFile('index.html', WEB_DIST);
  });
}
