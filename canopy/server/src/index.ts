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

import Fastify from 'fastify';
import { loadConfig } from './config.js';
import { registerSession } from './plugins/session.js';
import { registerStatic } from './plugins/static.js';
import { registerProxy } from './plugins/proxy.js';
import { registerAuth } from './routes/auth.js';

const config = loadConfig();

const app = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || 'info',
    redact: ['req.headers.authorization', 'res.headers.authorization'],
  },
});

// Security headers
app.addHook('onSend', async (_request, reply) => {
  void reply.header('X-Content-Type-Options', 'nosniff');
  void reply.header('Referrer-Policy', 'strict-origin-when-cross-origin');
  void reply.header(
    'Content-Security-Policy',
    [
      "default-src 'self'",
      "script-src 'self' 'unsafe-inline'",
      "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
      "font-src 'self' https://fonts.gstatic.com",
      "img-src 'self' data:",
      "connect-src 'self'",
    ].join('; '),
  );
});

app.get('/healthz', async () => ({ status: 'ok' }));

async function start() {
  await registerSession(app, config);
  await registerAuth(app, config);
  await registerProxy(app, config);
  await registerStatic(app);

  await app.listen({ port: config.port, host: '0.0.0.0' });
  app.log.info(`Canopy BFF listening on port ${config.port}`);
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
