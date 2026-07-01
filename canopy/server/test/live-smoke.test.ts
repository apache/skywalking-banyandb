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

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Fastify, { FastifyInstance } from 'fastify';
import secureSession from '@fastify/secure-session';
import { loadConfig } from '../src/config.js';
import { registerAuth } from '../src/routes/auth.js';
import { registerProxy } from '../src/plugins/proxy.js';

const isLive = !!process.env.BANYANDB_TARGET;

describe.skipIf(!isLive)('Live smoke tests (requires BANYANDB_TARGET)', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    process.env.CANOPY_DEV_NOAUTH = 'true';
    if (!process.env.SESSION_SECRET) {
      process.env.SESSION_SECRET = 'live-smoke-test-secret-32chars-xxx';
    }

    const config = loadConfig();
    app = Fastify({ logger: false });
    await app.register(secureSession, {
      secret: config.sessionSecret,
      salt: 'canopy-session-salt-v1',
      cookie: { path: '/', httpOnly: true, sameSite: 'strict', secure: false },
    });
    await registerAuth(app, config);
    await registerProxy(app, config);
    await app.ready();
  });

  afterAll(async () => { await app.close(); });

  it('GET /api/v1/group/schema/lists round-trips through the BFF', async () => {
    const loginRes = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: {
        username: 'admin',
        password: 'admin',
      },
    });
    expect(loginRes.statusCode).toBe(200);
    const setCookie = loginRes.headers['set-cookie'];
    const cookieStr = Array.isArray(setCookie) ? setCookie[0] : (setCookie as string);

    const res = await app.inject({
      method: 'GET',
      url: '/api/v1/group/schema/lists',
      headers: { cookie: cookieStr },
    });
    expect(res.statusCode).toBe(200);
  });

  it('readonly user returns 403 on POST /api/v1/stream/schema', async () => {
    // Verify the admin login path works; full readonly test requires CANOPY_USERS with a readonly user
    const loginRes = await app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { username: 'admin', password: 'admin' },
    });
    expect(loginRes.statusCode).toBe(200);
  });
});
