/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';

import type { Config } from '../src/config.js';
import { registerSession } from '../src/plugins/session.js';

const TEST_CONFIG: Config = {
  port: 4000,
  sessionSecret: 'test-secret-32-chars-minimum-xxxxx',
  banyandbTarget: 'http://127.0.0.1:17913',
  monitorTarget: 'http://127.0.0.1:2121',
  upstreamTimeoutMs: 120000,
  users: [],
  devNoAuth: false,
  blockRfc1918: false,
  basePath: '/',
  baseHref: '/',
};

function cookieHeader(setCookie: string | string[] | undefined): string {
  if (Array.isArray(setCookie)) {
    return setCookie[0];
  }
  return setCookie ?? '';
}

describe('session cookies', () => {
  const apps: FastifyInstance[] = [];

  afterEach(async () => {
    await Promise.all(apps.splice(0).map(app => app.close()));
  });

  async function buildApp(config: Config = TEST_CONFIG): Promise<FastifyInstance> {
    const app = Fastify({ trustProxy: true });
    apps.push(app);
    await registerSession(app, config);
    app.get('/login', async request => {
      request.session.user = 'admin';
      request.session.role = 'admin';
      request.session.banyanVersion = null;
      return { ok: true };
    });
    return app;
  }

  it('scopes the session cookie to the configured base path', async () => {
    const app = await buildApp({ ...TEST_CONFIG, basePath: '/canopy', baseHref: '/canopy/' });

    const response = await app.inject({ method: 'GET', url: '/login' });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).toContain('; Path=/canopy/');
  });

  it('sets a Secure cookie when TLS is terminated by a proxy', async () => {
    const app = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: '/login',
      headers: { 'x-forwarded-proto': 'https' },
    });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).toContain('; Secure');
  });

  it('does not set a Secure cookie over HTTP', async () => {
    const app = await buildApp();

    const response = await app.inject({ method: 'GET', url: '/login' });

    expect(response.statusCode).toBe(200);
    expect(cookieHeader(response.headers['set-cookie'])).not.toContain('; Secure');
  });
});
