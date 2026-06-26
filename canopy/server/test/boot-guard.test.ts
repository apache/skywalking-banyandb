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

import { describe, it, expect } from 'vitest';
import { loadConfig } from '../src/config.js';

describe('Boot guards (MF3)', () => {
  it('NODE_ENV=production + CANOPY_DEV_NOAUTH=true → loadConfig throws', () => {
    const orig = { ...process.env };
    process.env.NODE_ENV = 'production';
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.SESSION_SECRET = 'test-secret-32-chars-minimum-xxxxx';
    delete process.env.CANOPY_USERS;

    expect(() => loadConfig()).toThrow(/not allowed in NODE_ENV=production/);

    Object.assign(process.env, orig);
  });

  it('missing SESSION_SECRET → loadConfig throws', () => {
    const orig = { ...process.env };
    process.env.NODE_ENV = 'development';
    process.env.CANOPY_DEV_NOAUTH = 'true';
    delete process.env.SESSION_SECRET;

    expect(() => loadConfig()).toThrow(/SESSION_SECRET/);

    Object.assign(process.env, orig);
  });

  it('missing CANOPY_USERS (no devNoAuth) → loadConfig throws', () => {
    const orig = { ...process.env };
    process.env.NODE_ENV = 'development';
    process.env.SESSION_SECRET = 'test-secret-32-chars-minimum-xxxxx';
    delete process.env.CANOPY_DEV_NOAUTH;
    delete process.env.CANOPY_USERS;

    expect(() => loadConfig()).toThrow(/CANOPY_USERS/);

    Object.assign(process.env, orig);
  });

  it('CANOPY_DEV_NOAUTH=true (non-prod) → loadConfig succeeds with dev admin user', () => {
    const orig = { ...process.env };
    process.env.NODE_ENV = 'development';
    process.env.CANOPY_DEV_NOAUTH = 'true';
    process.env.SESSION_SECRET = 'test-secret-32-chars-minimum-xxxxx';
    delete process.env.CANOPY_USERS;

    expect(() => loadConfig()).not.toThrow();
    const cfg = loadConfig();
    expect(cfg.users.length).toBeGreaterThan(0);
    expect(cfg.users[0].role).toBe('admin');

    Object.assign(process.env, orig);
  });
});
