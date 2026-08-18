/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config.js';

const TEST_SECRET = 'test-secret-32-chars-minimum-xxxxx';
let originalEnv: NodeJS.ProcessEnv;

describe('CANOPY_BASE_PATH', () => {
  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.NODE_ENV = 'test';
    process.env.SESSION_SECRET = TEST_SECRET;
    process.env.CANOPY_DEV_NOAUTH = 'true';
    delete process.env.CANOPY_BASE_PATH;
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it.each([
    [undefined, '/', '/'],
    ['/', '/', '/'],
    ['canopy', '/canopy', '/canopy/'],
    ['/canopy', '/canopy', '/canopy/'],
    ['/canopy/', '/canopy', '/canopy/'],
    ['/tools/banyandb/canopy', '/tools/banyandb/canopy', '/tools/banyandb/canopy/'],
  ])('normalizes %s', (configuredPath, expectedPath, expectedHref) => {
    if (configuredPath !== undefined) {
      process.env.CANOPY_BASE_PATH = configuredPath;
    }

    const config = loadConfig();

    expect(config.basePath).toBe(expectedPath);
    expect(config.baseHref).toBe(expectedHref);
  });

  it.each([
    'http://example.com/canopy',
    '//example.com/canopy',
    '/canopy?mode=admin',
    '/canopy#settings',
    '/canopy/../admin',
    '/canopy\\admin',
    '/%2e%2e/admin',
  ])('rejects invalid value %j', configuredPath => {
    process.env.CANOPY_BASE_PATH = configuredPath;

    expect(() => loadConfig()).toThrow(/CANOPY_BASE_PATH/);
  });
});
