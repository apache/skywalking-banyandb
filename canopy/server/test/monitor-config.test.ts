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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config.js';

describe('monitor configuration', () => {
  let originalEnv: NodeJS.ProcessEnv;
  beforeEach(() => {
    originalEnv = { ...process.env };
    process.env.NODE_ENV = 'test';
    process.env.SESSION_SECRET = 'test-secret-32-chars-minimum-xxxxx';
    process.env.CANOPY_DEV_NOAUTH = 'true';
    delete process.env.CANOPY_MONITOR_ALLOW_READONLY;
    delete process.env.CANOPY_MONITOR_REFRESH_INTERVAL_MS;
    delete process.env.CANOPY_MONITOR_EXPERIMENTAL;
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });
  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it('defaults to admin-only access, ten-second refresh and hidden experimental sections', () => {
    expect(loadConfig().monitor).toEqual({ allowReadonly: false, refreshIntervalMs: 10000, experimentalSections: false });
  });

  it('accepts explicit readonly, experimental, and bounded polling settings', () => {
    process.env.CANOPY_MONITOR_ALLOW_READONLY = 'true';
    process.env.CANOPY_MONITOR_EXPERIMENTAL = 'true';
    process.env.CANOPY_MONITOR_REFRESH_INTERVAL_MS = '5000';
    expect(loadConfig().monitor).toEqual({ allowReadonly: true, refreshIntervalMs: 5000, experimentalSections: true });
    process.env.CANOPY_MONITOR_REFRESH_INTERVAL_MS = '300000';
    expect(loadConfig().monitor?.refreshIntervalMs).toBe(300000);
  });

  it.each(['', '4999', '300001', '10000ms', '5e3', 'NaN', '5000.5'])('rejects invalid polling interval %j', value => {
    process.env.CANOPY_MONITOR_REFRESH_INTERVAL_MS = value;
    expect(() => loadConfig()).toThrow('CANOPY_MONITOR_REFRESH_INTERVAL_MS');
  });

  it.each(['CANOPY_MONITOR_ALLOW_READONLY', 'CANOPY_MONITOR_EXPERIMENTAL'])('rejects ambiguous boolean %s', key => {
    process.env[key] = 'yes';
    expect(() => loadConfig()).toThrow(key);
  });
});
