/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

const CONFIG_ENV_KEYS = [
  'MCP_PORT',
  'MCP_MAX_BODY_BYTES',
  'MCP_RATE_LIMIT_WINDOW_MS',
  'MCP_RATE_LIMIT_MAX_REQUESTS',
  'MCP_RATE_LIMIT_MAX_CLIENTS',
  'TRANSPORT',
  'BANYANDB_ADDRESS',
  'MCP_HOST',
  'MCP_AUTH_TOKEN',
];

const savedEnv: Record<string, string | undefined> = {};

async function loadConfig(): Promise<typeof import('./config.js')> {
  vi.resetModules();
  return import('./config.js');
}

beforeAllEnv();

function beforeAllEnv() {
  for (const key of CONFIG_ENV_KEYS) {
    savedEnv[key] = process.env[key];
    delete process.env[key];
  }
}

describe('config environment parsing', () => {
  afterEach(() => {
    for (const key of CONFIG_ENV_KEYS) {
      delete process.env[key];
      if (savedEnv[key] !== undefined) {
        process.env[key] = savedEnv[key];
      }
    }
  });

  it('rejects a MCP_PORT with trailing garbage instead of silently parsing it', async () => {
    process.env.MCP_PORT = '3000abc';
    await expect(loadConfig()).rejects.toThrow(/Invalid MCP_PORT value/);
  });

  it('rejects an out-of-range MCP_PORT', async () => {
    process.env.MCP_PORT = '70000';
    await expect(loadConfig()).rejects.toThrow(/Invalid MCP_PORT value/);
  });

  it('accepts a valid MCP_PORT', async () => {
    process.env.MCP_PORT = '4555';
    const config = await loadConfig();
    expect(config.MCP_PORT).toBe(4555);
  });

  it('rejects non-numeric positive integer settings like "1MiB"', async () => {
    process.env.MCP_MAX_BODY_BYTES = '1MiB';
    await expect(loadConfig()).rejects.toThrow(/Invalid MCP_MAX_BODY_BYTES value/);
  });

  it('rejects "60s" for MCP_RATE_LIMIT_WINDOW_MS', async () => {
    process.env.MCP_RATE_LIMIT_WINDOW_MS = '60s';
    await expect(loadConfig()).rejects.toThrow(/Invalid MCP_RATE_LIMIT_WINDOW_MS value/);
  });

  it('normalizes TRANSPORT case-insensitively', async () => {
    process.env.TRANSPORT = 'HTTP';
    const config = await loadConfig();
    expect(config.TRANSPORT).toBe('http');
  });

  it('rejects an unsupported TRANSPORT value', async () => {
    process.env.TRANSPORT = 'streamable-http';
    await expect(loadConfig()).rejects.toThrow(/Invalid TRANSPORT value/);
  });

  it('defaults TRANSPORT to stdio', async () => {
    const config = await loadConfig();
    expect(config.TRANSPORT).toBe('stdio');
  });
});
