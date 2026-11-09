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
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

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

describe('dotenv loading', () => {
  const originalCwd = process.cwd();

  afterEach(() => {
    process.chdir(originalCwd);
  });

  async function withEnvFile(contents: string, fn: () => Promise<void>): Promise<void> {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'banyandb-mcp-env-'));
    await fs.writeFile(path.join(tmpDir, '.env'), contents, 'utf-8');
    process.chdir(tmpDir);
    try {
      await fn();
    } finally {
      process.chdir(originalCwd);
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
  }

  it('reads BANYANDB_ADDRESS, MCP_HOST, and MCP_AUTH_TOKEN from a .env file', async () => {
    for (const key of ['BANYANDB_ADDRESS', 'MCP_HOST', 'MCP_AUTH_TOKEN']) {
      delete process.env[key];
    }
    await withEnvFile(
      ['BANYANDB_ADDRESS=banyandb.example:17910', 'MCP_HOST=0.0.0.0', 'MCP_AUTH_TOKEN=env-file-token'].join('\n'),
      async () => {
        const config = await loadConfig();
        expect(config.BANYANDB_ADDRESS).toBe('banyandb.example:17910');
        expect(config.MCP_HOST).toBe('0.0.0.0');
        expect(config.MCP_AUTH_TOKEN).toBe('env-file-token');
      },
    );
  });

  it('prefers an already-set process environment variable over the .env file', async () => {
    process.env.MCP_AUTH_TOKEN = 'process-env-token';
    await withEnvFile('MCP_AUTH_TOKEN=env-file-token', async () => {
      const config = await loadConfig();
      expect(config.MCP_AUTH_TOKEN).toBe('process-env-token');
    });
  });
});
