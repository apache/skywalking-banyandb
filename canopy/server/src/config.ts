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

import { readFileSync, existsSync } from 'node:fs';
import yaml from 'js-yaml';
import bcrypt from 'bcryptjs';

export interface CanopyUser {
  readonly username: string;
  readonly passwordHash: string;
  readonly role: 'admin' | 'readonly';
}

export interface Config {
  readonly port: number;
  readonly sessionSecret: string;
  readonly banyandbTarget: string;
  readonly monitorTarget: string;
  readonly upstreamTimeoutMs: number;
  readonly upstreamUsername?: string;
  readonly upstreamPassword?: string;
  readonly users: CanopyUser[];
  readonly devNoAuth: boolean;
  readonly blockRfc1918: boolean;
}

function loadUsers(canopyUsersPath: string | undefined, devNoAuth: boolean): CanopyUser[] {
  if (devNoAuth) {
    // bcryptjs.hashSync is fine at startup (runs once); cost 10 is the standard dev default.
    const devHash = bcrypt.hashSync('admin', 10);
    return [{ username: 'admin', passwordHash: devHash, role: 'admin' }];
  }
  if (!canopyUsersPath) {
    throw new Error('CANOPY_USERS env var is required (path to users JSON/YAML file), or set CANOPY_DEV_NOAUTH=true for development');
  }
  if (!existsSync(canopyUsersPath)) {
    throw new Error(`CANOPY_USERS file not found: ${canopyUsersPath}`);
  }
  const raw = readFileSync(canopyUsersPath, 'utf-8');
  const parsed = canopyUsersPath.endsWith('.json') ? JSON.parse(raw) : yaml.load(raw);
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error(`CANOPY_USERS file must be a non-empty array of {username, passwordHash, role}`);
  }
  return parsed as CanopyUser[];
}

function resolveUpstreamPassword(): string | undefined {
  const file = process.env.BANYANDB_UPSTREAM_PASSWORD_FILE;
  if (file && existsSync(file)) {
    return readFileSync(file, 'utf-8').trim();
  }
  return process.env.BANYANDB_UPSTREAM_PASSWORD || undefined;
}

export function loadConfig(): Config {
  const devNoAuth = process.env.CANOPY_DEV_NOAUTH === 'true';
  const isProd = process.env.NODE_ENV === 'production';

  if (devNoAuth && isProd) {
    throw new Error('CANOPY_DEV_NOAUTH=true is not allowed in NODE_ENV=production. Set CANOPY_USERS and remove CANOPY_DEV_NOAUTH.');
  }

  if (devNoAuth) {
    console.warn('[CANOPY] WARNING: CANOPY_DEV_NOAUTH=true — using dev admin/admin credentials. DO NOT USE IN PRODUCTION.');
  }

  const sessionSecret = process.env.SESSION_SECRET;
  if (!sessionSecret || sessionSecret.length < 32) {
    throw new Error('SESSION_SECRET env var is required and must be at least 32 characters');
  }

  const users = loadUsers(process.env.CANOPY_USERS, devNoAuth);

  const upstreamPassword = resolveUpstreamPassword();

  return {
    port: parseInt(process.env.PORT || '4000', 10),
    sessionSecret,
    banyandbTarget: process.env.BANYANDB_TARGET || 'http://127.0.0.1:17913',
    monitorTarget: process.env.MONITOR_TARGET || 'http://127.0.0.1:2121',
    upstreamTimeoutMs: parseInt(process.env.UPSTREAM_TIMEOUT_MS || '120000', 10),
    upstreamUsername: process.env.BANYANDB_UPSTREAM_USERNAME || undefined,
    upstreamPassword,
    users,
    devNoAuth,
    blockRfc1918: process.env.BLOCK_RFC1918 === 'true',
  };
}
