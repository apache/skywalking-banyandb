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
import { isArgon2idHash, isBcryptHash } from './lib/password.js';
import { mapRole, type Role } from './lib/role.js';
import { isSha256Hex, TOKEN_ID_PATTERN, type TokenEntry } from './lib/tokens.js';

export interface CanopyUser {
  readonly username: string;
  readonly passwordHash: string;
  readonly role: Role;
}

export type AuthBackend = 'local' | 'sso';

export interface SessionConfig {
  readonly cookieName: string;
  readonly ttlMinutes: number;
  // 'auto' keeps the cookie Secure only when the request arrived over HTTPS.
  readonly cookieSecure: boolean | 'auto';
}

export interface AuthConfig {
  readonly backend: AuthBackend;
  readonly local: {
    readonly users: CanopyUser[];
    // Re-reads the configured user files; absent when users cannot change at runtime (dev no-auth).
    readonly reload?: () => CanopyUser[];
  };
  // API tokens (hzn_<id>_<secret>); absent means none are configured.
  readonly tokens?: readonly TokenEntry[];
  readonly session: SessionConfig;
}

export interface Config {
  readonly port: number;
  readonly sessionSecret: string;
  readonly banyandbTarget: string;
  readonly monitorTarget: string;
  readonly upstreamTimeoutMs: number;
  readonly upstreamUsername?: string;
  readonly upstreamPassword?: string;
  readonly auth: AuthConfig;
  readonly devNoAuth: boolean;
  readonly blockRfc1918: boolean;
  readonly basePath: string;
  readonly baseHref: string;
}

export function normalizeBasePath(value: string | undefined): { basePath: string; baseHref: string } {
  const input = value?.trim() || '/';
  if (input.startsWith('//') || !/^\/?[A-Za-z0-9._~/-]+$/.test(input) || /(?:^|\/)\.\.?($|\/)/.test(input)) {
    throw new Error('CANOPY_BASE_PATH must be a URL path without a query, fragment, backslash, or traversal segment');
  }
  let decoded: string;
  try {
    decoded = decodeURIComponent(input);
  } catch {
    throw new Error('CANOPY_BASE_PATH must contain valid URL encoding');
  }
  if (decoded.startsWith('//') || !/^\/?[A-Za-z0-9._~/-]+$/.test(decoded) || /(?:^|\/)\.\.?($|\/)/.test(decoded)) {
    throw new Error('CANOPY_BASE_PATH must not contain encoded traversal segments');
  }
  const withLeadingSlash = input.startsWith('/') ? input : `/${input}`;
  const basePath = withLeadingSlash.replace(/\/+$/, '') || '/';
  return { basePath, baseHref: basePath === '/' ? '/' : `${basePath}/` };
}

function readStructuredFile(path: string, what: string): unknown {
  if (!existsSync(path)) {
    throw new Error(`${what} file not found: ${path}`);
  }
  const raw = readFileSync(path, 'utf-8');
  return path.endsWith('.json') ? JSON.parse(raw) : yaml.load(raw);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseUsers(raw: unknown, source: string): CanopyUser[] {
  if (raw === undefined || raw === null) {
    return [];
  }
  if (!Array.isArray(raw)) {
    throw new Error(`${source} must be an array of {username, passwordHash, role}`);
  }
  const seen = new Set<string>();
  return raw.map((entry: unknown, index: number): CanopyUser => {
    const where = `${source}[${index}]`;
    if (!isRecord(entry)) {
      throw new Error(`${where} must be an object with username, passwordHash and role`);
    }
    const { username, passwordHash, role } = entry;
    if (typeof username !== 'string' || username === '') {
      throw new Error(`${where}.username must be a non-empty string`);
    }
    if (seen.has(username)) {
      throw new Error(`${where}: duplicate username "${username}"`);
    }
    seen.add(username);
    if (typeof passwordHash !== 'string' || !(isArgon2idHash(passwordHash) || isBcryptHash(passwordHash))) {
      throw new Error(`${where}.passwordHash for "${username}" must be an argon2id PHC string ($argon2id$...) or a bcrypt hash`);
    }
    const mapped = mapRole(role);
    if (!mapped) {
      throw new Error(`${where}.role for "${username}" must be one of admin, readonly, viewer, maintainer, operator (got ${JSON.stringify(role)})`);
    }
    return { username, passwordHash, role: mapped };
  });
}

function parseTokens(raw: unknown, source: string): TokenEntry[] {
  const list = isRecord(raw) ? raw.tokens : raw;
  if (list === undefined || list === null) {
    return [];
  }
  if (!Array.isArray(list)) {
    throw new Error(`${source} must be an array of {id, username, secretHash, description?}`);
  }
  const seen = new Set<string>();
  return list.map((entry: unknown, index: number): TokenEntry => {
    const where = `${source}[${index}]`;
    if (!isRecord(entry)) {
      throw new Error(`${where} must be an object with id, username and secretHash`);
    }
    const { id, username, secretHash, description } = entry;
    if (typeof id !== 'string' || !TOKEN_ID_PATTERN.test(id)) {
      throw new Error(`${where}.id must be a non-empty string of letters, digits and "-"`);
    }
    if (seen.has(id)) {
      throw new Error(`${where}: duplicate token id "${id}"`);
    }
    seen.add(id);
    if (typeof username !== 'string' || username === '') {
      throw new Error(`${where}.username must be a non-empty string`);
    }
    if (typeof secretHash !== 'string' || !isSha256Hex(secretHash)) {
      throw new Error(`${where}.secretHash for token "${id}" must be a SHA-256 hex digest (64 hex characters)`);
    }
    if (description !== undefined && typeof description !== 'string') {
      throw new Error(`${where}.description must be a string`);
    }
    return description === undefined ? { id, username, secretHash } : { id, username, secretHash, description };
  });
}

function parseBackend(value: unknown, source: string): AuthBackend | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (value !== 'local' && value !== 'sso') {
    throw new Error(`auth.backend (${source}) must be "local" or "sso" (got ${JSON.stringify(value)})`);
  }
  return value;
}

function parseCookieSecure(value: unknown, source: string): boolean | 'auto' | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (value === true || value === 'true') return true;
  if (value === false || value === 'false') return false;
  if (value === 'auto') return 'auto';
  throw new Error(`${source} must be true, false or auto`);
}

function readAuthSection(authFile: string | undefined): Record<string, unknown> {
  if (!authFile) {
    return {};
  }
  const parsed = readStructuredFile(authFile, 'CANOPY_AUTH_CONFIG');
  if (parsed === undefined || parsed === null) {
    return {};
  }
  if (!isRecord(parsed) || (parsed.auth !== undefined && !isRecord(parsed.auth))) {
    throw new Error('CANOPY_AUTH_CONFIG must contain a top-level "auth" mapping');
  }
  return (parsed.auth as Record<string, unknown> | undefined) ?? {};
}

// Reads auth.local.users plus the CANOPY_USERS alias from the files they live in.
function readConfiguredUsers(authFile: string | undefined, aliasPath: string | undefined): CanopyUser[] {
  const local = readAuthSection(authFile).local;
  if (local !== undefined && !isRecord(local)) {
    throw new Error('auth.local must be a mapping');
  }
  let users = parseUsers(local?.users, 'auth.local.users');
  if (aliasPath) {
    // CANOPY_USERS is an alias for auth.local.users.
    const aliasUsers = parseUsers(readStructuredFile(aliasPath, 'CANOPY_USERS'), 'CANOPY_USERS');
    if (users.length > 0 && aliasUsers.length > 0) {
      throw new Error('CANOPY_USERS and auth.local.users are both set; configure users in only one of them');
    }
    if (aliasUsers.length > 0) users = aliasUsers;
  }
  return users;
}

function loadAuth(devNoAuth: boolean): AuthConfig {
  const authFile = process.env.CANOPY_AUTH_CONFIG;
  const fileAuth = readAuthSection(authFile);
  const fileLocal = fileAuth.local;
  if (fileLocal !== undefined && !isRecord(fileLocal)) {
    throw new Error('auth.local must be a mapping');
  }
  const fileSession = fileAuth.session;
  if (fileSession !== undefined && !isRecord(fileSession)) {
    throw new Error('auth.session must be a mapping');
  }

  // The dev no-auth override comes first so a stale users file is never read.
  let users: CanopyUser[];
  const aliasPath = process.env.CANOPY_USERS;
  if (devNoAuth) {
    // bcryptjs.hashSync is fine at startup (runs once); cost 10 is the standard dev default.
    users = [{ username: 'admin', passwordHash: bcrypt.hashSync('admin', 10), role: 'admin' }];
  } else {
    users = readConfiguredUsers(authFile, aliasPath);
  }

  const explicitBackend = parseBackend(process.env.CANOPY_AUTH_BACKEND, 'CANOPY_AUTH_BACKEND')
    ?? parseBackend(fileAuth.backend, 'auth.backend');
  // Existing deployments that only set CANOPY_USERS (or the dev switch) keep working as local.
  const backend: AuthBackend = explicitBackend ?? (aliasPath || devNoAuth ? 'local' : 'sso');
  if (backend === 'sso') {
    throw new Error(
      'auth.backend "sso" is not yet supported; set CANOPY_AUTH_BACKEND=local (or CANOPY_USERS / auth.local.users) ' +
      'or set CANOPY_DEV_NOAUTH=true for development',
    );
  }

  if (!devNoAuth) {
    for (const user of users) {
      if (isBcryptHash(user.passwordHash)) {
        console.warn(`[CANOPY] WARNING: user "${user.username}" has a bcrypt password hash; rehash it with argon2id (see canopy/README.md).`);
      }
    }
  }

  const ttlRaw = process.env.CANOPY_AUTH_SESSION_TTL_MINUTES ?? fileSession?.ttlMinutes;
  const ttlMinutes = ttlRaw === undefined || ttlRaw === '' ? 60 : Number(ttlRaw);
  if (!Number.isInteger(ttlMinutes) || ttlMinutes <= 0) {
    throw new Error(`auth.session.ttlMinutes must be a positive whole number of minutes (got ${JSON.stringify(ttlRaw)})`);
  }
  const cookieNameRaw = process.env.CANOPY_AUTH_SESSION_COOKIE_NAME || fileSession?.cookieName;
  const cookieName = cookieNameRaw === undefined ? 'canopy_sid' : cookieNameRaw;
  if (typeof cookieName !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(cookieName)) {
    throw new Error('auth.session.cookieName must be a cookie-safe name (letters, digits, "_", ".", "-")');
  }
  const cookieSecure = parseCookieSecure(process.env.CANOPY_AUTH_SESSION_COOKIE_SECURE, 'CANOPY_AUTH_SESSION_COOKIE_SECURE')
    ?? parseCookieSecure(fileSession?.cookieSecure, 'auth.session.cookieSecure')
    ?? 'auto';

  const tokensFile = process.env.CANOPY_AUTH_TOKENS_FILE || fileAuth.tokensFile;
  if (tokensFile !== undefined && (typeof tokensFile !== 'string' || tokensFile === '')) {
    throw new Error('auth.tokensFile must be a file path');
  }
  const tokens = tokensFile ? parseTokens(readStructuredFile(tokensFile, 'auth.tokensFile'), 'auth.tokensFile') : [];

  const reload = devNoAuth ? undefined : () => readConfiguredUsers(authFile, aliasPath);
  return { backend, local: { users, reload }, tokens, session: { cookieName, ttlMinutes, cookieSecure } };
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
    throw new Error('CANOPY_DEV_NOAUTH=true is not allowed in NODE_ENV=production. Configure auth.local.users (CANOPY_AUTH_CONFIG or CANOPY_USERS) and remove CANOPY_DEV_NOAUTH.');
  }

  if (devNoAuth) {
    console.warn('[CANOPY] WARNING: CANOPY_DEV_NOAUTH=true — using dev admin/admin credentials. DO NOT USE IN PRODUCTION.');
  }

  const sessionSecret = process.env.SESSION_SECRET;
  if (!sessionSecret || sessionSecret.length < 32) {
    throw new Error('SESSION_SECRET env var is required and must be at least 32 characters');
  }

  const auth = loadAuth(devNoAuth);

  const upstreamPassword = resolveUpstreamPassword();
  const { basePath, baseHref } = normalizeBasePath(process.env.CANOPY_BASE_PATH);

  return {
    port: parseInt(process.env.PORT || '4000', 10),
    sessionSecret,
    banyandbTarget: process.env.BANYANDB_TARGET || 'http://127.0.0.1:17913',
    monitorTarget: process.env.MONITOR_TARGET || 'http://127.0.0.1:2121',
    upstreamTimeoutMs: parseInt(process.env.UPSTREAM_TIMEOUT_MS || '120000', 10),
    upstreamUsername: process.env.BANYANDB_UPSTREAM_USERNAME || undefined,
    upstreamPassword,
    auth,
    devNoAuth,
    blockRfc1918: process.env.BLOCK_RFC1918 === 'true',
    basePath,
    baseHref,
  };
}
