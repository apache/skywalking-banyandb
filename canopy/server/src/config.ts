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

export type SsoProviderType = 'oidc' | 'oauth2';

export interface SsoProviderConfig {
  readonly id: string;
  readonly name: string;
  readonly type: SsoProviderType;
  readonly clientId: string;
  readonly clientSecret: string;
  // OIDC providers are described by their issuer; endpoints come from its discovery document.
  readonly issuer?: string;
  // OAuth2 providers list their endpoints explicitly.
  readonly authorizeUrl?: string;
  readonly tokenUrl?: string;
  readonly userinfoUrl?: string;
  // Optional list-emails endpoint (GitHub's /user/emails) used when userinfo has no email.
  readonly emailsUrl?: string;
  readonly scopes: readonly string[];
  // Overrides the callback URL derived from the request (needed when the public URL differs).
  readonly redirectUri?: string;
}

/** Maps an SSO login to Canopy roles; role names go through mapRole. Keys of the two maps are lowercase. */
export interface SsoRolesConfig {
  readonly defaultRoles: readonly Role[];
  readonly roleByEmail: Readonly<Record<string, readonly Role[]>>;
  readonly roleByDomain: Readonly<Record<string, readonly Role[]>>;
}

export interface SsoConfig {
  readonly providers: readonly SsoProviderConfig[];
  readonly roles: SsoRolesConfig;
  // Bound on every request to an identity provider (discovery, token, userinfo, health probe).
  readonly timeoutMs: number;
}

export interface BreakGlassConfig {
  readonly enabled: boolean;
  readonly users: CanopyUser[];
}

/** The parts of the sso backend that are re-read at runtime: role mapping and break-glass accounts. */
export interface AuthPolicy {
  readonly roles: SsoRolesConfig;
  readonly breakGlass: BreakGlassConfig | undefined;
}

export interface AuthConfig {
  readonly backend: AuthBackend;
  // Re-reads the role mapping and break-glass accounts from the auth file; absent when they cannot change at runtime.
  readonly reloadPolicy?: () => AuthPolicy;
  // Present when backend is "sso".
  readonly sso?: SsoConfig;
  // Local accounts accepted with backend "sso" only while the identity provider is unhealthy.
  readonly breakGlass?: BreakGlassConfig;
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

const SSO_DEFAULT_TIMEOUT_MS = 5000;
const SSO_PROVIDER_ID = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

function requireString(value: unknown, where: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${where} must be a non-empty string`);
  }
  return value.trim();
}

function optionalUrl(value: unknown, where: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  const text = requireString(value, where);
  let parsed: URL;
  try {
    parsed = new URL(text);
  } catch {
    throw new Error(`${where} must be an absolute http(s) URL`);
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error(`${where} must be an absolute http(s) URL`);
  }
  return text;
}

// The client secret never lives in the config file: it comes from an environment variable
// (clientSecretEnv, default CANOPY_AUTH_SSO_<ID>_CLIENT_SECRET) or a file (clientSecretFile).
function resolveClientSecret(entry: Record<string, unknown>, id: string, where: string): string {
  if (entry.clientSecret !== undefined) {
    throw new Error(`${where}.clientSecret must not be set in the config file; use clientSecretEnv or clientSecretFile`);
  }
  if (entry.clientSecretEnv !== undefined && entry.clientSecretFile !== undefined) {
    throw new Error(`${where}: set only one of clientSecretEnv and clientSecretFile`);
  }
  let secret: string | undefined;
  if (entry.clientSecretFile !== undefined) {
    const file = requireString(entry.clientSecretFile, `${where}.clientSecretFile`);
    if (!existsSync(file)) {
      throw new Error(`${where}.clientSecretFile not found: ${file}`);
    }
    secret = readFileSync(file, 'utf-8').trim();
  } else {
    const envName = entry.clientSecretEnv !== undefined
      ? requireString(entry.clientSecretEnv, `${where}.clientSecretEnv`)
      : `CANOPY_AUTH_SSO_${id.toUpperCase().replace(/[^A-Z0-9]/g, '_')}_CLIENT_SECRET`;
    secret = process.env[envName]?.trim();
    if (!secret) {
      throw new Error(`${where}: client secret is not set; export ${envName} or set clientSecretFile`);
    }
  }
  if (!secret) {
    throw new Error(`${where}: the client secret is empty`);
  }
  return secret;
}

function parseSsoProviders(raw: unknown): SsoProviderConfig[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error(
      'auth.backend "sso" needs auth.sso.providers (at least one provider in CANOPY_AUTH_CONFIG); ' +
      'set CANOPY_AUTH_BACKEND=local (or CANOPY_USERS / auth.local.users) or set CANOPY_DEV_NOAUTH=true for development',
    );
  }
  const seen = new Set<string>();
  return raw.map((entry: unknown, index: number): SsoProviderConfig => {
    const where = `auth.sso.providers[${index}]`;
    if (!isRecord(entry)) {
      throw new Error(`${where} must be a mapping`);
    }
    const id = requireString(entry.id, `${where}.id`);
    if (!SSO_PROVIDER_ID.test(id)) {
      throw new Error(`${where}.id must contain only letters, digits, "_" and "-"`);
    }
    if (seen.has(id)) {
      throw new Error(`${where}: duplicate provider id "${id}"`);
    }
    seen.add(id);
    const type = entry.type;
    if (type !== 'oidc' && type !== 'oauth2') {
      throw new Error(`${where}.type must be "oidc" or "oauth2" (got ${JSON.stringify(type)})`);
    }
    const clientId = requireString(entry.clientId, `${where}.clientId`);
    const clientSecret = resolveClientSecret(entry, id, where);
    const name = entry.name === undefined ? id : requireString(entry.name, `${where}.name`);
    let scopes: string[];
    if (entry.scopes === undefined) {
      scopes = type === 'oidc' ? ['openid', 'email', 'profile'] : ['read:user', 'user:email'];
    } else if (Array.isArray(entry.scopes) && entry.scopes.every(scope => typeof scope === 'string' && scope !== '')) {
      scopes = entry.scopes as string[];
    } else {
      throw new Error(`${where}.scopes must be a list of strings`);
    }
    const base = { id, name, type: type as SsoProviderType, clientId, clientSecret, scopes, redirectUri: optionalUrl(entry.redirectUri, `${where}.redirectUri`) };
    if (type === 'oidc') {
      const issuer = optionalUrl(entry.issuer, `${where}.issuer`);
      if (!issuer) {
        throw new Error(`${where}.issuer is required for an oidc provider`);
      }
      if (!scopes.includes('openid')) {
        throw new Error(`${where}.scopes must include "openid" for an oidc provider`);
      }
      return { ...base, issuer };
    }
    const authorizeUrl = optionalUrl(entry.authorizeUrl, `${where}.authorizeUrl`);
    const tokenUrl = optionalUrl(entry.tokenUrl, `${where}.tokenUrl`);
    const userinfoUrl = optionalUrl(entry.userinfoUrl, `${where}.userinfoUrl`);
    if (!authorizeUrl || !tokenUrl || !userinfoUrl) {
      throw new Error(`${where}: an oauth2 provider needs authorizeUrl, tokenUrl and userinfoUrl`);
    }
    return { ...base, authorizeUrl, tokenUrl, userinfoUrl, emailsUrl: optionalUrl(entry.emailsUrl, `${where}.emailsUrl`) };
  });
}

function parseRoleNames(raw: unknown, where: string): Role[] {
  const names = Array.isArray(raw) ? raw : [raw];
  return names.map(name => {
    const mapped = mapRole(name);
    if (!mapped) {
      throw new Error(`${where} must name roles from admin, readonly, viewer, maintainer, operator (got ${JSON.stringify(name)})`);
    }
    return mapped;
  });
}

function parseRoleMap(raw: unknown, where: string): Record<string, Role[]> {
  if (raw === undefined || raw === null) return {};
  if (!isRecord(raw)) {
    throw new Error(`${where} must be a mapping`);
  }
  const out: Record<string, Role[]> = {};
  for (const [key, value] of Object.entries(raw)) {
    out[key.trim().toLowerCase()] = parseRoleNames(value, `${where}.${key}`);
  }
  return out;
}

function parseSsoRoles(rolesRaw: unknown): SsoRolesConfig {
  if (rolesRaw !== undefined && !isRecord(rolesRaw)) {
    throw new Error('auth.sso.roles must be a mapping');
  }
  const defaultRoles = rolesRaw?.defaultRoles === undefined || rolesRaw.defaultRoles === null
    ? []
    : parseRoleNames(rolesRaw.defaultRoles, 'auth.sso.roles.defaultRoles');
  return {
    defaultRoles,
    roleByEmail: parseRoleMap(rolesRaw?.roleByEmail, 'auth.sso.roles.roleByEmail'),
    roleByDomain: parseRoleMap(rolesRaw?.roleByDomain, 'auth.sso.roles.roleByDomain'),
  };
}

function ssoSection(raw: unknown): Record<string, unknown> {
  if (raw !== undefined && !isRecord(raw)) {
    throw new Error('auth.sso must be a mapping');
  }
  return raw ?? {};
}

function parseSso(raw: unknown): SsoConfig {
  const section = ssoSection(raw);
  const providers = parseSsoProviders(section.providers);
  const roles = parseSsoRoles(section.roles);
  const timeoutRaw = section.timeoutMs;
  const timeoutMs = timeoutRaw === undefined ? SSO_DEFAULT_TIMEOUT_MS : Number(timeoutRaw);
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`auth.sso.timeoutMs must be a positive whole number of milliseconds (got ${JSON.stringify(timeoutRaw)})`);
  }
  return { providers, roles, timeoutMs };
}

function parseBreakGlass(raw: unknown): BreakGlassConfig | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (!isRecord(raw)) {
    throw new Error('auth.breakGlass must be a mapping {enabled, users}');
  }
  if (raw.enabled !== undefined && typeof raw.enabled !== 'boolean') {
    throw new Error('auth.breakGlass.enabled must be true or false');
  }
  const users = parseUsers(raw.users, 'auth.breakGlass.users');
  const enabled = raw.enabled === true;
  if (enabled && users.length === 0) {
    throw new Error('auth.breakGlass.enabled needs at least one entry in auth.breakGlass.users');
  }
  return { enabled, users };
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
  const sso = backend === 'sso' ? parseSso(fileAuth.sso) : undefined;
  // Break-glass only applies to the sso backend; with local auth every user is already a local account.
  const breakGlass = backend === 'sso' ? parseBreakGlass(fileAuth.breakGlass) : undefined;

  if (!devNoAuth) {
    for (const user of [...users, ...(breakGlass?.users ?? [])]) {
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
  // Role mapping and break-glass accounts are re-read like the users files; provider settings need a restart.
  const reloadPolicy = backend === 'sso' && authFile
    ? (): AuthPolicy => {
      const section = readAuthSection(authFile);
      return { roles: parseSsoRoles(ssoSection(section.sso).roles), breakGlass: parseBreakGlass(section.breakGlass) };
    }
    : undefined;
  return { backend, sso, breakGlass, reloadPolicy, local: { users, reload }, tokens, session: { cookieName, ttlMinutes, cookieSecure } };
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
