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

import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

export const TOKEN_PREFIX = 'hzn_';

/** A tokens-file entry. `secretHash` is the SHA-256 hex digest of the token secret. */
export interface TokenEntry {
  readonly id: string;
  readonly username: string;
  readonly secretHash: string;
  readonly description?: string;
}

export const TOKEN_ID_PATTERN = /^[A-Za-z0-9-]+$/;
const TOKEN_PATTERN = /^hzn_([A-Za-z0-9-]+)_([A-Za-z0-9_-]+)$/;
const SHA256_HEX = /^[0-9a-fA-F]{64}$/;

export function isSha256Hex(value: string): boolean {
  return SHA256_HEX.test(value);
}

export function hashSecret(secret: string): string {
  return createHash('sha256').update(secret, 'utf8').digest('hex');
}

/** Splits `hzn_<id>_<secret>` into its parts, or returns undefined when malformed. */
export function parseToken(raw: string): { id: string; secret: string } | undefined {
  const match = TOKEN_PATTERN.exec(raw);
  return match ? { id: match[1], secret: match[2] } : undefined;
}

/** Extracts the credential of an `Authorization: Bearer ...` header, or undefined for other schemes. */
export function bearerCredential(header: string | undefined): string | undefined {
  // A bare "Bearer" still counts as a presented (empty) credential, so it is rejected
  // rather than silently falling back to the session cookie.
  const match = header === undefined ? null : /^Bearer(?:\s+(.*))?$/i.exec(header);
  return match ? (match[1] ?? '').trim() : undefined;
}

/** Mints a new token for a user; the plain token must be shown once and never stored. */
export function mintToken(id: string, username: string, description?: string): { token: string; entry: TokenEntry } {
  if (!TOKEN_ID_PATTERN.test(id)) {
    throw new Error('token id must contain only letters, digits and "-"');
  }
  const secret = randomBytes(32).toString('base64url');
  const entry: TokenEntry = description ? { id, username, secretHash: hashSecret(secret), description } : { id, username, secretHash: hashSecret(secret) };
  return { token: `${TOKEN_PREFIX}${id}_${secret}`, entry };
}

const MISSING_HASH = Buffer.alloc(32);

/** Looks up and verifies a token in constant time; returns the matching entry or undefined. */
export class TokenVerifier {
  private readonly byId = new Map<string, TokenEntry>();

  constructor(entries: readonly TokenEntry[]) {
    for (const entry of entries) this.byId.set(entry.id, entry);
  }

  verify(raw: string): TokenEntry | undefined {
    const parsed = parseToken(raw);
    if (!parsed) return undefined;
    const entry = this.byId.get(parsed.id);
    const actual = createHash('sha256').update(parsed.secret, 'utf8').digest();
    // An unknown id still runs one comparison so timing does not reveal which ids exist.
    const expected = entry ? Buffer.from(entry.secretHash, 'hex') : MISSING_HASH;
    const equal = expected.length === actual.length && timingSafeEqual(expected, actual);
    return entry && equal ? entry : undefined;
  }
}
