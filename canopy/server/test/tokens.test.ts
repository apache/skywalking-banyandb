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

import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import yaml from 'js-yaml';
import { describe, expect, it } from 'vitest';
import { appendToken } from '../src/lib/token-file.js';
import { bearerCredential, hashSecret, mintToken, parseToken, TokenVerifier } from '../src/lib/tokens.js';

describe('tokens', () => {
  it('mints a verifiable hzn_<id>_<secret> token whose entry holds only the hash', () => {
    const { token, entry } = mintToken('t1', 'alice', 'ci');
    const parsed = parseToken(token);

    expect(token.startsWith('hzn_t1_')).toBe(true);
    expect(parsed?.id).toBe('t1');
    expect(entry.secretHash).toMatch(/^[0-9a-f]{64}$/);
    expect(entry.secretHash).toBe(hashSecret(parsed!.secret));
    expect(JSON.stringify(entry)).not.toContain(parsed!.secret);
    expect(new TokenVerifier([entry]).verify(token)).toEqual(entry);
  });

  it('rejects wrong secrets, unknown ids and malformed tokens', () => {
    const { token, entry } = mintToken('t1', 'alice');
    const verifier = new TokenVerifier([entry]);

    expect(verifier.verify('hzn_t1_other')).toBeUndefined();
    expect(verifier.verify(token.replace('hzn_t1_', 'hzn_t9_'))).toBeUndefined();
    expect(verifier.verify('hzn_t1')).toBeUndefined();
    expect(verifier.verify('nope')).toBeUndefined();
  });

  it('refuses ids that would make the token ambiguous', () => {
    expect(() => mintToken('a_b', 'alice')).toThrow(/id/);
  });

  it('hashes secrets with SHA-256 (FIPS 180-2 test vector for "abc")', () => {
    expect(hashSecret('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  });

  it('treats a bare or empty Bearer header as a presented empty credential', () => {
    expect(bearerCredential(undefined)).toBeUndefined();
    expect(bearerCredential('Basic abc')).toBeUndefined();
    expect(bearerCredential('Bearer')).toBe('');
    expect(bearerCredential('Bearer   ')).toBe('');
    expect(bearerCredential('bearer hzn_t1_x')).toBe('hzn_t1_x');
  });
});

describe('appendToken (mint-token persistence)', () => {
  const tmp = (name: string): string => join(mkdtempSync(join(tmpdir(), 'canopy-mint-')), name);

  it('creates the file and appends entries that verify, never storing the secret', () => {
    const file = tmp('tokens.yaml');
    const first = appendToken(file, 't1', 'alice', 'ci');
    const second = appendToken(file, 't2', 'bob');

    const stored = yaml.load(readFileSync(file, 'utf-8')) as Array<{ id: string; username: string; secretHash: string }>;
    expect(stored.map(e => e.id)).toEqual(['t1', 't2']);
    const verifier = new TokenVerifier(stored);
    expect(verifier.verify(first)?.username).toBe('alice');
    expect(verifier.verify(second)?.username).toBe('bob');
    expect(readFileSync(file, 'utf-8')).not.toContain(parseToken(first)!.secret);
  });

  it('keeps a {tokens: [...]} wrapper and works for JSON files', () => {
    const file = tmp('tokens.json');
    writeFileSync(file, JSON.stringify({ tokens: [{ id: 'old', username: 'carol', secretHash: hashSecret('x') }], note: 'keep' }));

    appendToken(file, 'new', 'alice');

    const stored = JSON.parse(readFileSync(file, 'utf-8')) as { tokens: Array<{ id: string }>; note: string };
    expect(stored.tokens.map(e => e.id)).toEqual(['old', 'new']);
    expect(stored.note).toBe('keep');
  });

  it('refuses a duplicate id and leaves the file unchanged', () => {
    const file = tmp('tokens.yaml');
    appendToken(file, 't1', 'alice');
    const before = readFileSync(file, 'utf-8');

    expect(() => appendToken(file, 't1', 'bob')).toThrow(/already exists/);
    expect(readFileSync(file, 'utf-8')).toBe(before);
  });

  it('refuses to overwrite an existing file that is not a tokens list', () => {
    const file = tmp('auth.yaml');
    const original = 'auth:\n  backend: local\n';
    writeFileSync(file, original);

    expect(() => appendToken(file, 't1', 'alice')).toThrow(/not a tokens file/);
    expect(readFileSync(file, 'utf-8')).toBe(original);

    writeFileSync(file, 'just a string');
    expect(() => appendToken(file, 't1', 'alice')).toThrow(/not a tokens file/);
  });
});
