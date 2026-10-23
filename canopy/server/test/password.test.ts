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

import { argon2id, argon2Verify } from 'hash-wasm';
import { describe, expect, it, vi } from 'vitest';
import bcrypt from 'bcryptjs';

import { verifyPassword } from '../src/lib/password.js';

vi.mock('hash-wasm', async importOriginal => {
  const actual = await importOriginal<typeof import('hash-wasm')>();
  return { ...actual, argon2Verify: vi.fn(actual.argon2Verify) };
});

async function argon(password: string): Promise<string> {
  return argon2id({
    password,
    salt: new Uint8Array(16).fill(7),
    parallelism: 1,
    iterations: 1,
    memorySize: 64,
    hashLength: 32,
    outputType: 'encoded',
  });
}

describe('verifyPassword', () => {
  it('verifies a standard argon2id PHC string', async () => {
    const hash = await argon('s3cret');
    expect(hash).toMatch(/^\$argon2id\$v=19\$m=64,t=1,p=1\$/);
    expect(await verifyPassword(hash, 's3cret')).toBe(true);
    expect(await verifyPassword(hash, 'wrong')).toBe(false);
  });

  it('verifies the PHC string produced by the native argon2 library (argon2-cffi)', async () => {
    const hash = '$argon2id$v=19$m=19456,t=2,p=1$qtvJUo3yRJ9Q7yeDuLd6lg$18rU5YxB7kD3+TkDcnz/k0Jb9fyO8PHzwbo/nJKu1To';
    expect(await verifyPassword(hash, 'password')).toBe(true);
  });

  it('still verifies legacy bcrypt hashes', async () => {
    const hash = await bcrypt.hash('legacy', 4);
    expect(await verifyPassword(hash, 'legacy')).toBe(true);
    expect(await verifyPassword(hash, 'nope')).toBe(false);
  });

  it('is false for an unknown user and for malformed hashes', async () => {
    vi.mocked(argon2Verify).mockClear();
    expect(await verifyPassword(undefined, 'anything')).toBe(false);
    // An unknown user must still cost one argon2id verification (timing equalisation).
    expect(argon2Verify).toHaveBeenCalledTimes(1);
    expect(vi.mocked(argon2Verify).mock.calls[0][0].hash).toMatch(/^\$argon2id\$v=19\$/);
    expect(await verifyPassword('$argon2id$garbage', 'x')).toBe(false);
  });
});
