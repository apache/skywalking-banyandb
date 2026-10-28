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

import bcrypt from 'bcryptjs';
import { argon2Verify } from 'hash-wasm';

// A valid argon2id PHC string used when the username is unknown, so an unknown
// user costs one argon2id verification just like a known one.
const DUMMY_ARGON2ID_HASH =
  '$argon2id$v=19$m=19456,t=3,p=1$SGTSvVj9STeDVo9r+1w+AA$30d/k+glX7kCFS1g+tl3kYpYs5z0C7v74j27p3C69Eg';

export function isBcryptHash(hash: string): boolean {
  return /^\$2[aby]\$/.test(hash);
}

export function isArgon2idHash(hash: string): boolean {
  return hash.startsWith('$argon2id$');
}

/**
 * Verifies a password against an argon2id PHC string or a legacy bcrypt hash.
 * A missing hash (unknown user) verifies against the dummy hash and is always false.
 */
export async function verifyPassword(hash: string | undefined, password: string): Promise<boolean> {
  try {
    if (hash === undefined) {
      await argon2Verify({ password, hash: DUMMY_ARGON2ID_HASH });
      return false;
    }
    if (isBcryptHash(hash)) {
      return await bcrypt.compare(password, hash);
    }
    return await argon2Verify({ password, hash });
  } catch {
    return false;
  }
}
