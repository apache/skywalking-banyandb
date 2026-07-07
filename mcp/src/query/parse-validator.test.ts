/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { validateBydbQLSyntax, validatorBinaryPath } from './parse-validator.js';

const integrationTimeoutMs = 30000;
const mcpRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');

// Ensure the prebuilt validator binary exists so the spawn round-trip is
// exercised. If the Go toolchain is unavailable the build is skipped and the
// suite is skipped gracefully rather than failing.
function ensureValidatorBinary(): boolean {
  if (existsSync(validatorBinaryPath())) {
    return true;
  }
  const build = spawnSync('node', ['tools/build-validator.js'], {
    cwd: mcpRoot,
    stdio: 'ignore',
    timeout: integrationTimeoutMs,
  });
  return build.status === 0 && existsSync(validatorBinaryPath());
}

const binaryAvailable = ensureValidatorBinary();
const describeIntegration = binaryAvailable ? describe : describe.skip;

describeIntegration('validateBydbQLSyntax (spawn integration)', () => {
  it(
    'round-trips a valid query through the built binary',
    async () => {
      const result = await validateBydbQLSyntax("SELECT * FROM STREAM sw IN default TIME > '-30m'");
      expect(result.valid).toBe(true);
      expect(result.syntaxOnly).toBe(true);
      expect(result.queryType?.toUpperCase()).toBe('STREAM');
    },
    integrationTimeoutMs,
  );

  it(
    'reports SHOW TOP queries as TOPN',
    async () => {
      const result = await validateBydbQLSyntax("SHOW TOP 10 FROM MEASURE m IN g TIME > '-30m' ORDER BY DESC");
      expect(result.valid).toBe(true);
      expect(result.queryType?.toUpperCase()).toBe('TOPN');
    },
    integrationTimeoutMs,
  );

  it(
    'returns valid=false for a query that fails to parse',
    async () => {
      const result = await validateBydbQLSyntax('DELETE FROM STREAM sw IN default');
      expect(result.valid).toBe(false);
      expect(result.syntaxOnly).toBe(true);
    },
    integrationTimeoutMs,
  );
});
