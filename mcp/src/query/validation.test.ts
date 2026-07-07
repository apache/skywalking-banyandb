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

import { describe, expect, it } from 'vitest';

import { validateBydbQL } from './validation.js';

describe('validateBydbQL (read-only first gate)', () => {
  it('accepts a SELECT query and returns the trimmed value', () => {
    expect(validateBydbQL("  SELECT * FROM STREAM sw IN default TIME > '-30m'  ")).toBe(
      "SELECT * FROM STREAM sw IN default TIME > '-30m'",
    );
  });

  it('accepts a SHOW TOP query (case-insensitive prefix)', () => {
    expect(validateBydbQL("show top 10 FROM MEASURE m IN g TIME > '-30m' ORDER BY DESC")).toBe(
      "show top 10 FROM MEASURE m IN g TIME > '-30m' ORDER BY DESC",
    );
  });

  it('rejects non-read-only statements', () => {
    expect(() => validateBydbQL('DELETE FROM STREAM sw IN default')).toThrow(
      'BydbQL must be a read-only SELECT or SHOW TOP query',
    );
    expect(() => validateBydbQL('DROP STREAM sw')).toThrow('BydbQL must be a read-only SELECT or SHOW TOP query');
  });

  it('rejects statement terminators and comment syntax', () => {
    expect(() => validateBydbQL('SELECT * FROM STREAM sw IN default;')).toThrow(
      'BydbQL contains unsupported multi-statement or comment syntax',
    );
    expect(() => validateBydbQL('SELECT * FROM STREAM sw IN default -- comment')).toThrow(
      'BydbQL contains unsupported multi-statement or comment syntax',
    );
    expect(() => validateBydbQL('SELECT * FROM STREAM sw IN default /* comment */')).toThrow(
      'BydbQL contains unsupported multi-statement or comment syntax',
    );
  });

  it('rejects empty input', () => {
    expect(() => validateBydbQL('   ')).toThrow('BydbQL cannot be empty');
  });

  it('rejects over-length input (> 4096 characters)', () => {
    const overLength = `SELECT * FROM STREAM sw IN default WHERE service = '${'a'.repeat(4096)}'`;
    expect(() => validateBydbQL(overLength)).toThrow('BydbQL exceeds the maximum length of 4096 characters');
  });
});
