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

import { toTagValueParams, validateBydbQL } from './validation.js';

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

describe('toTagValueParams (BydbQL parameter validation and conversion)', () => {
  it('converts every parameter type to its protojson TagValue shape', () => {
    expect(
      toTagValueParams([
        { type: 'str', value: 'error' },
        { type: 'int', value: 42 },
        { type: 'int', value: '-7' },
        { type: 'str_array', value: ['a', 'b'] },
        { type: 'int_array', value: [1, '2'] },
        { type: 'null' },
      ]),
    ).toEqual([
      { str: { value: 'error' } },
      { int: { value: '42' } },
      { int: { value: '-7' } },
      { strArray: { value: ['a', 'b'] } },
      { intArray: { value: ['1', '2'] } },
      { null: null },
    ]);
  });

  it('accepts the int64 boundary values', () => {
    expect(
      toTagValueParams([
        { type: 'int', value: '9223372036854775807' },
        { type: 'int', value: '-9223372036854775808' },
      ]),
    ).toEqual([{ int: { value: '9223372036854775807' } }, { int: { value: '-9223372036854775808' } }]);
  });

  it('rejects int64-overflowing values with a specific client-side message', () => {
    expect(() => toTagValueParams([{ type: 'int', value: '9223372036854775808' }])).toThrow(
      'params[0]: int value 9223372036854775808 is out of the int64 range',
    );
    expect(() => toTagValueParams([{ type: 'int', value: '99999999999999999999' }])).toThrow(
      'params[0]: int value 99999999999999999999 is out of the int64 range',
    );
    expect(() => toTagValueParams([{ type: 'int', value: '999999999999999999999' }])).toThrow(
      'must be an integer of at most 20 characters',
    );
  });

  it('rejects non-integer and unsafe number values', () => {
    expect(() => toTagValueParams([{ type: 'int', value: 'abc' }])).toThrow('params[0]: int value must be an integer');
    expect(() => toTagValueParams([{ type: 'int', value: 1.5 }])).toThrow('params[0]: int value must be an integer');
    expect(() => toTagValueParams([{ type: 'int', value: Number.MAX_SAFE_INTEGER + 1 }])).toThrow(
      'params[0]: int value must be an integer',
    );
  });

  it('rejects type mismatches', () => {
    expect(() => toTagValueParams([{ type: 'str', value: 42 }])).toThrow('params[0]: str value must be a string');
    expect(() => toTagValueParams([{ type: 'str_array', value: 'not-an-array' }])).toThrow(
      'params[0]: array value must be a non-empty array',
    );
    expect(() => toTagValueParams([{ type: 'unknown' } as never])).toThrow('params[0]: type must be one of');
    expect(() => toTagValueParams(['bare-string' as never])).toThrow(
      'params[0]: each parameter must be an object with a "type" field',
    );
  });

  it('rejects empty and oversized arrays', () => {
    expect(() => toTagValueParams([{ type: 'str_array', value: [] }])).toThrow(
      'params[0]: array value must be a non-empty array',
    );
    const oversized = Array.from({ length: 257 }, (_, index) => String(index));
    expect(() => toTagValueParams([{ type: 'str_array', value: oversized }])).toThrow(
      'params[0]: array value exceeds 256 entries',
    );
  });

  it('rejects oversized str values and too many parameters', () => {
    expect(() => toTagValueParams([{ type: 'str', value: 'a'.repeat(4097) }])).toThrow(
      'params[0]: value exceeds 4096 characters',
    );
    const tooMany = Array.from({ length: 65 }, () => ({ type: 'str', value: 'x' }) as const);
    expect(() => toTagValueParams([...tooMany])).toThrow('params exceeds 64 entries');
  });
});
