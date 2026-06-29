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

import { describe, it, expect } from 'vitest';

import { fuzzyFilter, fuzzyScore } from './Combobox.js';

describe('fuzzyScore', () => {
  it('returns 1 for empty query (everything matches)', () => {
    expect(fuzzyScore('', 'anything')).toBe(1);
  });

  it('ranks exact match highest', () => {
    expect(fuzzyScore('foo', 'foo')).toBe(1000);
  });

  it('ranks prefix match above substring', () => {
    expect(fuzzyScore('foo', 'foobar')).toBeGreaterThan(fuzzyScore('foo', 'barfoo'));
  });

  it('ranks substring match above subsequence', () => {
    expect(fuzzyScore('bar', 'foobar')).toBeGreaterThan(fuzzyScore('far', 'foobar'));
  });

  it('returns 0 when no subsequence match', () => {
    expect(fuzzyScore('xyz', 'foobar')).toBe(0);
  });

  it('finds subsequence matches across word boundaries', () => {
    expect(fuzzyScore('bsi', 'by_service_id')).toBeGreaterThan(0);
    expect(fuzzyScore('bsi', 'by_host')).toBe(0);
  });

  it('is case-insensitive', () => {
    expect(fuzzyScore('FOO', 'foo')).toBe(1000);
    expect(fuzzyScore('foo', 'FOOBAR')).toBe(500);
  });
});

describe('fuzzyFilter', () => {
  it('returns all options when query is empty', () => {
    expect(fuzzyFilter('', ['a', 'b', 'c'])).toEqual(['a', 'b', 'c']);
  });

  it('drops options that do not match', () => {
    const out = fuzzyFilter('bsi', ['by_host', 'by_service_id', 'by_path']);
    expect(out).toContain('by_service_id');
    expect(out).not.toContain('by_host');
    expect(out).not.toContain('by_path');
  });

  it('ranks exact match first', () => {
    const out = fuzzyFilter('host', ['by_hostname', 'by_host', 'by_other_host']);
    expect(out[0]).toBe('by_host');
  });

  it('breaks ties by alphabetical order', () => {
    const out = fuzzyFilter('foo', ['foo_b', 'foo_a', 'foo_c']);
    expect(out).toEqual(['foo_a', 'foo_b', 'foo_c']);
  });
});
