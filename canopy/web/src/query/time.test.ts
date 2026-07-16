/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) to you under
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
import { parseTs, parseIntervalMs, formatTs, tipRows } from './time.js';

describe('parseTs', () => {
  it('returns NaN for nullish input', () => {
    expect(parseTs(null)).toBeNaN();
    expect(parseTs(undefined)).toBeNaN();
    expect(parseTs('')).toBeNaN();
    expect(parseTs('   ')).toBeNaN();
  });

  it('returns NaN for non-time garbage', () => {
    expect(parseTs({ foo: 'bar' })).toBeNaN();
    expect(parseTs([])).toBeNaN();
    expect(parseTs('not a date')).toBeNaN();
  });

  it('treats large numbers (>1e12) as already-ms epoch', () => {
    expect(parseTs(1_700_000_000_000)).toBe(1_700_000_000_000);
  });

  it('treats small numbers (<1e12) as seconds, multiplies by 1000', () => {
    expect(parseTs(1_700_000_000)).toBe(1_700_000_000_000);
    expect(parseTs(0)).toBe(0);
  });

  it('parses ISO-8601 strings', () => {
    const iso = '2026-07-14T11:25:49.273Z';
    expect(parseTs(iso)).toBe(Date.parse(iso));
  });

  it('parses numeric strings with the same s/ms heuristic', () => {
    expect(parseTs('1700000000')).toBe(1_700_000_000_000); // string seconds
    expect(parseTs('1700000000000')).toBe(1_700_000_000_000); // string ms
  });

  it('returns NaN for Infinity', () => {
    expect(parseTs(Infinity)).toBeNaN();
    expect(parseTs(-Infinity)).toBeNaN();
  });
});

describe('parseIntervalMs', () => {
  it('returns undefined for missing or unrecognized input', () => {
    expect(parseIntervalMs(undefined)).toBeUndefined();
    expect(parseIntervalMs('')).toBeUndefined();
    expect(parseIntervalMs('weekly')).toBeUndefined();
    expect(parseIntervalMs('5x')).toBeUndefined();
  });

  it('parses common BanyanDB intervals', () => {
    expect(parseIntervalMs('500ms')).toBe(500);
    expect(parseIntervalMs('30s')).toBe(30_000);
    expect(parseIntervalMs('1m')).toBe(60_000);
    expect(parseIntervalMs('5m')).toBe(300_000);
    expect(parseIntervalMs('1h')).toBe(3_600_000);
    expect(parseIntervalMs('1d')).toBe(86_400_000);
  });

  it('handles microsecond and nanosecond units', () => {
    expect(parseIntervalMs('100µs')).toBeCloseTo(0.1, 5);
    expect(parseIntervalMs('500us')).toBeCloseTo(0.5, 5);
    expect(parseIntervalMs('1ns')).toBeCloseTo(1e-6, 9);
  });

  it('is case-insensitive on the unit', () => {
    expect(parseIntervalMs('1M')).toBe(60_000);
    expect(parseIntervalMs('1H')).toBe(3_600_000);
  });

  it('rejects zero / negative intervals', () => {
    expect(parseIntervalMs('0s')).toBeUndefined();
    expect(parseIntervalMs('-5m')).toBeUndefined();
  });
});

describe('formatTs', () => {
  const utc = new Date('2026-07-14T11:25:49.273Z').getTime();

  it('returns the em-dash sentinel for NaN', () => {
    expect(formatTs(NaN)).toBe('—');
  });

  it('honors a custom empty placeholder', () => {
    expect(formatTs(NaN, { empty: '' })).toBe('');
    expect(formatTs(NaN, { empty: 'n/a' })).toBe('n/a');
  });

  it('renders HH:MM:SS.mmm when no intervalMs is given (trace default)', () => {
    // Local formatting will reflect the test environment's timezone; verify
    // the format shape (length + fractional digits) instead of exact text.
    const out = formatTs(utc, { timezone: 'utc' });
    expect(out).toMatch(/^\d{2}:\d{2}:\d{2}\.\d{3}$/);
    expect(out).toBe('11:25:49.273');
  });

  it('renders UTC wall clock when timezone is "utc"', () => {
    expect(formatTs(utc, { timezone: 'utc' })).toBe('11:25:49.273');
  });

  it('interval-aware granularity: sub-second rolls show ms', () => {
    expect(formatTs(utc, { intervalMs: 100, timezone: 'utc' })).toBe('11:25:49.273');
  });

  it('interval-aware granularity: minute drops seconds (1m interval)', () => {
    // 60_000 ms = 1 minute → falls into "sub-hour" branch (drops seconds, keeps minute).
    expect(formatTs(utc, { intervalMs: 60_000, timezone: 'utc' })).toBe('11:25');
  });

  it('interval-aware granularity: sub-second keeps ms (1s interval)', () => {
    expect(formatTs(utc, { intervalMs: 1, timezone: 'utc' })).toBe('11:25:49.273');
  });

  it('interval-aware granularity: hour shows date+hour (1h interval)', () => {
    // 3_600_000 ms = 1 hour → falls into "sub-day" branch (date + truncated hour).
    expect(formatTs(utc, { intervalMs: 3_600_000, timezone: 'utc' })).toBe('2026-07-14 11:00');
  });

  it('interval-aware granularity: day shows date', () => {
    expect(formatTs(utc, { intervalMs: 86_400_000, timezone: 'utc' })).toBe('2026-07-14');
  });

  it('interval-aware granularity: between hour and day shows date+hour', () => {
    expect(formatTs(utc, { intervalMs: 7_200_000, timezone: 'utc' })).toBe('2026-07-14 11:00');
  });
});

describe('tipRows', () => {
  it('returns ISO + Epoch + Local in order', () => {
    const utc = new Date('2026-07-14T11:25:49.273Z').getTime();
    const rows = tipRows(utc);
    expect(rows.map((r) => r.label)).toEqual(['ISO', 'Epoch', 'Local']);
    expect(rows[0].value).toBe('2026-07-14T11:25:49.273Z');
    expect(Number(rows[1].value.replace(/,/g, ''))).toBe(utc);
  });

  it('returns no rows for NaN', () => {
    expect(tipRows(NaN)).toEqual([]);
  });
});
