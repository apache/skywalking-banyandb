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

import type React from 'react';
import type { IntervalRule } from 'canopy-shared';

import { IconMeasures, IconStreams, IconTraces, IconProperties } from '../components/icons.js';

export interface CatalogEntry {
  catalog: string;
  label: string;
  singular: string;
  plural: string;
}

export const CATALOG_MAP: Record<string, CatalogEntry> = {
  measures:   { catalog: 'CATALOG_MEASURE',  label: 'MEASURE',  singular: 'measure',  plural: 'measures' },
  streams:    { catalog: 'CATALOG_STREAM',   label: 'STREAM',   singular: 'stream',   plural: 'streams' },
  traces:     { catalog: 'CATALOG_TRACE',    label: 'TRACE',    singular: 'trace',    plural: 'traces' },
  properties: { catalog: 'CATALOG_PROPERTY', label: 'PROPERTY', singular: 'property', plural: 'properties' },
};

export const TYPE_TITLES: Record<string, string> = {
  measures:   'Measures',
  streams:    'Streams',
  traces:     'Traces',
  properties: 'Properties',
};

export const TYPE_ICONS: Record<string, React.ComponentType<{ size?: number }>> = {
  measures:   IconMeasures,
  streams:    IconStreams,
  traces:     IconTraces,
  properties: IconProperties,
};

/** Converts an IntervalRule to a human-readable string e.g. "1d", "24h". */
export function formatInterval(r: IntervalRule | undefined | null): string {
  if (!r) return '';
  const suffix = r.unit === 'UNIT_HOUR' ? 'h' : 'd';
  return `${r.num}${suffix}`;
}

/** Parses a user-typed interval string like "1d" or "24h" into an IntervalRule. */
export function parseInterval(s: string): IntervalRule | null {
  const m = /^(\d+)\s*([dDhH]?)$/.exec(s.trim());
  if (!m || !m[1]) return null;
  const num = parseInt(m[1], 10);
  if (num <= 0) return null;
  const unit = (m[2] ?? 'd').toLowerCase() === 'h' ? 'UNIT_HOUR' : 'UNIT_DAY';
  return { unit, num };
}

// ── Binding validity window helpers ─────────────────────────────────────────

/**
 * Sentinel for "never expires" — BanyanDB liaison accepts timestamps up to
 * 9999-12-31, so 2099 is a comfortable horizon without overflowing 32-bit
 * signed integers anywhere downstream.
 */
export const FAR_FUTURE: number = Date.parse('2099-12-31T00:00:00Z');

/** Format epoch ms as "YYYY-MM-DDTHH:MM" for `<input type="datetime-local">`. */
export function msToLocalInput(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '';
  // `<input type="datetime-local">` expresses LOCAL wall-clock time, and
  // localInputToMs parses it back as local. Shift by the timezone offset so
  // toISOString() yields the local components (not UTC) — otherwise the
  // display→parse round-trip drifts by the user's UTC offset.
  const d = new Date(ms);
  const local = new Date(d.getTime() - d.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

/** Parse a "YYYY-MM-DDTHH:MM" string back to epoch ms; returns null when empty/invalid. */
export function localInputToMs(s: string): number | null {
  if (!s) return null;
  // No timezone suffix → parsed as local time, matching msToLocalInput.
  const t = new Date(s).getTime();
  return Number.isNaN(t) ? null : t;
}

/** Round a number down to the start of the current minute. */
export function floorToMinute(ms: number): number {
  return Math.floor(ms / 60_000) * 60_000;
}

/**
 * Compute the lifecycle status of an IndexRuleBinding. The binding is "orphan"
 * when its subject no longer exists in the group; otherwise we compare the
 * (numeric-ms) validity window against the current wall-clock time.
 */
export type BindingStatus = 'active' | 'expired' | 'orphan' | 'pending';

export function bindingStatus(
  beginAt: number,
  expireAt: number,
  subjectExists: boolean,
  nowMs: number,
): BindingStatus {
  if (!subjectExists) return 'orphan';
  if (typeof beginAt === 'number' && beginAt > nowMs) return 'pending';
  if (typeof expireAt === 'number' && expireAt < nowMs) return 'expired';
  return 'active';
}
