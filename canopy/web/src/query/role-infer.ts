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

// role-infer.ts — port of srInferRole()/srRenderValue() from stream-results.jsx.
// BanyanDB stores only a storage TagType per tag (STRING / INT / STRING_ARRAY /
// INT_ARRAY / DATA_BINARY / TIMESTAMP). It carries no presentation intent — a
// log level and a log body are BOTH strings. So we never key rendering on the
// tag's name; we resolve a display role in FOUR LAYERS, each overriding the
// last:
//   1. type       — what the schema guarantees
//   2. value      — cardinality + shape of the values
//   3. convention — well-known fields w/ semantic color
//   4. user       — an explicit override from the Fields panel
//
// Tag-name-based hints are ONLY layer 3; layers 1, 2, and 4 are tag-name-agnostic.

export type SR_TAG_TYPE =
  | 'TAG_TYPE_STRING'
  | 'TAG_TYPE_INT'
  | 'TAG_TYPE_INT64'
  | 'TAG_TYPE_STRING_ARRAY'
  | 'TAG_TYPE_INT_ARRAY'
  | 'TAG_TYPE_DATA_BINARY'
  | 'TAG_TYPE_TIMESTAMP';

export type SR_ROLE =
  | 'cat'        // categorical / badge; semantic color for known fields, hashed palette otherwise
  | 'body'       // log body / message; preformatted text
  | 'id'         // entity / trace id; mono truncated
  | 'numeric'    // generic number; plain
  | 'time'       // timestamp
  | 'list'       // string/int array
  | 'binary'     // DATA_BINARY; hex/base64 inspector
  | 'text';      // default plain text

export type SR_OVERRIDES = ReadonlyMap<string, SR_ROLE> | Readonly<Record<string, SR_ROLE>>;

/** Valid display roles (excluding the "auto" pseudo-option). */
export const SR_ROLES: readonly SR_ROLE[] = ['cat', 'body', 'id', 'numeric', 'time', 'list', 'binary', 'text'];

/** Layer 3 — name-convention roles + semantic color maps. Strip this and layers 1–2 still render. */
interface Convention {
  readonly role: SR_ROLE;
  readonly label: string;
  readonly colors?: Record<string, string>;
  readonly http?: boolean;
}

const SR_SEVERITY: Record<string, string> = {
  ERROR: 'var(--danger)',
  FATAL: 'var(--danger)',
  WARN: 'var(--warn)',
  WARNING: 'var(--warn)',
  INFO: 'var(--accent)',
  DEBUG: 'var(--text-dim)',
  TRACE: 'var(--text-faint)',
};

const SR_LAYER: Record<string, string> = {
  Http: '#3d82f6',
  RPCFramework: '#9b8cf0',
  Database: '#d98a3c',
  Cache: '#3cc8b4',
  MQ: '#d8b13c',
  FAAS: '#56c2d6',
  Unknown: 'var(--text-dim)',
};

const SR_CONVENTIONS: Record<string, Convention> = {
  level: { role: 'cat', colors: SR_SEVERITY, label: 'severity' },
  log_level: { role: 'cat', colors: SR_SEVERITY, label: 'severity' },
  loglevel: { role: 'cat', colors: SR_SEVERITY, label: 'severity' },
  'log.level': { role: 'cat', colors: SR_SEVERITY, label: 'severity' },
  severity: { role: 'cat', colors: SR_SEVERITY, label: 'severity' },
  status_code: { role: 'cat', http: true, label: 'http status' },
  http_status_code: { role: 'cat', http: true, label: 'http status' },
  'http.status_code': { role: 'cat', http: true, label: 'http status' },
  span_layer: { role: 'cat', colors: SR_LAYER, label: 'span layer' },
  trace_id: { role: 'id', label: 'trace id' },
  span_id: { role: 'id', label: 'span id' },
  parent_span_id: { role: 'id', label: 'parent span id' },
  endpoint: { role: 'body', label: 'endpoint' },
  operation_name: { role: 'body', label: 'operation' },
  endpoint_name: { role: 'body', label: 'endpoint' },
};

// Convention keys preserve the user's tag-name punctuation: 'service_name'
// and 'serviceName' both look up `service_name` after a case-fold. We do NOT
// strip underscores / dots because real tag names in the wild use them, and
// stripping collapses distinct fields (e.g. 'service_name' and 'servicename'
// are different concepts).
const normalize = (s: string): string => s.toLowerCase();

/** Look up the convention metadata for a tag name (used for semantic colors). */
export const srConventionFor = (name: string): Convention | null => {
  return SR_CONVENTIONS[normalize(name)] ?? null;
};

/** Layer 1 — render a role from the storage type alone. */
export const srRoleFromType = (type: SR_TAG_TYPE | string): SR_ROLE => {
  switch (type) {
    case 'TAG_TYPE_DATA_BINARY':
      return 'binary';
    case 'TAG_TYPE_STRING_ARRAY':
    case 'TAG_TYPE_INT_ARRAY':
      return 'list';
    case 'TAG_TYPE_TIMESTAMP':
      return 'time';
    case 'TAG_TYPE_INT':
    case 'TAG_TYPE_INT64':
      return 'numeric';
    case 'TAG_TYPE_STRING':
    default:
      return 'text';
  }
};

const HEXLIKE_RE = /^(?:[0-9a-f]{8,}|0x[0-9a-f]+)$/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Per-tag cardinality stats used by the value layer and the Fields panel. */
export interface SR_COL_STATS {
  readonly distinct: number;
  readonly total: number;
  readonly min?: number;
  readonly max?: number;
  readonly avgLen?: number;
  readonly maxLen?: number;
  readonly idLike?: boolean;
}

export const srColStats = (sample: readonly unknown[]): SR_COL_STATS => {
  if (sample.length === 0) return { distinct: 0, total: 0 };
  const strs = sample.map((v) => (v == null ? '' : String(v)));
  const n = strs.length;
  const set = new Set(strs);
  const distinct = set.size;
  const lens = strs.map((s) => s.length);
  const avgLen = lens.reduce((a, b) => a + b, 0) / n;
  const maxLen = Math.max(...lens);
  const hexish = strs.every((s) => s === '' || HEXLIKE_RE.test(s) || UUID_RE.test(s));
  const idLike = distinct >= n * 0.8 && hexish && maxLen >= 8;
  const numeric: number[] = [];
  for (const v of sample) {
    const s = String(v);
    if (/^-?\d+(\.\d+)?$/.test(s)) numeric.push(Number(s));
  }
  if (numeric.length > 0) {
    return { distinct, total: n, min: Math.min(...numeric), max: Math.max(...numeric), avgLen, maxLen, idLike };
  }
  return { distinct, total: n, avgLen, maxLen, idLike };
};

/** Layer 2 — refine a role from value cardinality + shape. */
export const srRoleFromValue = (
  type: SR_TAG_TYPE | string,
  sample: readonly unknown[],
  hint: SR_ROLE,
): SR_ROLE => {
  if (type === 'TAG_TYPE_DATA_BINARY') return 'binary';
  if (type === 'TAG_TYPE_STRING_ARRAY' || type === 'TAG_TYPE_INT_ARRAY') return 'list';
  if (type === 'TAG_TYPE_TIMESTAMP') return 'time';
  if (sample.length === 0) return hint;
  const stats = srColStats(sample);
  switch (type) {
    case 'TAG_TYPE_INT':
    case 'TAG_TYPE_INT64': {
      // A tiny closed set of ints reads as a category, not a measurement.
      if (stats.distinct <= 6 && stats.total >= stats.distinct * 2) return 'cat';
      return 'numeric';
    }
    case 'TAG_TYPE_STRING':
    default: {
      if (stats.idLike) return 'id';
      if (stats.distinct <= 8 && (stats.avgLen ?? 0) <= 24 && stats.total >= stats.distinct * 1.5) return 'cat';
      if ((stats.avgLen ?? 0) >= 36 || (stats.maxLen ?? 0) >= 60) return 'body';
      return 'text';
    }
  }
};

/** Layer 3 — name-convention overlay (SkyWalking-flavoured). */
export const srRoleFromConvention = (name: string): SR_ROLE | null => {
  return srConventionFor(name)?.role ?? null;
};

/** Layer 4 — apply a user override (from the Fields panel). */
export const srRoleFromOverride = (name: string, overrides: SR_OVERRIDES): SR_ROLE | null => {
  if (overrides instanceof Map) return overrides.get(name) ?? null;
  if (Object.prototype.hasOwnProperty.call(overrides, name)) {
    return (overrides as Record<string, SR_ROLE>)[name];
  }
  return null;
};

/** The full 4-layer ladder. Returns the resolved role + the layer that resolved it. */
export const srInferRole = (
  name: string,
  type: SR_TAG_TYPE | string,
  sample: readonly unknown[],
  overrides: SR_OVERRIDES,
): { readonly role: SR_ROLE; readonly layer: 1 | 2 | 3 | 4 } => {
  // Layer 4 — user override wins outright.
  const ov = srRoleFromOverride(name, overrides);
  if (ov) return { role: ov, layer: 4 };
  // Layer 3 — convention.
  const cn = srRoleFromConvention(name);
  if (cn) return { role: cn, layer: 3 };
  // Layer 2 — value shape refines layer-1 type role.
  const t1 = srRoleFromType(type);
  const t2 = srRoleFromValue(type, sample, t1);
  return { role: t2, layer: t1 === t2 ? 1 : 2 };
};

/** Layer 1 + 3 + 4 inference for schema that has no sample values yet. */
export const srInferRoleFromSchema = (
  name: string,
  type: SR_TAG_TYPE | string,
  overrides: SR_OVERRIDES,
): { readonly role: SR_ROLE; readonly layer: 1 | 3 | 4 } => {
  const ov = srRoleFromOverride(name, overrides);
  if (ov) return { role: ov, layer: 4 };
  const cn = srRoleFromConvention(name);
  if (cn) return { role: cn, layer: 3 };
  return { role: srRoleFromType(type), layer: 1 };
};

/** Layer number → short source label used in the tag-rendering popover. */
export const srLayerLabel = (layer: 1 | 2 | 3 | 4): 'TYPE' | 'AUTO' | 'CONV' | 'SET' => {
  switch (layer) {
    case 1:
      return 'TYPE';
    case 2:
      return 'AUTO';
    case 3:
      return 'CONV';
    case 4:
      return 'SET';
  }
};

/** All "shown as" override dropdown options (including the "auto" sentinel). */
export const SR_ROLE_OPTIONS: readonly string[] = ['auto', 'text', 'cat', 'numeric', 'id', 'body', 'time', 'list', 'binary'];

/** True if a string is one of the display roles. */
export const isValidRole = (r: string): r is SR_ROLE => (SR_ROLES as readonly string[]).includes(r);

/** HTTP status code → semantic CSS var name (matches the handoff's srHttpColor). */
export const srHttpColor = (v: unknown): string => {
  const n = typeof v === 'number' ? v : parseInt(String(v), 10);
  if (!Number.isFinite(n)) return 'var(--text-dim)';
  if (n >= 500) return 'var(--danger)';
  if (n >= 400) return 'var(--warn)';
  if (n >= 200 && n < 300) return 'var(--accent)';
  return 'var(--text-dim)';
};

/** Severity string → semantic CSS var name. */
export const srSeverityColor = (v: unknown): string => {
  const k = String(v ?? '').toUpperCase();
  return SR_SEVERITY[k] ?? 'var(--text)';
};

// Hashed palette for categorical values with no convention.
const SR_CHIP_PALETTE = ['#9b8cf0', '#3d82f6', '#3cc8b4', '#d98a3c', '#56c2d6', '#e0707a', '#7fc296', '#c9a14a'];

function srHash(s: string): number {
  let h = 2166136261;
  s = String(s);
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

/** Color for a categorical value: convention map → http class → hashed palette. */
export const srCatColor = (name: string, value: unknown): string => {
  const conv = srConventionFor(name);
  const s = String(value ?? '');
  if (conv?.colors && conv.colors[s] != null) return conv.colors[s];
  if (conv?.http) return srHttpColor(value);
  return SR_CHIP_PALETTE[srHash(name + '::' + s) % SR_CHIP_PALETTE.length];
};

/** Format a value for display given its resolved role.
 *  The returned `color` is an optional CSS color the caller can apply to the
 *  pill so categorical roles render with the handoff's accent palette. */
export const srRenderValue = (
  role: SR_ROLE,
  value: unknown,
  name?: string,
): { readonly display: string; readonly title?: string; readonly color?: string } => {
  switch (role) {
    case 'binary':
      return { display: 'binary', title: 'DATA_BINARY — open inspector to view bytes' };
    case 'list':
      return { display: Array.isArray(value) ? value.join(', ') : String(value ?? '') };
    case 'id': {
      const s = String(value ?? '');
      const head = s.slice(0, 7);
      const tail = s.length > 9 ? s.slice(-4) : '';
      return { display: tail ? `${head}…${tail}` : s, title: s };
    }
    case 'time': {
      if (value == null || value === '') return { display: '' };
      const t = typeof value === 'number' ? value : Date.parse(String(value));
      if (!Number.isFinite(t)) return { display: String(value) };
      const d = new Date(t);
      return { display: d.toISOString(), title: d.toUTCString() };
    }
    case 'numeric': {
      const n = typeof value === 'number' ? value : Number(String(value ?? ''));
      if (!Number.isFinite(n)) return { display: String(value ?? '') };
      if (name && /dur|latency|elapsed|_ms$|time_ms/i.test(name)) {
        return { display: n < 1000 ? `${n} ms` : `${(n / 1000).toFixed(2)} s` };
      }
      return { display: n.toLocaleString('en-US') };
    }
    case 'cat':
      return { display: value == null ? '' : String(value), color: name ? srCatColor(name, value) : undefined };
    case 'body':
    case 'text':
    default:
      return { display: value == null ? '' : String(value) };
  }
};
