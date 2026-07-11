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

// role-infer.ts — port of srTagRole() from stream-results.jsx.
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
  | 'severity'      // log level, error/warn/info; semantic color
  | 'http_status'   // HTTP status code; semantic color
  | 'service'       // service identifier; pill
  | 'id'            // entity / trace id; mono
  | 'duration_ms'   // milliseconds duration; number badge
  | 'duration_ns'   // nanoseconds duration; number badge
  | 'time'          // timestamp; relative time
  | 'body'          // log body / message; preformatted text
  | 'binary'        // DATA_BINARY; hex/base64 inspector
  | 'array'         // string/int array; comma list
  | 'number'        // generic number; plain
  | 'text';         // default; plain text

export type SR_OVERRIDES = ReadonlyMap<string, SR_ROLE> | Readonly<Record<string, SR_ROLE>>;

/** Layer 3 — name-convention roles. Strip this and layers 1–2 still render.
 * Keys preserve underscores/dots so 'service_name' (a real tag name) and
 * 'service.name' (dotted naming) match the user's input directly. */
const SR_CONVENTIONS: Record<string, SR_ROLE> = {
  // severity
  level: 'severity',
  severity: 'severity',
  loglevel: 'severity',
  log_level: 'severity',
  'log.level': 'severity',
  // http_status
  status: 'http_status',
  status_code: 'http_status',
  statuscode: 'http_status',
  'status.code': 'http_status',
  http_status: 'http_status',
  'http.status': 'http_status',
  httpstatus: 'http_status',
  httpstatuscode: 'http_status',
  // service
  service: 'service',
  service_name: 'service',
  service_id: 'service',
  svc: 'service',
  'service.name': 'service',
  // id
  trace_id: 'id',
  traceid: 'id',
  span_id: 'id',
  spanid: 'id',
  endpoint_id: 'id',
  instance_id: 'id',
  user_id: 'id',
  userid: 'id',
  'user.id': 'id',
  'trace.id': 'id',
  'span.id': 'id',
  // duration_ms
  duration: 'duration_ms',
  duration_ms: 'duration_ms',
  durationms: 'duration_ms',
  latency: 'duration_ms',
  latency_ms: 'duration_ms',
  elapse: 'duration_ms',
  elapsed: 'duration_ms',
  // time
  timestamp: 'time',
  ts: 'time',
  time: 'time',
  '@timestamp': 'time',
  // body
  body: 'body',
  message: 'body',
  msg: 'body',
  log: 'body',
  log_message: 'body',
  endpoint: 'body',
  operation: 'body',
  operation_name: 'body',
};

// Convention keys preserve the user's tag-name punctuation: 'service_name'
// and 'serviceName' both look up `service_name` after a case-fold. We do NOT
// strip underscores / dots because real tag names in the wild use them, and
// stripping collapses distinct fields (e.g. 'service_name' and 'servicename'
// are different concepts).
const normalize = (s: string): string => s.toLowerCase();

/** Layer 1 — render a role from the storage type alone. */
export const srRoleFromType = (type: SR_TAG_TYPE | string): SR_ROLE => {
  switch (type) {
    case 'TAG_TYPE_DATA_BINARY':
      return 'binary';
    case 'TAG_TYPE_STRING_ARRAY':
    case 'TAG_TYPE_INT_ARRAY':
      return 'array';
    case 'TAG_TYPE_TIMESTAMP':
      return 'time';
    case 'TAG_TYPE_INT':
    case 'TAG_TYPE_INT64':
      return 'number';
    case 'TAG_TYPE_STRING':
    default:
      return 'text';
  }
};

const HEXLIKE_RE = /^(?:[0-9a-f]{6,}|0x[0-9a-f]+)$/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function looksLikeId(v: unknown): boolean {
  const s = String(v ?? '');
  return HEXLIKE_RE.test(s) || UUID_RE.test(s);
}

function looksLikeNumber(v: unknown): boolean {
  const s = String(v ?? '');
  return /^-?\d+(\.\d+)?$/.test(s);
}

/** Layer 2 — refine a role from value cardinality + shape. */
export const srRoleFromValue = (
  type: SR_TAG_TYPE | string,
  sample: readonly unknown[],
  hint: SR_ROLE,
): SR_ROLE => {
  if (type === 'TAG_TYPE_DATA_BINARY') return 'binary';
  if (type === 'TAG_TYPE_STRING_ARRAY' || type === 'TAG_TYPE_INT_ARRAY') return 'array';
  if (type === 'TAG_TYPE_TIMESTAMP') return 'time';
  if (sample.length === 0) return hint;
  // For strings, use value shape to decide between number / id / body / text.
  if (type === 'TAG_TYPE_STRING') {
    let totalLen = 0;
    let numberLikeCount = 0;
    let idLikeCount = 0;
    for (const v of sample) {
      const s = String(v);
      totalLen += s.length;
      if (looksLikeNumber(v)) numberLikeCount++;
      else if (looksLikeId(v)) idLikeCount++;
    }
    // A string column that stores numeric codes (e.g. status_code) renders as a number.
    if (numberLikeCount === sample.length) return 'number';
    // Long strings are bodies, even when cardinality is low.
    if (totalLen / sample.length > 40) return 'body';
    // Hex / UUID / snowflake identifiers render as id.
    if (idLikeCount >= sample.length / 2) return 'id';
  }
  return hint;
};

/** Layer 3 — name-convention overlay (SkyWalking-flavoured). */
export const srRoleFromConvention = (name: string): SR_ROLE | null => {
  const k = normalize(name);
  return SR_CONVENTIONS[k] ?? null;
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
  // Layer 3 — convention
  const cn = srRoleFromConvention(name);
  if (cn) return { role: cn, layer: 3 };
  // Layer 2 — value shape refines layer-1 type role
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

/** Per-tag cardinality stats used by the Fields panel. */
export interface SR_COL_STATS {
  readonly distinct: number;
  readonly total: number;
  readonly min?: number;
  readonly max?: number;
  readonly avgLen?: number;
}

export const srColStats = (sample: readonly unknown[]): SR_COL_STATS => {
  if (sample.length === 0) return { distinct: 0, total: 0 };
  const distinct = new Set(sample.map((v) => String(v))).size;
  const numeric: number[] = [];
  let totalLen = 0;
  for (const v of sample) {
    const s = String(v);
    totalLen += s.length;
    if (/^-?\d+(\.\d+)?$/.test(s)) numeric.push(Number(s));
  }
  if (numeric.length > 0) {
    return {
      distinct,
      total: sample.length,
      min: Math.min(...numeric),
      max: Math.max(...numeric),
      avgLen: totalLen / sample.length,
    };
  }
  return {
    distinct,
    total: sample.length,
    avgLen: totalLen / sample.length,
  };
};

// ── Semantic color helpers used by the result views ─────────────────────────

/** Layer number → short source label used in the tag-rendering popover. */
export const srLayerLabel = (layer: 1 | 2 | 3 | 4): 'TYPE' | 'AUTO' | 'CONV' | 'SET' => {
  switch (layer) {
    case 1: return 'TYPE';
    case 2: return 'AUTO';
    case 3: return 'CONV';
    case 4: return 'SET';
  }
};

/** All renderable roles exposed in the "shown as" override dropdown. */
export const SR_ROLE_OPTIONS: readonly SR_ROLE[] = [
  'severity', 'http_status', 'service', 'id', 'duration_ms', 'duration_ns',
  'time', 'body', 'binary', 'array', 'number', 'text',
];

/** HTTP status code → semantic CSS var name (matches the handoff's srHttpColor). */
export const srHttpColor = (v: unknown): string => {
  const n = typeof v === 'number' ? v : parseInt(String(v), 10);
  if (!Number.isFinite(n)) return 'var(--text-dim)';
  if (n >= 500) return 'var(--danger)';
  if (n >= 400) return 'var(--warn)';
  if (n >= 200 && n < 300) return 'var(--accent)';
  return 'var(--text-dim)';
};

const SR_SEVERITY: Record<string, string> = {
  ERROR: 'var(--danger)',
  FATAL: 'var(--danger)',
  CRITICAL: 'var(--danger)',
  WARN: 'var(--warn)',
  WARNING: 'var(--warn)',
  INFO: 'var(--accent)',
  DEBUG: 'var(--text-dim)',
  TRACE: 'var(--text-dim)',
};

/** Severity string → semantic CSS var name. */
export const srSeverityColor = (v: unknown): string => {
  const k = String(v ?? '').toUpperCase();
  return SR_SEVERITY[k] ?? 'var(--text)';
};

// Service-color palette: soft, distinguishable hues that stay readable on the
// dark panel background. Colors are assigned deterministically by hashing the
// service name so the same service always gets the same pill color.
const SR_SERVICE_COLORS = [
  '#3d82f6', '#3cc8b4', '#9b8cf0', '#d98a3c', '#e0707a',
  '#56c2d6', '#7fc296', '#c77dbf', '#d8b13c', '#6b9fff',
];

/** Service identifier → stable accent color from the handoff palette. */
export const srServiceColor = (v: unknown): string => {
  const s = String(v ?? '');
  if (!s) return 'var(--text-dim)';
  let hash = 0;
  for (let i = 0; i < s.length; i++) {
    hash = (hash * 31 + s.charCodeAt(i)) >>> 0;
  }
  return SR_SERVICE_COLORS[hash % SR_SERVICE_COLORS.length];
};

/** Format a value for display given its resolved role.
 *  The returned `color` is an optional CSS color the caller can apply to the
 *  pill / text so semantic roles (severity, http_status, service, id) render
 *  with the handoff's accent palette. */
export const srRenderValue = (
  role: SR_ROLE,
  value: unknown,
): { readonly display: string; readonly title?: string; readonly color?: string } => {
  switch (role) {
    case 'binary':
      return { display: 'binary', title: 'DATA_BINARY — open inspector to view bytes' };
    case 'array':
      return { display: Array.isArray(value) ? value.join(', ') : String(value) };
    case 'service':
      return { display: value == null ? '' : String(value), color: srServiceColor(value) };
    case 'id':
      return { display: value == null ? '' : String(value), color: '#6b9fff' };
    case 'time': {
      const t = typeof value === 'number' ? value : Date.parse(String(value));
      if (!Number.isFinite(t)) return { display: String(value) };
      const d = new Date(t);
      return { display: d.toISOString(), title: d.toUTCString() };
    }
    case 'duration_ms':
    case 'duration_ns': {
      const n = typeof value === 'number' ? value : Number(String(value));
      if (!Number.isFinite(n)) return { display: String(value) };
      if (role === 'duration_ns') {
        return { display: n < 1000 ? `${n} ns` : `${(n / 1_000_000).toFixed(2)} ms` };
      }
      return { display: n < 1000 ? `${n} ms` : `${(n / 1000).toFixed(2)} s` };
    }
    default:
      return { display: value == null ? '' : String(value) };
  }
};