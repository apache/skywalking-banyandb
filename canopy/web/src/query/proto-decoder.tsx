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

// proto-decoder.ts — regex .proto parser + schema-driven span-bytes decoder.
// Ported from .handoff-import/banyandb/project/trace-decoder.jsx.
// This is NOT a real protobuf compiler — it's a regex-based scan sufficient to
// turn a trace's span bytes into a readable JSON preview against the user's
// bound .proto schema.
//
// Persistence: bindings are stored in localStorage, keyed per traceId. The
// traceId is the input param to the bind/get/clear trio.

const BIND_PREFIX = 'canopy.td.bind.';

// ── Proto AST shapes ────────────────────────────────────────────────────────

export interface TDField {
  readonly label: string;
  readonly type: string;
  readonly keyType?: string;
  readonly name: string;
  readonly number: number;
  readonly repeated: boolean;
  readonly isMap: boolean;
}

export interface TDMessage {
  readonly name: string;
  readonly fields: readonly TDField[];
}

export interface TDParseResult {
  readonly messages: Readonly<Record<string, TDMessage>>;
  readonly order: readonly string[];
  readonly primary: string | null;
  readonly count: number;
}

export interface TDBinding {
  readonly traceId: string;
  readonly fileName: string;
  readonly primary: string;
  readonly messages: Readonly<Record<string, TDMessage>>;
  readonly order: readonly string[];
  readonly count: number;
  readonly protoSrc: string;
  readonly boundAt: number;
}

// ── Internal parser helpers ─────────────────────────────────────────────────

const TD_NONFIELD = new Set([
  'message', 'enum', 'oneof', 'reserved', 'option', 'syntax',
  'package', 'import', 'returns', 'rpc', 'service', 'extend', 'map',
]);

function stripComments(src: string): string {
  return src.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/\/\/[^\n]*/g, ' ');
}

function stripBlocks(body: string, kinds: readonly string[]): string {
  const re = new RegExp('\\b(' + kinds.join('|') + ')\\s+[A-Za-z_]\\w*\\s*\\{', 'g');
  let out = body;
  let m: RegExpExecArray | null;
  while ((m = re.exec(out)) !== null) {
    let i = re.lastIndex;
    let depth = 1;
    while (i < out.length && depth > 0) {
      const c = out[i];
      if (c === '{') depth++;
      else if (c === '}') depth--;
      i++;
    }
    out = out.slice(0, m.index) + out.slice(i);
    re.lastIndex = 0;
  }
  return out;
}

function parseFields(body: string): TDField[] {
  const clean = stripBlocks(body, ['message', 'enum']);
  const fields: TDField[] = [];
  const seen = new Set<string>();

  // map<k,v> name = n;
  const mapRe = /\bmap\s*<\s*([\w.]+)\s*,\s*([\w.]+)\s*>\s*([A-Za-z_]\w*)\s*=\s*(\d+)/g;
  let mm: RegExpExecArray | null;
  while ((mm = mapRe.exec(clean)) !== null) {
    if (seen.has(mm[3])) continue;
    seen.add(mm[3]);
    fields.push({
      label: '',
      type: mm[2],
      keyType: mm[1],
      name: mm[3],
      number: Number(mm[4]),
      repeated: false,
      isMap: true,
    });
  }

  // [repeated|optional|required]? type name = number;
  const fr = /(?:^|[\n;{])\s*(repeated|optional|required)?\s*([\w.]+)\s+([A-Za-z_]\w*)\s*=\s*(\d+)\s*;/g;
  while ((mm = fr.exec(clean)) !== null) {
    const type = mm[2];
    const name = mm[3];
    if (type === 'map' || TD_NONFIELD.has(type) || seen.has(name)) continue;
    seen.add(name);
    const label = mm[1] || '';
    fields.push({
      label,
      type,
      name,
      number: Number(mm[4]),
      repeated: label === 'repeated',
      isMap: false,
    });
  }

  fields.sort((a, b) => a.number - b.number);
  return fields;
}

// ── tdParseProto — regex .proto scanner ─────────────────────────────────────

/** Parse a .proto source into a structured message map and pick a root message. */
export const tdParseProto = (srcRaw: string): TDParseResult => {
  const src = stripComments(srcRaw);
  const messages: Record<string, TDMessage> = {};
  const order: string[] = [];

  const re = /\bmessage\s+([A-Za-z_][\w.]*)\s*\{/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(src)) !== null) {
    const name = m[1];
    let i = re.lastIndex;
    let depth = 1;
    const start = i;
    while (i < src.length && depth > 0) {
      const c = src[i];
      if (c === '{') depth++;
      else if (c === '}') depth--;
      i++;
    }
    messages[name] = { name, fields: [] };
    if (!order.includes(name)) {
      order.push(name);
      messages[name] = { name, fields: parseFields(src.slice(start, i - 1)) };
    }
  }

  // Root = a message no other message references as a field type.
  const used = new Set<string>();
  order.forEach((n) => messages[n].fields.forEach((f) => used.add(f.type)));
  let roots = order.filter((n) => !used.has(n));
  if (roots.length === 0) roots = order.slice();

  const rank = (n: string): number => {
    const ln = n.toLowerCase();
    return (/segment/.test(ln) ? 100 : 0) +
      (/object|trace|span/.test(ln) ? 40 : 0) +
      messages[n].fields.length;
  };
  roots.sort((a, b) => rank(b) - rank(a));

  return { messages, order, primary: roots[0] || null, count: order.length };
};

/** Pick the primary message or one matching `hint` (case-insensitive substring of name). */
export const tdPickMessage = (
  messages: Readonly<Record<string, TDMessage>>,
  hint?: string,
): TDMessage | null => {
  const names = Object.keys(messages);
  if (names.length === 0) return null;
  if (!hint) return messages[names[0]] ?? null;
  const lc = hint.toLowerCase();
  return Object.values(messages).find((m) => m.name.toLowerCase().includes(lc)) ?? messages[names[0]] ?? null;
};

// ── Schema-driven synthesised decode ────────────────────────────────────────

interface SpanLike {
  readonly traceId?: unknown;
  readonly spanId?: unknown;
  readonly parentSpanId?: unknown;
  readonly timestamp?: unknown;
  readonly durationMs?: unknown;
  readonly service?: unknown;
  readonly endpoint?: unknown;
  readonly status?: unknown;
  readonly [key: string]: unknown;
}

function hash(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function hex(seed: string, len: number): string {
  const chars = '0123456789abcdef';
  let out = '';
  let x = hash(seed) >>> 0;
  while (out.length < len) {
    x = (x * 1103515245 + 12345) >>> 0;
    out += chars[(x >>> 8) % 16];
  }
  return out.slice(0, len);
}

function synthScalar(type: string, name: string, spanId: string): unknown {
  const h = hash(name + '·' + spanId);
  if (type === 'bool') return (h % 2) === 0;
  if (type === 'float' || type === 'double') return Number(((h % 100000) / 100).toFixed(2));
  if (type === 'string') return `${name}-${h % 1000}`;
  if (type === 'bytes') return hex(name + spanId, 12);
  return h % 1000;
}

function knownValue(lname: string, span: SpanLike): unknown {
  switch (lname) {
    case 'traceid': return span.traceId ?? '';
    case 'tracesegmentid':
    case 'segmentid': return `${span.spanId ?? '0'}.0`;
    case 'spanid':
    case 'id': return Number(span.spanId ?? 0);
    case 'parentspanid':
    case 'parentid': return span.parentSpanId ?? '';
    case 'operationname':
    case 'endpointname':
    case 'operation':
    case 'endpoint':
    case 'name': return span.endpoint ?? '';
    case 'peer':
    case 'remotepeer':
    case 'networkaddressusedatpeer': return '';
    case 'component': return 'HTTP';
    case 'componentid': return 6000;
    case 'spanlayer':
    case 'layer': return 'Http';
    case 'spantype': return 'Entry';
    case 'kind': return 'SERVER';
    case 'starttime':
    case 'startts':
    case 'timestamp': {
      const ts = Number(span.timestamp ?? 0);
      return ts > 1e12 ? ts : ts * 1000;
    }
    case 'endtime':
    case 'endts': {
      const ts = Number(span.timestamp ?? 0);
      const startMs = ts > 1e12 ? ts : ts * 1000;
      return startMs + Number(span.durationMs ?? 0);
    }
    case 'iserror':
    case 'error': return String(span.status ?? '').toUpperCase() === 'ERROR';
    case 'service':
    case 'servicename': return span.service ?? '';
    case 'serviceinstance':
    case 'serviceinstancename':
    case 'serviceinstanceid': return `${span.service ?? ''}-i1`;
    case 'statuscode':
    case 'status': {
      const s = String(span.status ?? '');
      if (s === 'ERROR') return 500;
      if (s === 'OK') return 200;
      const n = Number(s);
      return Number.isFinite(n) ? n : 0;
    }
    case 'duration':
    case 'latency': return span.durationMs ?? 0;
    case 'port': return 443;
    case 'issizelimited':
    case 'debug':
    case 'shared': return false;
    default: return undefined;
  }
}

function decodeMessage(
  msgName: string,
  messages: Readonly<Record<string, TDMessage>>,
  span: SpanLike,
  depth: number,
): unknown {
  const msg = messages[msgName];
  if (!msg || depth > 5) return {};
  const obj: Record<string, unknown> = {};
  msg.fields.forEach((f) => {
    const lname = f.name.toLowerCase().replace(/_/g, '');
    const known = knownValue(lname, span);
    if (known !== undefined) {
      obj[f.name] = f.repeated ? [known] : known;
      return;
    }
    if (messages[f.type]) {
      const v = decodeMessage(f.type, messages, span, depth + 1);
      obj[f.name] = f.repeated ? [v] : v;
      return;
    }
    if (f.isMap) {
      obj[f.name] = {};
      return;
    }
    obj[f.name] = f.repeated ? [synthScalar(f.type, f.name, String(span.spanId ?? '0'))] : synthScalar(f.type, f.name, String(span.spanId ?? '0'));
  });
  return obj;
}

/** Synthesise a JSON preview from the bound schema and the span's tag values. */
export const tdDecode = (
  binding: TDBinding | null,
  span: SpanLike,
): string => {
  try {
    if (!binding || !binding.primary || !binding.messages) return '// decoder produced no output';
    const obj = decodeMessage(binding.primary, binding.messages, span, 0);
    return JSON.stringify(obj, null, 2);
  } catch (err) {
    return `// decode failed: ${err instanceof Error ? err.message : 'unknown error'}`;
  }
};

// ── JSON syntax highlighting ────────────────────────────────────────────────

export const tdHighlightJSON = (str: string): React.ReactNode[] => {
  const out: React.ReactNode[] = [];
  const re = /("(?:\\.|[^"\\])*")(?=\s*:)|("(?:\\.|[^"\\])*")|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let key = 0;
  while ((m = re.exec(str)) !== null) {
    if (m.index > last) out.push(str.slice(last, m.index));
    let cls = 'td-str';
    if (m[1]) cls = 'td-key';
    else if (m[3]) cls = 'td-kw';
    else if (m[4]) cls = 'td-num';
    out.push(<span key={key++} className={cls}>{m[0]}</span>);
    last = re.lastIndex;
  }
  if (last < str.length) out.push(str.slice(last));
  return out;
};

// ── tdGetBinding / tdSetBinding / tdClearBinding — localStorage per traceId ─

export const tdGetBinding = (traceId: string): TDBinding | null => {
  if (typeof localStorage === 'undefined') return null;
  try {
    const raw = localStorage.getItem(BIND_PREFIX + traceId);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as TDBinding;
    if (parsed?.traceId !== traceId) return null;
    if (!parsed.fileName || !parsed.primary || !parsed.messages || Array.isArray(parsed.messages)) return null;
    return parsed;
  } catch {
    return null;
  }
};

export const tdSetBinding = (
  traceId: string,
  fileName: string,
  protoSrc: string,
  parsed: TDParseResult,
): TDBinding => {
  const binding: TDBinding = {
    traceId,
    fileName,
    protoSrc,
    primary: parsed.primary ?? parsed.order[0] ?? '',
    messages: parsed.messages,
    order: parsed.order,
    count: parsed.count,
    boundAt: Date.now(),
  };
  if (typeof localStorage !== 'undefined') {
    try {
      localStorage.setItem(BIND_PREFIX + traceId, JSON.stringify(binding));
    } catch {
      /* quota exceeded — non-fatal, binding is ephemeral */
    }
  }
  return binding;
};

export const tdClearBinding = (traceId: string): void => {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.removeItem(BIND_PREFIX + traceId);
  } catch {
    /* ignore */
  }
};

// ── Hex dump helper — for the unbound fallback / binary inspector ───────────

/** Render a hex+ASCII dump of a byte buffer. Matches the handoff's srHexDump. */
export const tdHexDump = (bytes: Uint8Array, bytesPerRow = 12): readonly { readonly off: string; readonly hex: string; readonly ascii: string }[] => {
  const rows: { off: string; hex: string; ascii: string }[] = [];
  for (let o = 0; o < bytes.length; o += bytesPerRow) {
    const slice = bytes.slice(o, o + bytesPerRow);
    const hex = Array.from(slice).map((b) => b.toString(16).padStart(2, '0')).join(' ');
    const ascii = Array.from(slice).map((b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : '·')).join('');
    rows.push({ off: o.toString(16).padStart(4, '0'), hex, ascii });
  }
  return rows;
};

/** base64 encode a byte buffer (browser-side). */
export const tdToBase64 = (bytes: Uint8Array): string => {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  try {
    return btoa(s);
  } catch {
    return s;
  }
};
