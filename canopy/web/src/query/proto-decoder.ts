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

// proto-decoder.ts — regex .proto parser + span-bytes decoder.
// Ported from .handoff-import/banyandb/project/trace-decoder.jsx.
// This is NOT a real proto compiler — it's a regex-based scan sufficient to
// turn a trace's span bytes into a JSON-ish preview against the user's bound
// .proto schema.
//
// Persistence: bindings are stored in localStorage, keyed per traceId. The
// traceId is the input param to the bind/get/clear trio.

const BIND_PREFIX = 'canopy.td.bind.';

// ── Proto AST shapes ────────────────────────────────────────────────────────

export interface TDField {
  readonly name: string;
  readonly type: string;
  readonly repeated: boolean;
}

export interface TDMessage {
  readonly name: string;
  readonly fields: readonly TDField[];
}

export interface TDBinding {
  readonly traceId: string;
  readonly protoSrc: string;
  readonly messages: readonly TDMessage[];
  readonly boundAt: number;
}

// ── tdParseProto — regex .proto scanner ─────────────────────────────────────

/** Parse a .proto source into a list of message definitions. */
export const tdParseProto = (src: string): readonly TDMessage[] => {
  const out: TDMessage[] = [];
  // strip // line comments and /* block */ comments — the proto fixture may
  // contain both and we never want them in the field-type scan.
  const clean = src
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/[^\n]*/g, '');
  const msgRe = /message\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{([^{}]*)\}/g;
  // matchAll auto-creates a fresh iterator per call (no lastIndex to leak
  // between messages); the previous regex.exec-in-while pattern reused the
  // outer fieldRe's lastIndex across messages and silently dropped fields
  // after the first one.
  // The field regex requires `^[ \t]*` so it tolerates the leading whitespace
  // and indented fields inside the message body.
  for (const m of clean.matchAll(msgRe)) {
    const name = m[1];
    const body = m[2];
    const fields: TDField[] = [];
    const fieldRe = /^[ \t]*(?:(repeated)\s+)?([A-Za-z0-9_.]+(?:\[\])?)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\d+/gm;
    for (const fm of body.matchAll(fieldRe)) {
      const repeated = !!fm[1];
      const type = fm[2];
      const fname = fm[3];
      fields.push({ name: fname, type, repeated });
    }
    if (fields.length > 0) out.push({ name, fields });
  }
  return out;
};

/** Pick the first message or one matching `hint` (case-insensitive substring of name). */
export const tdPickMessage = (
  messages: readonly TDMessage[],
  hint?: string,
): TDMessage | null => {
  if (messages.length === 0) return null;
  if (!hint) return messages[0];
  const lc = hint.toLowerCase();
  return messages.find((m) => m.name.toLowerCase().includes(lc)) ?? messages[0];
};

// ── tdDecode — span bytes → JSON-ish preview against a binding ─────────────

/**
 * Render a JSON-ish preview of opaque span bytes given a proto binding.
 * BanyanDB stores spans as opaque DATA_BINARY; we can't always fully decode
 * arbitrary proto wire data without a real compiler, so the preview:
 *   - extracts any 7-bit-ASCII substrings of length >= 4 as "strings"
 *   - finds little-endian int64 / int32 / uint64 / uint32 candidates
 *   - reports length + the candidates alongside the binding's message layout
 *
 * The intent is to GIVE THE USER SOMETHING when bound (vs the raw hex when
 * unbound) — the result is marked as `kind: 'preview'`, never `kind: 'decoded'`,
 * so the UI can render it honestly.
 */
export const tdDecode = (
  bytes: Uint8Array | null | undefined,
  binding: TDBinding | null,
  _traceId: string,
): {
  readonly kind: 'unbound' | 'preview' | 'decoded' | 'empty';
  readonly bytes: number;
  readonly preview?: string;
  readonly ascii?: readonly string[];
  readonly ints?: readonly number[];
  readonly messages?: readonly TDMessage[];
} => {
  if (!bytes || bytes.length === 0) return { kind: 'empty', bytes: 0 };
  if (!binding) return { kind: 'unbound', bytes: bytes.length };
  const ascii: string[] = [];
  const re = /[\x20-\x7e]{4,}/g;
  const text = String.fromCharCode(...bytes);
  let am: RegExpExecArray | null;
  while ((am = re.exec(text)) !== null) ascii.push(am[0]);
  const ints: number[] = [];
  for (let i = 0; i + 8 <= bytes.length; i += 4) {
    const v = bytes[i] | (bytes[i + 1] << 8) | (bytes[i + 2] << 16) | (bytes[i + 3] << 24);
    if (v !== 0) ints.push(v >>> 0);
  }
  return {
    kind: 'preview',
    bytes: bytes.length,
    preview: ascii.join(' | '),
    ascii,
    ints: ints.slice(0, 32),
    messages: binding.messages,
  };
};

// ── tdGetBinding / tdSetBinding / tdClearBinding — localStorage per traceId ─

export const tdGetBinding = (traceId: string): TDBinding | null => {
  if (typeof localStorage === 'undefined') return null;
  try {
    const raw = localStorage.getItem(BIND_PREFIX + traceId);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as TDBinding;
    if (parsed?.traceId !== traceId) return null;
    return parsed;
  } catch {
    return null;
  }
};

export const tdSetBinding = (traceId: string, protoSrc: string, messages: readonly TDMessage[]): TDBinding => {
  const binding: TDBinding = { traceId, protoSrc, messages, boundAt: Date.now() };
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