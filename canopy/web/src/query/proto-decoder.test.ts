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

import { describe, it, expect, beforeEach } from 'vitest';
import {
  tdParseProto, tdPickMessage, tdDecode, tdGetBinding, tdSetBinding, tdClearBinding,
  tdHexDump, tdToBase64,
} from './proto-decoder.js';

const ZIPKIN_PROTO = `
// zipkin_trace.proto — minimal span shape used as a decoder fixture.
syntax = "proto3";

package zipkin;

/* A single span in a trace. */
message Span {
  string trace_id = 1;
  string id = 2;
  string name = 3;
  uint64 timestamp = 4;
  uint64 duration = 5;
  repeated string tags = 6;
  // end of span
}

/* An annotation on a span. */
message Annotation {
  string trace_id = 1;
  string span_id = 2;
  int64 timestamp = 3;
  string value = 4;
}
`;

describe('tdParseProto', () => {
  it('extracts messages + fields, skipping comments', () => {
    const msgs = tdParseProto(ZIPKIN_PROTO);
    expect(msgs.map((m) => m.name)).toEqual(['Span', 'Annotation']);
    const span = msgs.find((m) => m.name === 'Span');
    expect(span?.fields.map((f) => f.name)).toEqual(['trace_id', 'id', 'name', 'timestamp', 'duration', 'tags']);
    const tags = span?.fields.find((f) => f.name === 'tags');
    expect(tags?.repeated).toBe(true);
    expect(tags?.type).toBe('string');
  });
  it('strips // line comments and /* block */ comments', () => {
    const src = `
      // top-level comment
      /* block comment */
      message M {
        // field comment
        string a = 1;
        /* another */
        int32 b = 2;
      }
    `;
    const msgs = tdParseProto(src);
    expect(msgs).toHaveLength(1);
    expect(msgs[0].fields.map((f) => f.name)).toEqual(['a', 'b']);
  });
  it('returns [] for a source with no messages', () => {
    expect(tdParseProto('syntax = "proto3";')).toEqual([]);
  });
});

describe('tdPickMessage', () => {
  it('returns the first message when no hint', () => {
    const msgs = tdParseProto(ZIPKIN_PROTO);
    expect(tdPickMessage(msgs)?.name).toBe('Span');
  });
  it('returns the message matching the hint (case-insensitive substring)', () => {
    const msgs = tdParseProto(ZIPKIN_PROTO);
    expect(tdPickMessage(msgs, 'annot')?.name).toBe('Annotation');
    expect(tdPickMessage(msgs, 'SPAN')?.name).toBe('Span');
  });
  it('returns null for empty messages', () => {
    expect(tdPickMessage([])).toBeNull();
  });
});

describe('tdDecode', () => {
  it('returns kind=empty for null/empty bytes', () => {
    expect(tdDecode(null, null, 't1').kind).toBe('empty');
    expect(tdDecode(new Uint8Array(0), null, 't1').kind).toBe('empty');
  });
  it('returns kind=unbound when no binding is supplied', () => {
    const r = tdDecode(new Uint8Array([1, 2, 3, 4]), null, 't1');
    expect(r.kind).toBe('unbound');
    expect(r.bytes).toBe(4);
  });
  it('returns kind=preview when bound — extracts ASCII + ints', () => {
    const bytes = new Uint8Array([0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x01, 0x02, 0x03, 0x04]);
    const binding = tdSetBinding('t1', ZIPKIN_PROTO, tdParseProto(ZIPKIN_PROTO));
    const r = tdDecode(bytes, binding, 't1');
    expect(r.kind).toBe('preview');
    expect(r.ascii).toContain('hello');
    expect(r.ints?.length).toBeGreaterThan(0);
    expect(r.messages?.map((m) => m.name)).toEqual(['Span', 'Annotation']);
  });
});

describe('localStorage binding lifecycle', () => {
  beforeEach(() => {
    tdClearBinding('t1');
    tdClearBinding('t2');
  });

  it('set → get round-trips a binding', () => {
    const msgs = tdParseProto(ZIPKIN_PROTO);
    tdSetBinding('t1', ZIPKIN_PROTO, msgs);
    const got = tdGetBinding('t1');
    expect(got?.traceId).toBe('t1');
    expect(got?.messages).toHaveLength(2);
    expect(got?.boundAt).toBeGreaterThan(0);
  });

  it('clear removes the binding', () => {
    tdSetBinding('t1', ZIPKIN_PROTO, tdParseProto(ZIPKIN_PROTO));
    tdClearBinding('t1');
    expect(tdGetBinding('t1')).toBeNull();
  });

  it('bindings are keyed per traceId', () => {
    tdSetBinding('t1', ZIPKIN_PROTO, tdParseProto(ZIPKIN_PROTO));
    tdSetBinding('t2', ZIPKIN_PROTO, tdParseProto(ZIPKIN_PROTO));
    expect(tdGetBinding('t1')?.traceId).toBe('t1');
    expect(tdGetBinding('t2')?.traceId).toBe('t2');
    tdClearBinding('t1');
    expect(tdGetBinding('t1')).toBeNull();
    expect(tdGetBinding('t2')?.traceId).toBe('t2');
  });

  it('getBinding returns null when storage is corrupted', () => {
    localStorage.setItem('canopy.td.bind.t1', 'not json');
    expect(tdGetBinding('t1')).toBeNull();
  });
});

describe('hex dump + base64', () => {
  it('tdHexDump renders offset + hex + ascii per row', () => {
    const rows = tdHexDump(new Uint8Array([0x68, 0x69, 0x0a, 0x21]), 4);
    expect(rows).toHaveLength(1);
    expect(rows[0].off).toBe('0000');
    expect(rows[0].hex).toBe('68 69 0a 21');
    expect(rows[0].ascii).toBe('hi·!');
  });
  it('tdToBase64 round-trips through atob', () => {
    const b = new Uint8Array([0x68, 0x69]);
    const s = tdToBase64(b);
    expect(s).toBe('aGk=');
  });
});