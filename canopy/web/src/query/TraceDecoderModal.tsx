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

// TraceDecoderModal.tsx — proto binding + hex fallback for trace span bytes.
// Ported from .handoff-import/banyandb/project/trace-decoder.jsx. Lets the
// user upload a .proto, parses it with tdParseProto, stores the binding in
// localStorage keyed by traceId, and renders the bound preview OR the raw
// hex dump when unbound.

import React, { useEffect, useState } from 'react';
import { tdParseProto, tdDecode, tdGetBinding, tdSetBinding, tdClearBinding, tdHexDump, tdPickMessage, type TDBinding } from './proto-decoder.js';

interface Props {
  readonly traceId: string;
  readonly bytes: Uint8Array;
  readonly onClose: () => void;
}

export function TraceDecoderModal({ traceId, bytes, onClose }: Props) {
  const [binding, setBinding] = useState<TDBinding | null>(() => tdGetBinding(traceId));

  useEffect(() => {
    setBinding(tdGetBinding(traceId));
  }, [traceId]);

  const decoded = tdDecode(bytes, binding, traceId);

  const onUpload = async (file: File) => {
    const src = await file.text();
    const msgs = tdParseProto(src);
    if (msgs.length === 0) {
      alert('No message definitions found in this .proto file.');
      return;
    }
    const next = tdSetBinding(traceId, src, msgs);
    setBinding(next);
  };

  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="tdm-title" onClick={onClose}>
      <div className="modal tdm-modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2 id="tdm-title" className="modal-title">Trace decoder — {traceId || '(no trace id)'}</h2>
          <button type="button" className="qb-btn qb-btn-ghost" onClick={onClose}>Close</button>
        </header>

        <div className="tdm-body">
          <section className="tdm-bind">
            <div className="tdm-bind-head">
              <span>Proto binding</span>
              {binding ? (
                <button type="button" className="qb-btn qb-btn-ghost" onClick={() => { tdClearBinding(traceId); setBinding(null); }}>
                  Unbind
                </button>
              ) : (
                <label className="qb-btn qb-btn-ghost">
                  Upload .proto
                  <input
                    type="file"
                    accept=".proto,.txt"
                    hidden
                    onChange={(e) => {
                      const f = e.target.files?.[0];
                      if (f) onUpload(f);
                      e.target.value = '';
                    }}
                  />
                </label>
              )}
            </div>
            {binding && (
              <div className="tdm-bind-meta">
                Bound {new Date(binding.boundAt).toLocaleString()} · {binding.messages.length} message(s):
                <ul>
                  {binding.messages.map((m) => <li key={m.name}>{m.name} ({m.fields.length} fields)</li>)}
                </ul>
              </div>
            )}
          </section>

          <section className="tdm-decoded">
            <div className="tdm-decoded-head">
              <span>{binding ? `Decoded preview (${decoded.kind})` : 'Hex fallback (unbound)'}</span>
              <span className="tdm-bytes">{bytes.length} bytes</span>
            </div>
            {decoded.kind === 'empty' ? (
              <pre className="tdm-empty">(no bytes)</pre>
            ) : decoded.kind === 'unbound' ? (
              <pre className="tdm-hex">
                {tdHexDump(bytes).map((r, i) => <div key={i}><span className="rv-off">{r.off}</span>  {r.hex}  <span className="rv-ascii">{r.ascii}</span></div>)}
              </pre>
            ) : (
              <>
                <pre className="tdm-preview">{JSON.stringify({
                  bytes: decoded.bytes,
                  ascii: decoded.ascii,
                  ints: decoded.ints,
                  messages: (decoded.messages ?? []).map((m) => ({
                    name: m.name,
                    fields: m.fields.map((f) => `${f.repeated ? 'repeated ' : ''}${f.type} ${f.name}`),
                  })),
                }, null, 2)}</pre>
                <p className="tdm-note">
                  Bound via {tdPickMessage(decoded.messages ?? [])?.name ?? '?'}. BanyanDB does not preserve proto wire metadata — the preview shows ASCII / int candidates alongside the schema; mark it as a <code>preview</code>, not a verbatim decode.
                </p>
              </>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}