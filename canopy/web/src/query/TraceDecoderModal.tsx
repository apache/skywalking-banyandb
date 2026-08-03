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

// TraceDecoderModal.tsx — proto binding for trace span bytes.
// Matches the handoff "Span bytes decoder" modal: drop/select a .proto file,
// parse it locally, preview the root message's fields, then bind it.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { tdParseProto, tdGetBinding, tdSetBinding, tdClearBinding, type TDBinding, type TDField, type TDParseResult } from './proto-decoder.js';

interface Props {
  readonly traceId: string;
  readonly onClose: () => void;
  readonly onChange?: (binding: TDBinding | null) => void;
}

interface ParsedFile {
  readonly name: string;
  readonly src: string;
  readonly parsed: TDParseResult;
}

export function TraceDecoderModal({ traceId, onClose, onChange }: Props) {
  const existing = tdGetBinding(traceId);
  const [pending, setPending] = useState<ParsedFile | null>(null);
  const [error, setError] = useState<string>('');
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setPending(null);
    setError('');
  }, [traceId]);

  const parseFile = useCallback(async (file: File) => {
    if (!/\.proto$/i.test(file.name)) {
      setError('Please choose a .proto file.');
      setPending(null);
      return;
    }
    const src = await file.text();
    const parsed = tdParseProto(src);
    if (!parsed.primary) {
      setError('No message definitions found in this .proto file.');
      setPending(null);
      return;
    }
    setError('');
    setPending({ name: file.name, src, parsed });
  }, []);

  const handleDrop = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) parseFile(file);
  }, [parseFile]);

  const handleDragOver = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
  }, []);

  const bind = useCallback(() => {
    if (!pending) return;
    const next = tdSetBinding(traceId, pending.name, pending.src, pending.parsed);
    onChange?.(next);
    onClose();
  }, [pending, traceId, onChange, onClose]);

  const unbind = useCallback(() => {
    tdClearBinding(traceId);
    setPending(null);
    onChange?.(null);
    onClose();
  }, [traceId, onChange, onClose]);

  const pendingBinding = pending ? {
    traceId,
    fileName: pending.name,
    primary: pending.parsed.primary,
    messages: pending.parsed.messages,
    order: pending.parsed.order,
    count: pending.parsed.count,
    protoSrc: pending.src,
    boundAt: Date.now(),
  } : null;
  const active = pendingBinding ?? existing;
  const activePrimary = active?.primary ?? '';
  const activeCount = active?.count ?? 0;
  const activeName = active?.fileName ?? '';

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="tdm-title" onClick={onClose}>
      <div className="modal is-wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <h2 id="tdm-title" className="modal-title">Span bytes decoder</h2>
            <p className="modal-sub">Bind a protobuf schema to {traceId || 'this trace'} to decode each span&apos;s opaque bytes.</p>
          </div>
          <button type="button" className="modal-x" onClick={onClose} aria-label="Close" />
        </div>

        <div className="modal-body">
          <div className="td-modal">
            <div
              className={'tdm-drop' + (dragOver ? ' is-dragover' : '') + (active ? ' has-file' : '')}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onClick={() => inputRef.current?.click()}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                inputRef.current?.click();
              }
            }}
            role="button"
            tabIndex={0}
            aria-label="Upload a .proto file"
          >
            <input
              ref={inputRef}
              type="file"
              accept=".proto,.txt"
              hidden
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) parseFile(file);
                e.target.value = '';
              }}
            />
            <span className="tdm-drop-icon">{'{ }'}</span>
            <span className="tdm-drop-text">Drop a .proto file here, or click to browse</span>
            <span className="tdm-drop-sub">The schema is parsed locally — the root message drives the decode.</span>
          </div>

          {error && <div className="td-err">{error}</div>}

          {active && (
            <div className="td-summary">
              <div className="td-summary-top">
                <span className="td-file"><IconCheck width={13} height={13} /> {activeName}</span>
                <span className="td-meta mono">{activeCount} message{activeCount === 1 ? '' : 's'} · root <span className="td-root">{activePrimary}</span></span>
              </div>
              <TDFieldList message={active.messages[activePrimary]} />
            </div>
          )}

          <p className="td-note">
            Decoding is schema-driven: span bytes are rendered through the root message&apos;s
            fields. Output is shown as auto-text — recognised JSON is highlighted. With no decoder bound,
            the inspector shows the raw opaque bytes.
          </p>
          </div>
        </div>

        <div className="modal-foot">
          {existing && (
            <button type="button" className="qb-btn qb-btn-ghost td-remove" onClick={unbind}>
              <IconTrash width={14} height={14} /> Remove decoder
            </button>
          )}
          <div style={{ flex: 1 }} />
          <button type="button" className="qb-btn qb-btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button type="button" className="qb-btn qb-btn-primary" onClick={bind} disabled={!pending}>
            {existing ? 'Replace decoder' : 'Bind decoder'}
          </button>
        </div>
      </div>
    </div>
  );
}

function TDFieldList({ message }: { message: { readonly name: string; readonly fields: readonly TDField[] } | undefined }) {
  if (!message) return null;
  return (
    <div className="td-fields">
      <div className="td-fields-h mono">{message.name.toUpperCase()} · {message.fields.length} fields</div>
      <div className="td-fields-grid">
        {message.fields.slice(0, 18).map((f) => (
          <div key={f.name} className="td-field mono">
            <span className="td-field-type">{f.repeated ? 'repeated ' : ''}{f.isMap ? `map<${f.keyType},${f.type}>` : f.type}</span>
            <span className="td-field-name">{f.name}</span>
            <span className="td-field-num">= {f.number}</span>
          </div>
        ))}
        {message.fields.length > 18 && <div className="td-field-more">+ {message.fields.length - 18} more</div>}
      </div>
    </div>
  );
}

const IconCheck = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M20 6L9 17l-5-5" />
  </svg>
);

const IconTrash = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
  </svg>
);
