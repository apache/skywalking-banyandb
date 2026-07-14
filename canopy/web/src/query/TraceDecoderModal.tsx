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
// parse it locally, then bind it. The inspector itself already shows decoded
// bytes when a binding exists; this modal is only responsible for picking and
// persisting the schema.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { tdParseProto, tdGetBinding, tdSetBinding, tdClearBinding, tdPickMessage, type TDBinding, type TDMessage } from './proto-decoder.js';

interface Props {
  readonly traceId: string;
  readonly onClose: () => void;
}

interface ParsedFile {
  readonly name: string;
  readonly src: string;
  readonly messages: readonly TDMessage[];
}

export function TraceDecoderModal({ traceId, onClose }: Props) {
  const [binding, setBinding] = useState<TDBinding | null>(() => tdGetBinding(traceId));
  const [pending, setPending] = useState<ParsedFile | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setBinding(tdGetBinding(traceId));
  }, [traceId]);

  const parseFile = useCallback(async (file: File) => {
    const src = await file.text();
    const messages = tdParseProto(src);
    if (messages.length === 0) {
      alert('No message definitions found in this .proto file.');
      return;
    }
    setPending({ name: file.name, src, messages });
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
    const next = tdSetBinding(traceId, pending.src, pending.messages);
    setBinding(next);
    onClose();
  }, [pending, traceId, onClose]);

  const unbind = useCallback(() => {
    tdClearBinding(traceId);
    setBinding(null);
    setPending(null);
  }, [traceId]);

  const rootMessage = pending ? tdPickMessage(pending.messages)?.name : binding ? tdPickMessage(binding.messages)?.name : null;

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
          <div
            className={'tdm-drop' + (dragOver ? ' is-dragover' : '') + (pending || binding ? ' has-file' : '')}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onClick={() => inputRef.current?.click()}
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
            <span className="tdm-drop-text">
              {pending ? pending.name : binding ? `${tdPickMessage(binding.messages)?.name ?? 'bound'} · bound` : 'Drop a .proto file here, or click to browse'}
            </span>
            <span className="tdm-drop-sub">
              {pending
                ? `${pending.messages.length} message(s) · root: ${rootMessage ?? 'none'}`
                : binding
                  ? `Bound ${new Date(binding.boundAt).toLocaleString()} · click to replace`
                  : 'The schema is parsed locally — the root message drives the decode.'}
            </span>
          </div>

          <p className="tdm-note">
            Decoding is schema-driven: span bytes are rendered through the root message&apos;s fields.
            Output is shown as auto-text — recognised JSON is highlighted. With no decoder bound,
            the inspector shows the raw opaque bytes.
          </p>
        </div>

        <div className="modal-foot">
          {binding && (
            <button type="button" className="qb-btn qb-btn-ghost" onClick={unbind}>
              Unbind
            </button>
          )}
          <div style={{ flex: 1 }} />
          <button type="button" className="qb-btn qb-btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button type="button" className="qb-btn qb-btn-primary" onClick={bind} disabled={!pending}>
            Bind decoder
          </button>
        </div>
      </div>
    </div>
  );
}
