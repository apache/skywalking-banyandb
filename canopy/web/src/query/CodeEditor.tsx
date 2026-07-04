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

// CodeEditor.tsx — textarea + line-number gutter + BydbQL syntax highlight.
// Ported from .handoff-import/banyandb/project/code.jsx. Verbatim shape: a
// transparent <textarea> layered over a highlighted <pre> with synced scroll,
// regex JSON / BydbQL tokenizer, lang pill + hints.

import React, { useMemo, useRef } from 'react';

interface CodeEditorProps {
  readonly value: string;
  readonly onChange: (v: string) => void;
  readonly language?: string;
  readonly hint?: string;
  readonly readOnly?: boolean;
  readonly ariaLabel?: string;
}

/** Tokenize BydbQL: keywords, strings, numbers, identifiers. */
function tokenize(src: string): string {
  // Escape first so highlights don't escape out of <pre>.
  const esc = src
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  // Keywords
  const kwRe = /\b(MEASURE|STREAM|TRACE|TOPN|TOP|SELECT|FROM|IN|WHERE|GROUP BY|ORDER BY|LIMIT|AGGREGATE BY|WITH QUERY_TRACE|TIME|NOW)\b/g;
  let out = esc.replace(kwRe, '<span class="ce-kw">$1</span>');
  // Strings
  out = out.replace(/'([^'\\]*(?:\\.[^'\\]*)*)'/g, "<span class=\"ce-str\">'$1'</span>");
  // Numbers (avoid those already inside kw / str spans — best-effort)
  out = out.replace(/(^|[\s(,=])(-?\d+(?:\.\d+)?)(?=$|[\s,)])/g, '$1<span class="ce-num">$2</span>');
  // Single-line comments
  out = out.replace(/(^|\s)(#[^\n]*)/g, '$1<span class="ce-cm">$2</span>');
  return out;
}

export function CodeEditor({ value, onChange, language = 'BydbQL', hint, readOnly, ariaLabel }: CodeEditorProps) {
  const taRef = useRef<HTMLTextAreaElement | null>(null);
  const gutterRef = useRef<HTMLDivElement | null>(null);
  const preRef = useRef<HTMLPreElement | null>(null);
  const lines = useMemo(() => (value ?? '').split('\n').length, [value]);
  const highlighted = useMemo(() => tokenize(value ?? '') + '\n', [value]);
  const syncScroll = (e: React.UIEvent<HTMLTextAreaElement>) => {
    const ta = e.currentTarget;
    if (gutterRef.current) gutterRef.current.scrollTop = ta.scrollTop;
    if (preRef.current) preRef.current.scrollTop = ta.scrollTop;
  };
  return (
    <div className="editor-card" aria-label={ariaLabel ?? 'BydbQL editor'}>
      <div className="editor-card-head">
        <span className="editor-pill">{language}</span>
        {hint && <span className="editor-hint">{hint}</span>}
      </div>
      <div className="editor-card-body">
        <div className="editor-gutter" ref={gutterRef} aria-hidden="true">
          {Array.from({ length: lines }, (_, i) => (
            <div key={i} className="editor-line-no">{i + 1}</div>
          ))}
        </div>
        <pre className="editor-pre" ref={preRef} aria-hidden="true">
          <code dangerouslySetInnerHTML={{ __html: highlighted }} />
        </pre>
        <textarea
          ref={taRef}
          className="editor-ta"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onScroll={syncScroll}
          spellCheck={false}
          readOnly={readOnly}
          aria-label={ariaLabel ?? 'BydbQL editor'}
        />
      </div>
    </div>
  );
}