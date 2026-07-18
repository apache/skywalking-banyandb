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

// CodeArea.tsx — lightweight syntax-highlight code editor for a single tag
// value (JSON / long text). Ported from .handoff-import/banyandb/project/code.jsx's
// CodeArea (window-global JSX -> ES module TSX). Used inline in
// PropertyForms.tsx's tag editor rows, behind the "<>" code toggle. Not to be
// confused with CodeEditor.tsx, which renders the full BydbQL query editor
// chrome (gutter + toolbar) — this is a smaller, per-value widget.
//
// No external deps: a self-contained JSON tokenizer + a highlight layer
// rendered behind a transparent textarea, mirroring `.ca` / `.ca-bar` /
// `.ca-edit` / `.ca-pre` / `.ca-ta` in canopy.css (already ported from
// "Property Document Styles.html").

import React, { useEffect, useRef } from 'react';
import { startsLikeJSON, looksLikeJSON, tryFormat } from './property-util.js';

const IconCode = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="m8 6-6 6 6 6M16 6l6 6-6 6" />
  </svg>
);
const IconFormat = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 6h16M4 12h10M4 18h16" />
  </svg>
);

function escapeHtml(s: string): string {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/** Produce highlighted HTML for a string. lang: 'json' | 'text'. */
function highlightCode(src: string, lang: string): string {
  if (src == null || src === '') return '';
  if (lang !== 'json') return escapeHtml(src);
  const esc = escapeHtml(src);
  const re = /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  return esc.replace(re, (m, str, colon, bool, nul, num) => {
    if (str !== undefined) {
      if (colon) return '<span class="cj-key">' + str + '</span>' + colon;
      return '<span class="cj-str">' + str + '</span>';
    }
    if (bool) return '<span class="cj-bool">' + bool + '</span>';
    if (nul) return '<span class="cj-null">' + nul + '</span>';
    if (num) return '<span class="cj-num">' + num + '</span>';
    return m;
  });
}

// guard so a trailing newline still renders a final line in the <pre>
function withTail(s: string): string {
  return s.length && s[s.length - 1] === '\n' ? s + '​' : s;
}

interface CodeAreaProps {
  readonly value: string;
  readonly onChange: (v: string) => void;
  readonly placeholder?: string;
  readonly minHeight?: number;
}

/** Editable, highlighted code area — used for JSON / long-text tag values. */
export function CodeArea({ value, onChange, placeholder, minHeight }: CodeAreaProps) {
  const taRef = useRef<HTMLTextAreaElement | null>(null);
  const preRef = useRef<HTMLPreElement | null>(null);
  const v = value || '';
  const json = startsLikeJSON(v);
  const lang = json ? 'json' : 'text';
  const valid = !json || looksLikeJSON(v);

  const sync = () => {
    const ta = taRef.current;
    const pre = preRef.current;
    if (ta && pre) {
      pre.scrollTop = ta.scrollTop;
      pre.scrollLeft = ta.scrollLeft;
    }
  };
  useEffect(() => { sync(); }, [v]);

  const html = highlightCode(withTail(v), lang);

  return (
    <div className="ca">
      <div className="ca-bar">
        <span className={'ca-lang' + (json && !valid ? ' is-bad' : '')}>
          <IconCode width={12} height={12} /> {json ? 'JSON' : 'text'}
          {json && (valid ? <span className="ca-ok">valid</span> : <span className="ca-err">invalid</span>)}
        </span>
        <div className="ca-tools">
          {json && (
            <button
              type="button"
              className="ca-tool"
              disabled={!valid}
              onClick={() => onChange(tryFormat(v))}
              title="Pretty-print JSON"
            >
              <IconFormat width={13} height={13} /> Format
            </button>
          )}
        </div>
      </div>
      <div className="ca-edit" style={minHeight ? { minHeight } : undefined}>
        {/* Self-contained tokenizer output (escapeHtml-ed) — no user HTML injection risk. */}
        <pre className="ca-pre" ref={preRef} aria-hidden="true"><code dangerouslySetInnerHTML={{ __html: html }} /></pre>
        <textarea
          className="ca-ta mono"
          ref={taRef}
          value={v}
          spellCheck={false}
          placeholder={placeholder ?? 'Enter value — JSON is highlighted automatically'}
          onChange={(e) => onChange(e.target.value)}
          onScroll={sync}
        />
      </div>
    </div>
  );
}
