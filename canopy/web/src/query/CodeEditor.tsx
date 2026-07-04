/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

// CodeEditor.tsx — textarea + line-number gutter.
//
// Markup mirrors the handoff's `editor-card` / `editor-toolbar` / `tb-left` /
// `tb-right` / `lang-pill` / `tb-hint` / `editor-area` / `gutter` / `gutter-num`
// / `code-input` class names so the styles in canopy.css (copied verbatim from
// the handoff) apply unchanged. Optional `toolbarRight` slot lets the caller
// drop a button (e.g. "← Builder") into the top-right of the toolbar.
//
// The handoff has no syntax highlight — it is a plain textarea. We previously
// carried a regex-highlight <pre> layered behind the textarea per decision #10
// in implement-m4-note.md, but it caused a double-rendered side-by-side look
// when the handoff's flex layout placed both as siblings; dropped in favor of
// the verbatim handoff rendering.

import React, { useMemo, useRef } from 'react';

interface CodeEditorProps {
  readonly value: string;
  readonly onChange: (v: string) => void;
  readonly language?: string;
  readonly hint?: string;
  readonly readOnly?: boolean;
  readonly ariaLabel?: string;
  /** Right-aligned node in the toolbar (e.g. a "← Builder" back button). */
  readonly toolbarRight?: React.ReactNode;
}

export function CodeEditor({ value, onChange, language = 'BydbQL', hint, readOnly, ariaLabel, toolbarRight }: CodeEditorProps) {
  const taRef = useRef<HTMLTextAreaElement | null>(null);
  const gutterRef = useRef<HTMLDivElement | null>(null);
  const lines = useMemo(() => (value ?? '').split('\n').length, [value]);
  const syncScroll = (e: React.UIEvent<HTMLTextAreaElement>) => {
    const ta = e.currentTarget;
    if (gutterRef.current) gutterRef.current.scrollTop = ta.scrollTop;
  };
  return (
    <div className="editor-card" aria-label={ariaLabel ?? 'BydbQL editor'}>
      <div className="editor-toolbar">
        <div className="tb-left">
          <span className="lang-pill">{language}</span>
          {hint && <span className="tb-hint">{hint}</span>}
        </div>
        {toolbarRight && <div className="tb-right">{toolbarRight}</div>}
      </div>
      <div className="editor-area">
        <div className="gutter" ref={gutterRef} aria-hidden="true">
          {Array.from({ length: lines }, (_, i) => (
            <div key={i} className="gutter-num">{i + 1}</div>
          ))}
        </div>
        <textarea
          ref={taRef}
          className="code-input"
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