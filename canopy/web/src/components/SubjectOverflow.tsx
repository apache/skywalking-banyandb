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

import React, { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';

import { IconArrowRight, IconAlert } from './icons.js';

export interface SubjectItem {
  name: string;
  /** false when the subject resource has been deleted (orphan binding). */
  exists: boolean;
  /** Optional status tone — drives the chip color. */
  status?: string;
}

const VISIBLE_LIMIT = 3;

/**
 * SubjectOverflow — renders the first N subject chips inline and tucks the
 * remainder behind a `+N more` button that opens a small popover. Used by the
 * Index page's Rule tab to keep rows compact when a rule has many bindings.
 */
export function SubjectOverflow({
  subjects, basePath,
}: {
  subjects: SubjectItem[];
  /** Resource base path; each chip navigates to `${basePath}/${name}`. */
  basePath: string;
}) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLSpanElement>(null);
  const popRef = useRef<HTMLDivElement>(null);

  // Close on outside click / Escape.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current?.contains(e.target as Node)) return;
      if (popRef.current?.contains(e.target as Node)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  if (subjects.length === 0) return null;

  const visible = subjects.slice(0, VISIBLE_LIMIT);
  const overflow = subjects.slice(VISIBLE_LIMIT);

  const navigate = (name: string) => {
    window.location.href = `${basePath}/${name}`;
  };

  return (
    <>
      {visible.map((s) => (
        s.exists ? (
          <button key={s.name}
            className={'subj-chip is-' + (s.status ?? 'ok')}
            title={`Open ${s.name}`}
            onClick={() => navigate(s.name)}>
            {s.name}
          </button>
        ) : (
          <span key={s.name} className="subj-chip is-danger" title="Subject no longer exists (orphan binding)">
            <IconAlert size={11} /> {s.name}
          </span>
        )
      ))}
      {overflow.length > 0 && (
        <span className="sf-wrap" ref={wrapRef}>
          <button
            type="button"
            className={'sf-btn' + (open ? ' is-on' : '')}
            onClick={() => setOpen((o) => !o)}
            title={`Show all ${subjects.length} bound subjects`}
          >
            +{overflow.length} more
          </button>
          {open && createPortal(
            <>
              <div className="sf-backdrop" onClick={() => setOpen(false)} />
              <div
                className="sfields-pop"
                ref={popRef}
                style={{ width: 320, left: wrapRef.current?.getBoundingClientRect().left, top: (wrapRef.current?.getBoundingClientRect().bottom ?? 0) + 4 }}
              >
                <div className="sfields-head">
                  <span>Bound subjects</span>
                  <span className="mono">{subjects.length}</span>
                </div>
                <div style={{ maxHeight: 280, overflowY: 'auto', padding: 4 }}>
                  {subjects.map((s) => (
                    <button
                      key={s.name}
                      type="button"
                      onClick={() => { setOpen(false); navigate(s.name); }}
                      className="sfields-row"
                      style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 10px', border: 'none', background: 'none', cursor: 'pointer', width: '100%', borderRadius: 6, color: 'inherit', textAlign: 'left' }}
                    >
                      {s.exists ? (
                        <span className={'subj-chip is-' + (s.status ?? 'ok')} style={{ fontSize: 10.5 }}>
                          {s.name}
                        </span>
                      ) : (
                        <span className="subj-chip is-danger" style={{ fontSize: 10.5 }}>
                          <IconAlert size={10} /> {s.name}
                        </span>
                      )}
                      <span className="sf-name" style={{ marginLeft: 'auto' }}>
                        {s.exists ? 'open' : 'orphan'}
                      </span>
                      <span className="sf-arrow"><IconArrowRight size={12} /></span>
                    </button>
                  ))}
                </div>
              </div>
            </>,
            document.body,
          )}
        </span>
      )}
    </>
  );
}