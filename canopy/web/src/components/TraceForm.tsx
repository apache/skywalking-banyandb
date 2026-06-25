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

import React, { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';

import type { TraceSchema, CreateTraceRequest, TagType } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconChevron } from './icons.js';

function RoleTagSelect({ label, value, onChange, options }: {
  label: string; value: string; onChange: (v: string) => void; options: string[];
}) {
  return (
    <label className="f-field">
      <span className="f-label">{label}</span>
      <div className="f-select-wrap">
        <select className="f-input f-select" value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">— select —</option>
          {options.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <span className="f-select-chev"><IconChevron size={13} /></span>
      </div>
    </label>
  );
}

const TAG_TYPES = [
  'TAG_TYPE_STRING',
  'TAG_TYPE_INT64',
  'TAG_TYPE_FLOAT64',
  'TAG_TYPE_STRING_ARRAY',
  'TAG_TYPE_INT64_ARRAY',
  'TAG_TYPE_DATA_BINARY',
] as const;

interface TagRow { name: string; type: string; }

/** TraceForm renders a create-trace modal or a delete-trace confirmation dialog. */
export function TraceForm({ mode, groupName, initialName, onClose }: {
  mode: 'create' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: TraceSchema) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [tags, setTags] = useState<TagRow[]>([{ name: '', type: 'TAG_TYPE_STRING' }]);
  const [traceIdTagName, setTraceIdTagName] = useState('');
  const [spanIdTagName, setSpanIdTagName] = useState('');
  const [timestampTagName, setTimestampTagName] = useState('');
  const [error, setError] = useState('');

  const createMut = useMutation({
    mutationFn: (req: CreateTraceRequest) => apiDataSource.createTrace(req),
    onSuccess: (trace) => {
      qc.invalidateQueries({ queryKey: ['resources', 'traces', groupName] });
      onClose(trace);
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteResource('traces', groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'traces', groupName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const allTagNames = tags.map((t) => t.name).filter(Boolean);

  function addTag() {
    setTags((prev) => [...prev, { name: '', type: 'TAG_TYPE_STRING' }]);
  }

  function removeTag(tagIdx: number) {
    setTags((prev) => prev.filter((_, i) => i !== tagIdx));
  }

  function updateTagName(tagIdx: number, value: string) {
    setTags((prev) => prev.map((tag, i) => i === tagIdx ? { ...tag, name: value } : tag));
  }

  function updateTagType(tagIdx: number, value: string) {
    setTags((prev) => prev.map((tag, i) => i === tagIdx ? { ...tag, type: value } : tag));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) { setError('Name is required.'); return; }
    if (tags.length === 0) { setError('At least one tag is required.'); return; }
    for (const tag of tags) {
      if (!tag.name.trim()) { setError('All tags must have a name.'); return; }
    }
    if (!traceIdTagName) { setError('Trace ID tag name is required.'); return; }
    if (!spanIdTagName) { setError('Span ID tag name is required.'); return; }
    if (!timestampTagName) { setError('Timestamp tag name is required.'); return; }
    setError('');

    createMut.mutate({
      trace: {
        metadata: { name: name.trim(), group: groupName },
        tags: tags.map((t) => ({ name: t.name, type: t.type as TagType })),
        traceIdTagName,
        spanIdTagName,
        timestampTagName,
      },
    });
  }

  const isPending = createMut.isPending || deleteMut.isPending;

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete trace</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p>This will permanently delete trace <span className="mono">{initialName}</span>.</p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">New trace</span>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          <div className="f-section">
            <label className="f-field">
              <span className="f-label">Name <span className="f-req">*</span></span>
              <input className="f-input mono" type="text" value={name}
                onChange={(e) => setName(e.target.value)} autoFocus />
            </label>
          </div>

          <div className="f-section">
            <span className="f-section-title">Tags <span className="f-req">*</span></span>
            <div className="fam-card">
              {tags.map((tag, tagIdx) => (
                <div className="spec-row" key={tagIdx}>
                  <div className="spec-cell">
                    <input className="f-input" type="text" placeholder="Tag name"
                      value={tag.name} onChange={(e) => updateTagName(tagIdx, e.target.value)} />
                  </div>
                  <div className="spec-cell">
                    <div className="f-select-wrap">
                      <select className="f-select" value={tag.type}
                        onChange={(e) => updateTagType(tagIdx, e.target.value)}>
                        {TAG_TYPES.map((tt) => <option key={tt} value={tt}>{tt}</option>)}
                      </select>
                      <span className="f-select-chev"><IconChevron /></span>
                    </div>
                  </div>
                  <button type="button" className="btn" onClick={() => removeTag(tagIdx)}>×</button>
                </div>
              ))}
              <button type="button" className="btn btn-ghost" onClick={addTag}>Add tag</button>
            </div>
          </div>

          <div className="f-section">
            <span className="f-section-title">Trace role tags <span className="f-req">*</span></span>
            <span className="f-section-desc">Select which tags serve as trace ID, span ID, and timestamp</span>
            <div className="f-grid" style={{ gridTemplateColumns: '1fr 1fr 1fr' }}>
              <RoleTagSelect label="Trace ID tag" value={traceIdTagName} onChange={setTraceIdTagName} options={allTagNames} />
              <RoleTagSelect label="Span ID tag" value={spanIdTagName} onChange={setSpanIdTagName} options={allTagNames} />
              <RoleTagSelect label="Timestamp tag" value={timestampTagName} onChange={setTimestampTagName} options={allTagNames} />
            </div>
          </div>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? 'Creating…' : 'Create'}
          </button>
        </div>
      </form>
    </div>
  );
}
