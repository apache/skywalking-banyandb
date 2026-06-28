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

import React, { useState, useEffect } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import type { TraceSchema, CreateTraceRequest, UpdateTraceRequest, TagType } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconChevron, IconPlus, IconTrash } from './icons.js';
import { useFocusTrap } from './modal-utils.js';

function RoleTagSelect({ label, value, onChange, options }: {
  label: string; value: string; onChange: (v: string) => void; options: string[];
}) {
  // Wire the label to the select via htmlFor/id so getByLabelText works in
  // tests and screen readers can announce the field's purpose.
  const id = React.useId();
  return (
    <div className="f-field">
      <label className="f-label" htmlFor={id}>{label} <span className="f-req">*</span></label>
      <div className="f-select-wrap">
        <select id={id} className="f-input f-select mono" value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">— select —</option>
          {options.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <span className="f-select-chev"><IconChevron size={13} /></span>
      </div>
    </div>
  );
}

// Mirrors the handoff Field wrapper: label + required indicator + hint/error.
function Field({
  label, hint, error, required, locked, children,
}: {
  label: React.ReactNode;
  hint?: string;
  error?: string;
  required?: boolean;
  locked?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className={`f-field${error ? ' has-error' : ''}`}>
      <label className="f-label">
        {label}
        {required && <span className="f-req">*</span>}
        {locked && <span className="f-lock">read-only</span>}
      </label>
      {children}
      {error ? <div className="f-error">{error}</div> : hint ? <div className="f-hint">{hint}</div> : null}
    </div>
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

/** TraceForm renders a create/edit-trace modal or a delete-trace confirmation dialog. */
export function TraceForm({ mode, groupName, initialName, onClose, onDeleted }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: TraceSchema) => void;
  /** Fired after a confirmed delete — distinct from `onClose` so callers can
   * navigate away only when the resource was actually deleted, not when the
   * user cancelled via the X button or backdrop. */
  onDeleted?: () => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [tags, setTags] = useState<TagRow[]>([{ name: '', type: 'TAG_TYPE_STRING' }]);
  const [traceIdTagName, setTraceIdTagName] = useState('');
  const [spanIdTagName, setSpanIdTagName] = useState('');
  const [timestampTagName, setTimestampTagName] = useState('');
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['resource', 'traces', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getResource('traces', groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });
  const editSchema = editResource as TraceSchema | undefined;

  useEffect(() => {
    if (mode === 'edit' && editSchema && !initialized) {
      setTags((editSchema.tags ?? []).map((t) => ({ name: t.name, type: t.type as string })));
      setTraceIdTagName(editSchema.traceIdTagName ?? '');
      setSpanIdTagName(editSchema.spanIdTagName ?? '');
      setTimestampTagName(editSchema.timestampTagName ?? '');
      setInitialized(true);
    }
  }, [mode, editSchema, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateTraceRequest) => apiDataSource.createTrace(req),
    onSuccess: (trace) => {
      qc.invalidateQueries({ queryKey: ['resources', 'traces', groupName] });
      onClose(trace);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateTraceRequest) => apiDataSource.updateTrace(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'traces', groupName] });
      qc.invalidateQueries({ queryKey: ['resource', 'traces', groupName, initialName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteResource('traces', groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'traces', groupName] });
      onClose();
      onDeleted?.();
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
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    if (!submittedName) { setError('Name is required.'); return; }
    if (tags.length === 0) { setError('At least one tag is required.'); return; }
    for (const tag of tags) {
      if (!tag.name.trim()) { setError('All tags must have a name.'); return; }
      if (tag.name.includes('#')) { setError(`Tag name "${tag.name}" must not contain "#".`); return; }
    }
    if (!traceIdTagName) { setError('Trace ID tag name is required.'); return; }
    if (!spanIdTagName) { setError('Span ID tag name is required.'); return; }
    if (!timestampTagName) { setError('Timestamp tag name is required.'); return; }
    setError('');

    const tracePayload = {
      metadata: { name: submittedName, group: groupName },
      tags: tags.map((t) => ({ name: t.name, type: t.type as TagType })),
      traceIdTagName,
      spanIdTagName,
      timestampTagName,
    };

    if (mode === 'edit') { updateMut.mutate({ trace: tracePayload }); }
    else { createMut.mutate({ trace: tracePayload }); }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const isEdit = mode === 'edit';
  const trapRef = useFocusTrap(true, () => onClose());

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete trace</span>
            <button className="modal-x" onClick={() => onClose()} aria-label="Close" />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete the trace{' '}
              <b className="mono">{initialName}</b> from group{' '}
              <b className="mono">{groupName}</b>. All stored spans for this trace will be removed.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete trace'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" ref={trapRef} onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit trace' : 'Create trace'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the schema below — it is revalidated on save.'
                : `Define a trace in group “${groupName}”.`}
            </p>
          </div>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>

        <div className="modal-body">
          {/* Identity */}
          <section className="f-section">
            <div className="f-section-title">Identity</div>
            <div className="f-grid">
              <Field
                label="Name"
                required={!isEdit}
                locked={isEdit}
                hint={isEdit ? undefined : "Unique within the group · letters, digits, '_' and '-'"}
              >
                <input className="f-input mono" type="text" placeholder="segment_traces"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </Field>
              <Field label="Group" locked hint="Resources are scoped to their group">
                <input className="f-input mono" type="text" value={groupName} disabled />
              </Field>
            </div>
          </section>

          {/* Tags */}
          <section className="f-section">
            <div className="f-section-title">Tags <span className="f-req">*</span></div>
            <div className="spec-list">
              {tags.map((tag, tagIdx) => (
                <div className="spec-row" key={tagIdx}>
                  <div className="spec-cell grow">
                    <input className="f-input mono" type="text" placeholder="tag_name"
                      value={tag.name} onChange={(e) => updateTagName(tagIdx, e.target.value)} />
                  </div>
                  <div className="spec-cell type">
                    <div className="f-select-wrap">
                      <select className="f-input f-select mono" value={tag.type}
                        onChange={(e) => updateTagType(tagIdx, e.target.value)}>
                        {TAG_TYPES.map((tt) => <option key={tt} value={tt}>{tt}</option>)}
                      </select>
                      <span className="f-select-chev"><IconChevron /></span>
                    </div>
                  </div>
                  <button type="button" className="spec-del" title="Remove tag"
                    onClick={() => removeTag(tagIdx)} disabled={tags.length === 1}
                    aria-label="Remove tag">
                    <IconTrash size={14} />
                  </button>
                </div>
              ))}
              <button type="button" className="spec-add" onClick={addTag}>
                <IconPlus size={14} /> Add tag
              </button>
            </div>
          </section>

          {/* Reserved tag mapping */}
          <section className="f-section">
            <div className="f-section-title">Reserved tag mapping <span className="f-req">*</span></div>
            <p className="f-section-desc">Map the trace ID, span ID and timestamp to defined tags. These cannot be deleted later.</p>
            <div className="f-grid">
              <RoleTagSelect label="Trace ID tag" value={traceIdTagName} onChange={setTraceIdTagName} options={allTagNames} />
              <RoleTagSelect label="Span ID tag" value={spanIdTagName} onChange={setSpanIdTagName} options={allTagNames} />
              <RoleTagSelect label="Timestamp tag" value={timestampTagName} onChange={setTimestampTagName} options={allTagNames} />
            </div>
          </section>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create trace')}
          </button>
        </div>
      </form>
    </div>
  );
}
