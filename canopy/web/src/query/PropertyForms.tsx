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

// PropertyForms.tsx — Property CRUD for the schema-free group/name/id model.
// Ported from .handoff-import/banyandb/project/property-form.jsx
// (window-global JSX -> ES module TSX). A Property collection is a
// schema-free document container (group/name); documents are keyed by `id`
// and carry key-value Tags. Mirrors docs/concept/data-model.md (Properties)
// and property/v1 Apply/Delete + database/v1 PropertyRegistryService
// Create/Delete. Reuses the Field/modal-overlay pattern every other *Form.tsx
// in this directory already duplicates locally (GroupForm.tsx et al.).
//
// ADAPTATIONS FROM THE HANDOFF:
//  - PropertyCollectionModal.onSubmit -> createPropertySchema (registry Create).
//  - PropertyEntryModal.onSubmit -> applyPropertyDocument (Apply, STRATEGY_REPLACE
//    — the modal submits the full tag set, so removed tags must be dropped).
//  - DeletePropertyEntryModal.onConfirm -> deletePropertyDocument (Delete).
//  - Added DeletePropertyCollectionModal (type-to-confirm, like GroupForm's
//    delete) -> deletePropertySchema; the handoff didn't have this modal.
//  - Tag value types: string/int/int[]/string[]/binary/timestamp — see
//    property-util.ts's PROP_VALUE_TYPES doc comment for why "float" (listed
//    in docs/property-design.md §7) isn't offered: model.v1.TagValue has no
//    float variant.

import React, { useMemo, useRef, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';

import type { PropertyDocument, PropertyDocTag } from 'canopy-shared';
import { apiDataSource, encodePropertyTagValue } from '../data/api.js';
import { useFocusTrap } from '../components/modal-utils.js';
import { IconKey, IconChevron } from '../components/icons.js';
import { PROP_VALUE_TYPES, looksLikeJSON } from './property-util.js';
import { CodeArea } from './CodeArea.js';

const PROP_NAME_RE = /^[a-zA-Z0-9_-]+$/;
const PROP_ID_RE = /^[a-zA-Z0-9_.-]+$/;

const IconClose = (p: React.SVGProps<SVGSVGElement>) => (
  <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M6 6l12 12M18 6 6 18" />
  </svg>
);

// Mirrors the Field wrapper every *Form.tsx in this directory duplicates
// locally (GroupForm.tsx, MeasureForm.tsx, ...): label + required + hint/error.
function Field({ label, hint, error, required, locked, children }: {
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

/* ============ property collection (group/name container) ============ */

export interface PropertyCollectionModalProps {
  readonly groupName: string;
  readonly existingNames: ReadonlySet<string>;
  readonly onClose: (created?: { readonly name: string }) => void;
}

export function PropertyCollectionModal({ groupName, existingNames, onClose }: PropertyCollectionModalProps) {
  const qc = useQueryClient();
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);
  const trapRef = useFocusTrap(true, () => onClose());

  const validate = (): string | null => {
    const n = name.trim();
    if (!n) return 'Name is required';
    if (n.length > 255) return 'Must be 255 characters or fewer';
    if (!PROP_NAME_RE.test(n)) return "Only letters, digits, '_' and '-' are allowed";
    if (existingNames.has(n.toLowerCase())) return `A property named "${n}" already exists in this group`;
    return null;
  };

  const createMut = useMutation({
    mutationFn: () => apiDataSource.createPropertySchema({ property: { metadata: { name: name.trim(), group: groupName } } }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['resources', 'properties', groupName] });
      onClose({ name: name.trim() });
    },
    onError: (e: Error) => setError(e.message),
  });

  const handleSubmit = () => {
    const e = validate();
    setSubmitted(true);
    setError(e);
    if (e) return;
    createMut.mutate();
  };

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <div className="modal" role="dialog" aria-modal="true" aria-label="Create property" ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">Create property</span>
            <p className="modal-sub">Define a schema-free property collection in group &ldquo;{groupName}&rdquo;. Documents are added later by id.</p>
          </div>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>
        <div className="modal-body">
          <section className="f-section">
            <div className="f-section-title">Identity</div>
            <div className="f-grid">
              <Field label="Name" required error={submitted ? error ?? undefined : undefined} hint="Unique within the group · letters, digits, '_' and '-'">
                <input
                  className="f-input mono"
                  value={name}
                  autoFocus
                  placeholder="temp_data"
                  onChange={(e) => { setName(e.target.value); if (submitted) setError(validate()); }}
                />
              </Field>
              <Field label="Group" locked hint="Properties are scoped to their group">
                <input className="f-input mono" value={groupName} disabled />
              </Field>
            </div>
          </section>
          <div className="prop-note">
            <IconKey size={14} />
            <span>Schema-free — each document carries its own key-value tags, keyed by <b className="mono">{groupName}/{name.trim() || 'name'}/&lt;id&gt;</b>.</span>
          </div>
          {createMut.isError && <div className="f-error" role="alert">{createMut.error.message}</div>}
        </div>
        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={createMut.isPending}>Cancel</button>
          <button type="button" className="btn btn-primary" onClick={handleSubmit} disabled={createMut.isPending}>
            {createMut.isPending ? 'Creating…' : 'Create property'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ============ delete collection (type-to-confirm; removes all documents) ============ */

export interface DeletePropertyCollectionModalProps {
  readonly groupName: string;
  readonly propName: string;
  readonly onClose: () => void;
  readonly onDeleted: () => void;
}

export function DeletePropertyCollectionModal({ groupName, propName, onClose, onDeleted }: DeletePropertyCollectionModalProps) {
  const qc = useQueryClient();
  const [confirmText, setConfirmText] = useState('');
  const trapRef = useFocusTrap(true, () => onClose());

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deletePropertySchema(groupName, propName),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['resources', 'properties', groupName] });
      onClose();
      onDeleted();
    },
  });

  const match = confirmText === propName;

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <div className="modal is-danger" role="dialog" aria-modal="true" aria-label="Delete property" ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">Delete property</span>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>
        <div className="modal-body">
          <p className="del-warn">
            You are about to permanently delete the property collection <b className="mono">{groupName}/{propName}</b> and every document in it. This cannot be undone.
          </p>
          <div className="f-field" style={{ marginTop: 16 }}>
            <label className="f-label">Type <span className="mono">{propName}</span> to confirm</label>
            <input
              type="text"
              className="f-input mono"
              autoFocus
              value={confirmText}
              placeholder={propName}
              onChange={(e) => setConfirmText(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter' && match && !deleteMut.isPending) deleteMut.mutate(); }}
            />
          </div>
          {deleteMut.isError && <div className="f-error" style={{ marginTop: 8 }}>{deleteMut.error.message}</div>}
        </div>
        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={deleteMut.isPending}>Cancel</button>
          <button type="button" className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={deleteMut.isPending || !match}>
            {deleteMut.isPending ? 'Deleting…' : 'Delete property'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ============ validation for a property document ============ */

interface EntryDraft {
  readonly id: string;
  readonly tags: readonly (PropertyDocTag & { readonly _uid: number })[];
}
interface EntryErrors {
  id?: string;
  tagsEmpty?: string;
  tags?: Array<{ key?: string } | undefined>;
}

export function validateEntry(v: EntryDraft, ctx: { readonly mode: 'create' | 'edit'; readonly existingIds?: ReadonlySet<string> }): EntryErrors {
  const e: EntryErrors = {};
  if (ctx.mode === 'create') {
    const id = (v.id || '').trim();
    if (!id) e.id = 'ID is required';
    else if (id.length > 255) e.id = 'Must be 255 characters or fewer';
    else if (!PROP_ID_RE.test(id)) e.id = "Only letters, digits, '.', '_' and '-' are allowed";
    else if (ctx.existingIds?.has(id.toLowerCase())) e.id = `A document with id "${id}" already exists`;
  }
  if (!(v.tags || []).length) e.tagsEmpty = 'Add at least one tag';
  const rows: Array<{ key?: string } | undefined> = [];
  const keys = new Set<string>();
  (v.tags || []).forEach((t, i) => {
    const te: { key?: string } = {};
    const k = (t.key || '').trim();
    if (!k) te.key = 'Required';
    else if (k.indexOf('#') !== -1) te.key = 'Cannot contain "#"';
    else if (keys.has(k.toLowerCase())) te.key = 'Duplicate key';
    if (k && !te.key) keys.add(k.toLowerCase());
    if (Object.keys(te).length) rows[i] = te;
  });
  if (rows.length) e.tags = rows;
  return e;
}

function entryHasErrors(e: EntryErrors): boolean {
  if (e.id || e.tagsEmpty) return true;
  return !!e.tags?.some((x) => x && Object.keys(x).length);
}

/* ============ property document (id + key-value tags) ============ */

export interface PropertyEntryModalProps {
  readonly mode: 'create' | 'edit';
  readonly groupName: string;
  readonly propName: string;
  readonly entry?: PropertyDocument;
  readonly existingIds?: ReadonlySet<string>;
  readonly onClose: () => void;
  readonly onApplied: () => void;
}

export function PropertyEntryModal({ mode, groupName, propName, entry, existingIds, onClose, onApplied }: PropertyEntryModalProps) {
  const qc = useQueryClient();
  const isEdit = mode === 'edit';
  const uidSeq = useRef(0);
  const init = useMemo<EntryDraft>(() => {
    const base: EntryDraft = (isEdit && entry)
      ? { id: entry.id, tags: entry.tags.map((t) => ({ ...t, _uid: ++uidSeq.current })) }
      : { id: '', tags: [{ key: '', valueType: 'str', value: '', _uid: ++uidSeq.current }] };
    return base;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- snapshot on mount only
  }, []);
  const [v, setV] = useState<EntryDraft>(init);
  const [errors, setErrors] = useState<EntryErrors>({});
  const [submitted, setSubmitted] = useState(false);
  const longStr = (t: PropertyDocTag) => t.valueType === 'str' && ((t.value || '').length > 48 || (t.value || '').indexOf('\n') !== -1 || looksLikeJSON(t.value || ''));
  const [codeOpen, setCodeOpen] = useState<Record<number, boolean>>(() => {
    const s: Record<number, boolean> = {};
    init.tags.forEach((t) => { if (longStr(t)) s[t._uid] = true; });
    return s;
  });
  const trapRef = useFocusTrap(true, () => onClose());

  const set = (patch: Partial<EntryDraft>) => setV((cur) => ({ ...cur, ...patch }));
  const updTag = (i: number, patch: Partial<PropertyDocTag>) => set({ tags: v.tags.map((t, idx) => (idx === i ? { ...t, ...patch } : t)) });
  const delTag = (i: number) => set({ tags: v.tags.filter((_, idx) => idx !== i) });
  const addTag = () => set({ tags: [...v.tags, { key: '', valueType: 'str', value: '', _uid: ++uidSeq.current }] });
  const toggleCode = (uid: number) => setCodeOpen((s) => ({ ...s, [uid]: !s[uid] }));

  const applyMut = useMutation({
    mutationFn: () => {
      const id = v.id.trim();
      return apiDataSource.applyPropertyDocument(groupName, propName, id, {
        property: {
          metadata: { group: groupName, name: propName },
          id,
          tags: v.tags.map((t) => ({ key: t.key.trim(), value: encodePropertyTagValue(t.valueType, t.value) })),
        },
        // This modal always submits the document's COMPLETE tag set, so REPLACE
        // is the correct strategy: a tag the user removed here must be dropped
        // server-side (MERGE would silently keep it, breaking "tags can be
        // dropped"). REPLACE is also correct for a brand-new document.
        strategy: 'STRATEGY_REPLACE',
      });
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['resources', 'properties', groupName] });
      onClose();
      onApplied();
    },
  });

  const handleSubmit = () => {
    const e = validateEntry(v, { mode, existingIds });
    setSubmitted(true);
    setErrors(e);
    if (entryHasErrors(e)) return;
    applyMut.mutate();
  };

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <div className="modal is-wide" role="dialog" aria-modal="true" aria-label={isEdit ? 'Edit property document' : 'Apply property document'} ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit' : 'Apply'} property document</span>
            <p className="modal-sub">{isEdit ? 'The id is immutable. Tags can be added, updated or dropped.' : `Write a document to ${groupName}/${propName}. Tags are key-value pairs.`}</p>
          </div>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>
        <div className="modal-body">
          <section className="f-section">
            <div className="f-section-title">Key</div>
            <div className="prop-key-grid">
              <Field label="Group" locked><input className="f-input mono" value={groupName} disabled /></Field>
              <span className="prop-key-sep">/</span>
              <Field label="Name" locked><input className="f-input mono" value={propName} disabled /></Field>
              <span className="prop-key-sep">/</span>
              <Field label="ID" required={!isEdit} locked={isEdit} error={submitted ? errors.id : undefined}>
                <input
                  className="f-input mono"
                  value={v.id}
                  disabled={isEdit}
                  autoFocus={!isEdit}
                  placeholder="General-Service"
                  onChange={(e) => set({ id: e.target.value })}
                />
              </Field>
            </div>
          </section>

          <section className="f-section">
            <div className="f-section-title">Tags <span className="f-req">*</span></div>
            <p className="f-section-desc">Key-value pairs. Add, update or drop them by key.</p>
            {submitted && errors.tagsEmpty && <div className="f-error" style={{ marginBottom: 10 }}>{errors.tagsEmpty}</div>}
            <div className="spec-list">
              {v.tags.map((t, i) => {
                const er = (submitted && errors.tags?.[i]) || {};
                const canCode = t.valueType === 'str';
                const open = canCode && !!codeOpen[t._uid];
                return (
                  <div key={t._uid} className="kv-item">
                    <div className="kv-row">
                      <div className={'spec-cell' + (er.key ? ' has-error' : '')}>
                        <input className="f-input mono" value={t.key} placeholder="key" onChange={(e) => updTag(i, { key: e.target.value })} />
                        {er.key && <div className="f-error">{er.key}</div>}
                      </div>
                      <div className="spec-cell type">
                        <div className="f-select-wrap">
                          <select className="f-input f-select mono" value={t.valueType} onChange={(e) => updTag(i, { valueType: e.target.value as PropertyDocTag['valueType'] })}>
                            {PROP_VALUE_TYPES.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                          </select>
                          <span className="f-select-chev"><IconChevron size={13} /></span>
                        </div>
                      </div>
                      <div className="spec-cell grow kv-valuecell">
                        {open ? (
                          <span className="kv-codehint mono">edited in code editor below</span>
                        ) : (
                          <input
                            className="f-input mono"
                            value={t.value}
                            placeholder={t.valueType === 'str_array' || t.valueType === 'int_array' ? 'a, b, c' : 'value'}
                            onChange={(e) => updTag(i, { value: e.target.value })}
                          />
                        )}
                        {canCode && (
                          <button
                            type="button"
                            className={'kv-code-toggle' + (open ? ' is-on' : '')}
                            onClick={() => toggleCode(t._uid)}
                            title={open ? 'Collapse code editor' : 'Open code editor (for JSON / long text)'}
                          >
                            {'<>'}
                          </button>
                        )}
                      </div>
                      <button type="button" className="spec-del" onClick={() => delTag(i)} aria-label="Remove tag">
                        <IconClose width={14} height={14} />
                      </button>
                    </div>
                    {open && (
                      <div className="kv-code">
                        <CodeArea value={t.value} minHeight={150} onChange={(val) => updTag(i, { value: val })} />
                      </div>
                    )}
                  </div>
                );
              })}
              <button type="button" className="spec-add" onClick={addTag}>+ Add tag</button>
            </div>
          </section>
          {applyMut.isError && <div className="f-error" role="alert">{applyMut.error.message}</div>}
        </div>
        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={applyMut.isPending}>Cancel</button>
          <button type="button" className="btn btn-primary" onClick={handleSubmit} disabled={applyMut.isPending}>
            {applyMut.isPending ? 'Saving…' : isEdit ? 'Save changes' : 'Apply document'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ============ delete document ============ */

export interface DeletePropertyEntryModalProps {
  readonly groupName: string;
  readonly propName: string;
  readonly entry: PropertyDocument;
  readonly onClose: () => void;
  readonly onDeleted: () => void;
}

export function DeletePropertyEntryModal({ groupName, propName, entry, onClose, onDeleted }: DeletePropertyEntryModalProps) {
  const qc = useQueryClient();
  const trapRef = useFocusTrap(true, () => onClose());
  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deletePropertyDocument(groupName, propName, entry.id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['resources', 'properties', groupName] });
      onClose();
      onDeleted();
    },
  });
  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <div className="modal is-danger" role="dialog" aria-modal="true" aria-label="Delete this document?" ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">Delete this document?</span>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>
        <div className="modal-body">
          <p className="del-warn">
            You are about to permanently delete the document <b className="mono">{groupName}/{propName}/{entry.id}</b> and all of its tags. This action cannot be undone.
          </p>
          {deleteMut.isError && <div className="f-error">{deleteMut.error.message}</div>}
        </div>
        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={deleteMut.isPending}>No, keep it</button>
          <button type="button" className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={deleteMut.isPending}>
            {deleteMut.isPending ? 'Deleting…' : 'Yes, delete'}
          </button>
        </div>
      </div>
    </div>
  );
}
