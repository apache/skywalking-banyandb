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

import type { StreamSchema, CreateStreamRequest, UpdateStreamRequest } from 'canopy-shared';
import { TagType } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconChevron, IconPlus, IconTrash } from './icons.js';
import { useFocusTrap } from './modal-utils.js';

const TAG_TYPES = [
  'TAG_TYPE_STRING',
  'TAG_TYPE_INT64',
  'TAG_TYPE_FLOAT64',
  'TAG_TYPE_STRING_ARRAY',
  'TAG_TYPE_INT64_ARRAY',
  'TAG_TYPE_DATA_BINARY',
] as const;

interface TagRow { name: string; type: string; }
interface FamilyRow { name: string; tags: TagRow[]; }

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

/** StreamForm renders either a create/edit-stream modal or a delete-stream confirmation dialog. */
export function StreamForm({ mode, groupName, initialName, onClose, onDeleted }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: StreamSchema) => void;
  /** Fired after a confirmed delete — distinct from `onClose` so callers can
   * navigate away only when the resource was actually deleted, not when the
   * user cancelled via the X button or backdrop. */
  onDeleted?: () => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [families, setFamilies] = useState<FamilyRow[]>([
    { name: 'default', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] },
  ]);
  const [entityTags, setEntityTags] = useState<string[]>([]);
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['resource', 'streams', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getResource('streams', groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });
  const editSchema = editResource as StreamSchema | undefined;

  useEffect(() => {
    if (mode === 'edit' && editSchema && !initialized) {
      setName(editSchema.metadata.name);
      setFamilies((editSchema.tagFamilies ?? []).map((f) => ({
        name: f.name,
        tags: (f.tags ?? []).map((t) => ({ name: t.name, type: t.type as string })),
      })));
      setEntityTags(editSchema.entity?.tagNames ?? []);
      setInitialized(true);
    }
  }, [mode, editSchema, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateStreamRequest) => apiDataSource.createStream(req),
    onSuccess: (stream) => {
      qc.invalidateQueries({ queryKey: ['resources', 'streams', groupName] });
      onClose(stream);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateStreamRequest) => apiDataSource.updateStream(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'streams', groupName] });
      qc.invalidateQueries({ queryKey: ['resource', 'streams', groupName, initialName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteResource('streams', groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'streams', groupName] });
      onClose();
      onDeleted?.();
    },
    onError: (e: Error) => setError(e.message),
  });

  const allTagNames = families.flatMap((f) => f.tags.map((t) => t.name)).filter(Boolean);
  const availTags = allTagNames.filter((n) => !entityTags.includes(n));

  function addFamily() {
    setFamilies((prev) => [...prev, { name: '', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] }]);
  }

  function removeFamily(famIdx: number) {
    setFamilies((prev) => prev.filter((_, i) => i !== famIdx));
  }

  function updateFamilyName(famIdx: number, value: string) {
    setFamilies((prev) => prev.map((fam, i) => i === famIdx ? { ...fam, name: value } : fam));
  }

  function addTag(famIdx: number) {
    setFamilies((prev) => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: [...fam.tags, { name: '', type: 'TAG_TYPE_STRING' }] } : fam,
    ));
  }

  function removeTag(famIdx: number, tagIdx: number) {
    setFamilies((prev) => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: fam.tags.filter((_, j) => j !== tagIdx) } : fam,
    ));
  }

  function updateTagName(famIdx: number, tagIdx: number, value: string) {
    setFamilies((prev) => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: fam.tags.map((tag, j) => j === tagIdx ? { ...tag, name: value } : tag) } : fam,
    ));
  }

  function updateTagType(famIdx: number, tagIdx: number, value: string) {
    setFamilies((prev) => prev.map((fam, i) =>
      i === famIdx ? { ...fam, tags: fam.tags.map((tag, j) => j === tagIdx ? { ...tag, type: value } : tag) } : fam,
    ));
  }

  function toggleEntityTag(tagName: string) {
    setEntityTags((prev) => prev.includes(tagName) ? prev.filter((t) => t !== tagName) : [...prev, tagName]);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const submittedName = mode === 'edit' ? initialName! : name.trim();
    if (!submittedName) { setError('Name is required.'); return; }
    if (families.length === 0) { setError('At least one tag family is required.'); return; }
    const seenTagNames = new Set<string>();
    for (const fam of families) {
      if (!fam.name.trim()) { setError('Each tag family must have a name.'); return; }
      for (const tag of fam.tags) {
        if (!tag.name.trim()) { setError(`All tags in family "${fam.name}" must have names.`); return; }
        if (tag.name.includes('#')) { setError(`Tag name "${tag.name}" must not contain "#".`); return; }
        if (seenTagNames.has(tag.name)) { setError(`Duplicate tag name "${tag.name}".`); return; }
        seenTagNames.add(tag.name);
      }
    }
    if (entityTags.length === 0) { setError('At least one entity tag is required.'); return; }
    // Guard against entity tags left dangling after a referenced tag was removed
    // or renamed (removeTag does not prune entityTags) — the server rejects these.
    for (const ent of entityTags) {
      if (!seenTagNames.has(ent)) { setError(`Entity tag "${ent}" is not defined in any family.`); return; }
    }
    setError('');

    const streamPayload = {
      metadata: { name: submittedName, group: groupName },
      tagFamilies: families.map((f) => ({
        name: f.name,
        tags: f.tags.map((t) => ({ name: t.name, type: t.type as TagType })),
      })),
      entity: { tagNames: entityTags },
    };

    if (mode === 'edit') { updateMut.mutate({ stream: streamPayload }); }
    else { createMut.mutate({ stream: streamPayload }); }
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;
  const trapRef = useFocusTrap(true, () => onClose());

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete stream</span>
            <button className="modal-x" onClick={() => onClose()} aria-label="Close" />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete the stream{' '}
              <b className="mono">{initialName}</b> from group{' '}
              <b className="mono">{groupName}</b>. All stored data for this stream will be removed.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={isPending}>
              {isPending ? 'Deleting…' : 'Delete stream'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const isEdit = mode === 'edit';

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" ref={trapRef} onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit stream' : 'Create stream'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the schema below — it is revalidated on save.'
                : `Define a stream in group “${groupName}”.`}
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
                <input className="f-input mono" type="text" placeholder="access_log"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </Field>
              <Field label="Group" locked hint="Resources are scoped to their group">
                <input className="f-input mono" type="text" value={groupName} disabled />
              </Field>
            </div>
          </section>

          {/* Tag families */}
          <section className="f-section">
            <div className="f-section-title">Tag families <span className="f-req">*</span></div>
            <div className="fam-list">
              {families.map((fam, famIdx) => (
                <div className="fam-card" key={famIdx}>
                  <div className="fam-head">
                    <div className="fam-name">
                      <input className="f-input mono" type="text" placeholder="Family name"
                        value={fam.name} onChange={(e) => updateFamilyName(famIdx, e.target.value)} />
                    </div>
                    {families.length > 1 && (
                      <button type="button" className="fam-del" title="Remove family"
                        onClick={() => removeFamily(famIdx)}>
                        Remove family
                      </button>
                    )}
                  </div>
                  {fam.tags.map((tag, tagIdx) => (
                    <div className="spec-row" key={tagIdx}>
                      <div className="spec-cell grow">
                        <input className="f-input mono" type="text" placeholder="tag_name"
                          value={tag.name} onChange={(e) => updateTagName(famIdx, tagIdx, e.target.value)} />
                      </div>
                      <div className="spec-cell type">
                        <div className="f-select-wrap">
                          <select className="f-input f-select mono" value={tag.type}
                            onChange={(e) => updateTagType(famIdx, tagIdx, e.target.value)}>
                            {TAG_TYPES.map((tt) => <option key={tt} value={tt}>{tt}</option>)}
                          </select>
                          <span className="f-select-chev"><IconChevron /></span>
                        </div>
                      </div>
                      <button type="button" className="spec-del" title="Remove tag"
                        onClick={() => removeTag(famIdx, tagIdx)} disabled={fam.tags.length === 1}
                        aria-label="Remove tag">
                        <IconTrash size={14} />
                      </button>
                    </div>
                  ))}
                  <button type="button" className="spec-add" onClick={() => addTag(famIdx)}>
                    <IconPlus size={14} /> Add tag
                  </button>
                </div>
              ))}
            </div>
            <button type="button" className="btn btn-ghost stage-add" onClick={addFamily}>
              <IconPlus size={14} /> Add tag family
            </button>
          </section>

          {/* Entity */}
          <section className="f-section">
            <div className="f-section-title">Entity <span className="f-req">*</span></div>
            <p className="f-section-desc">Tags whose values form the series key and determine sharding. Order matters.</p>
            <div className="picker">
              {allTagNames.length === 0 ? (
                <span className="picker-empty">Add tags above, then choose the entity tags.</span>
              ) : (
                <>
                  <div className="picker-selected">
                    {entityTags.map((tagName, ord) => (
                      <button key={tagName} type="button" className="picker-chip is-on"
                        onClick={() => toggleEntityTag(tagName)}>
                        <span className="picker-ord">{ord + 1}</span>
                        <span>{tagName}</span>
                      </button>
                    ))}
                  </div>
                  <div className="picker-avail">
                    {availTags.length === 0 ? (
                      <span className="picker-all">All tags selected</span>
                    ) : (
                      availTags.map((tagName) => (
                        <button key={tagName} type="button" className="picker-chip"
                          onClick={() => toggleEntityTag(tagName)}>
                          <IconPlus size={11} />
                          <span>{tagName}</span>
                        </button>
                      ))
                    )}
                  </div>
                </>
              )}
            </div>
          </section>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create stream')}
          </button>
        </div>
      </form>
    </div>
  );
}
