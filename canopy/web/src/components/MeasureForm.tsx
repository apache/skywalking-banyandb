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

import type { MeasureSchema, CreateMeasureRequest, UpdateMeasureRequest } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { IconChevron, IconPlus, IconTrash } from './icons.js';
import { useFocusTrap } from './modal-utils.js';

const TAG_TYPE_OPTIONS = [
  { value: 'TAG_TYPE_STRING',       label: 'String' },
  { value: 'TAG_TYPE_INT64',        label: 'Int64' },
  { value: 'TAG_TYPE_FLOAT64',      label: 'Float64' },
  { value: 'TAG_TYPE_STRING_ARRAY', label: 'String Array' },
  { value: 'TAG_TYPE_INT64_ARRAY',  label: 'Int64 Array' },
  { value: 'TAG_TYPE_DATA_BINARY',  label: 'Data Binary' },
];

const FIELD_TYPE_OPTIONS = [
  { value: 'FIELD_TYPE_STRING',      label: 'String' },
  { value: 'FIELD_TYPE_INT64',       label: 'Int64' },
  { value: 'FIELD_TYPE_FLOAT64',     label: 'Float64' },
  { value: 'FIELD_TYPE_DATA_BINARY', label: 'Data Binary' },
];

const ENCODING_OPTIONS = [
  { value: 'ENCODING_METHOD_UNSPECIFIED', label: 'Unspecified' },
  { value: 'ENCODING_METHOD_GORILLA',     label: 'Gorilla' },
];

const COMPRESSION_OPTIONS = [
  { value: 'COMPRESSION_METHOD_ZSTD', label: 'Zstd' },
];

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

interface TagRow { name: string; type: string; }
interface FamilyRow { name: string; tags: TagRow[]; }
interface FieldRow { name: string; fieldType: string; encodingMethod: string; compressionMethod: string; }

function SelectField({ value, onChange, options, id }: {
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
  id?: string;
}) {
  return (
    <div className="f-select-wrap">
      <select id={id} className="f-input f-select" value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
      <span className="f-select-chev"><IconChevron size={13} /></span>
    </div>
  );
}

/** MeasureForm renders a create/edit-measure modal or a delete-confirmation dialog. */
export function MeasureForm({ mode, groupName, initialName, onClose, onDeleted }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: MeasureSchema) => void;
  /** Fired after a confirmed delete — distinct from `onClose` so callers can
   * navigate away only when the resource was actually deleted, not when the
   * user cancelled via the X button or backdrop. */
  onDeleted?: () => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [interval, setInterval] = useState('1m');
  const [indexMode, setIndexMode] = useState(false);
  const [families, setFamilies] = useState<FamilyRow[]>([
    { name: 'default', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] },
  ]);
  const [fields, setFields] = useState<FieldRow[]>([]);
  const [entityTags, setEntityTags] = useState<string[]>([]);
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: editResource } = useQuery({
    queryKey: ['resource', 'measures', groupName, initialName ?? ''],
    queryFn: () => apiDataSource.getResource('measures', groupName, initialName!),
    enabled: mode === 'edit' && !!initialName,
  });
  const editSchema = editResource as MeasureSchema | undefined;

  useEffect(() => {
    if (mode === 'edit' && editSchema && !initialized) {
      setName(editSchema.metadata.name);
      setInterval(editSchema.interval ?? '1m');
      setIndexMode(editSchema.indexMode ?? false);
      setFamilies((editSchema.tagFamilies ?? []).map((f) => ({
        name: f.name,
        tags: (f.tags ?? []).map((t) => ({ name: t.name, type: t.type as string })),
      })));
      setFields((editSchema.fields ?? []).map((f) => ({
        name: f.name,
        fieldType: f.fieldType as string,
        encodingMethod: (f.encodingMethod ?? 'ENCODING_METHOD_UNSPECIFIED') as string,
        compressionMethod: (f.compressionMethod ?? 'COMPRESSION_METHOD_UNSPECIFIED') as string,
      })));
      setEntityTags(editSchema.entity?.tagNames ?? []);
      setInitialized(true);
    }
  }, [mode, editSchema, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateMeasureRequest) => apiDataSource.createMeasure(req),
    onSuccess: (measure) => {
      qc.invalidateQueries({ queryKey: ['resources', 'measures', groupName] });
      onClose(measure);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateMeasureRequest) => apiDataSource.updateMeasure(groupName, initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'measures', groupName] });
      qc.invalidateQueries({ queryKey: ['resource', 'measures', groupName, initialName] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteResource('measures', groupName, initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['resources', 'measures', groupName] });
      onClose();
      onDeleted?.();
    },
    onError: (e: Error) => setError(e.message),
  });

  function addFamily() {
    setFamilies((prev) => [...prev, { name: '', tags: [{ name: '', type: 'TAG_TYPE_STRING' }] }]);
  }

  function removeFamily(fi: number) {
    setFamilies((prev) => {
      const next = prev.filter((_, idx) => idx !== fi);
      const allNames = new Set(next.flatMap((f) => f.tags.map((t) => t.name).filter(Boolean)));
      setEntityTags((et) => et.filter((t) => allNames.has(t)));
      return next;
    });
  }

  function setFamilyName(fi: number, val: string) {
    setFamilies((prev) => prev.map((f, idx) => idx === fi ? { ...f, name: val } : f));
  }

  function addTag(fi: number) {
    setFamilies((prev) => prev.map((f, idx) =>
      idx === fi ? { ...f, tags: [...f.tags, { name: '', type: 'TAG_TYPE_STRING' }] } : f,
    ));
  }

  function removeTag(fi: number, ti: number) {
    setFamilies((prev) => {
      const next = prev.map((f, idx) => {
        if (idx !== fi) return f;
        return { ...f, tags: f.tags.filter((_, tagIdx) => tagIdx !== ti) };
      });
      const removedName = prev[fi]?.tags[ti]?.name;
      if (removedName) setEntityTags((et) => et.filter((t) => t !== removedName));
      return next;
    });
  }

  function setTagName(fi: number, ti: number, val: string) {
    setFamilies((prev) => prev.map((f, fIdx) =>
      fIdx !== fi ? f : { ...f, tags: f.tags.map((t, tIdx) => tIdx === ti ? { ...t, name: val } : t) },
    ));
  }

  function setTagType(fi: number, ti: number, val: string) {
    setFamilies((prev) => prev.map((f, fIdx) =>
      fIdx !== fi ? f : { ...f, tags: f.tags.map((t, tIdx) => tIdx === ti ? { ...t, type: val } : t) },
    ));
  }

  function addField() {
    setFields((prev) => [...prev, {
      name: '', fieldType: 'FIELD_TYPE_INT64',
      encodingMethod: 'ENCODING_METHOD_UNSPECIFIED', compressionMethod: 'COMPRESSION_METHOD_ZSTD',
    }]);
  }

  function removeField(fi: number) { setFields((prev) => prev.filter((_, idx) => idx !== fi)); }

  function setFieldProp<K extends keyof FieldRow>(fi: number, key: K, val: FieldRow[K]) {
    setFields((prev) => prev.map((f, idx) => idx === fi ? { ...f, [key]: val } : f));
  }

  const allTagNames = Array.from(new Set(families.flatMap((f) => f.tags.map((t) => t.name).filter(Boolean))));
  const availableTagNames = allTagNames.filter((n) => !entityTags.includes(n));

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError('');
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
    for (const field of fields) {
      if (!field.name.trim()) { setError('All fields must have a name.'); return; }
    }
    if (entityTags.length === 0) { setError('Select at least one entity tag.'); return; }
    for (const ent of entityTags) {
      if (!seenTagNames.has(ent)) { setError(`Entity tag "${ent}" is not defined in any family.`); return; }
    }
    if (!indexMode && !interval.trim()) { setError('Interval is required (e.g. 1d, 1h).'); return; }

    const measurePayload = {
      metadata: { name: submittedName, group: groupName },
      tagFamilies: families.map((f) => ({ name: f.name, tags: f.tags.map((t) => ({ name: t.name, type: t.type as never })) })),
      fields: indexMode ? [] : fields.map((f) => ({
        name: f.name, fieldType: f.fieldType as never,
        encodingMethod: f.encodingMethod as never, compressionMethod: f.compressionMethod as never,
      })),
      entity: { tagNames: entityTags },
      interval: indexMode ? '' : interval,
      indexMode,
    };

    if (mode === 'edit') { updateMut.mutate({ measure: measurePayload }); }
    else { createMut.mutate({ measure: measurePayload }); }
  }

  const trapRef = useFocusTrap(true, () => onClose());

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" role="dialog" aria-modal="true" aria-label="Delete measure" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete measure</span>
            <button className="modal-x" onClick={() => onClose()} aria-label="Close" />
          </div>
          <div className="modal-body">
            <p className="del-warn">
              You are about to permanently delete the measure{' '}
              <b className="mono">{initialName}</b> from group{' '}
              <b className="mono">{groupName}</b>. All stored data for this measure will be removed.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={deleteMut.isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={deleteMut.isPending}>
              {deleteMut.isPending ? 'Deleting…' : 'Delete measure'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const isEdit = mode === 'edit';
  const isBusy = createMut.isPending || updateMut.isPending;

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <div className="modal is-wide" role="dialog" aria-modal="true" aria-label={isEdit ? 'Edit measure' : 'Create measure'} ref={trapRef} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <div>
            <span className="modal-title">{isEdit ? 'Edit measure' : 'Create measure'}</span>
            <p className="modal-sub">
              {isEdit
                ? 'Name is immutable. Update the schema below — it is revalidated on save.'
                : `Define a measure in group “${groupName}”.`}
            </p>
          </div>
          <button type="button" className="modal-x" onClick={() => onClose()} aria-label="Close" />
        </div>

        <form id="measure-form" className="modal-body" onSubmit={handleSubmit} noValidate>
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
                <input id="m-name" className="f-input mono" type="text" placeholder="service_cpm_minute"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </Field>
              <Field label="Group" locked hint="Resources are scoped to their group">
                <input className="f-input mono" type="text" value={groupName} disabled />
              </Field>
            </div>
          </section>

          {/* Measure options */}
          <section className="f-section">
            <div className="f-section-title">Measure options</div>
            <div className="f-grid">
              <Field
                label="Interval"
                hint="Sampling rate, e.g. 1m, 1h, 1d · units ns, us, ms, s, m, h, d"
              >
                <input id="m-interval" className="f-input mono" type="text" placeholder="1m"
                  value={interval} onChange={(e) => setInterval(e.target.value)} disabled={indexMode} />
              </Field>
              <Field
                label="Index mode"
                hint="Store only in the index — no field values allowed"
              >
                <label className="f-check" style={{ marginTop: 6 }}>
                  <input type="checkbox" checked={indexMode}
                    onChange={(e) => {
                      const on = e.target.checked;
                      setIndexMode(on);
                      if (on) setFields([]);
                    }} />
                  Enable index mode
                </label>
              </Field>
            </div>
          </section>

          {/* Tag families */}
          <section className="f-section">
            <div className="f-section-title">Tag families <span className="f-req">*</span></div>
            <div className="fam-list">
              {families.map((fam, fi) => (
                <div className="fam-card" key={fi}>
                  <div className="fam-head">
                    <div className="fam-name">
                      <input className="f-input mono" type="text" placeholder="Family name"
                        value={fam.name} onChange={(e) => setFamilyName(fi, e.target.value)} />
                    </div>
                    {families.length > 1 && (
                      <button type="button" className="fam-del" title="Remove family"
                        onClick={() => removeFamily(fi)}>
                        Remove family
                      </button>
                    )}
                  </div>
                  {fam.tags.map((tag, ti) => (
                    <div className="spec-row" key={ti}>
                      <div className="spec-cell grow">
                        <input className="f-input mono" type="text" placeholder="tag_name"
                          value={tag.name} onChange={(e) => setTagName(fi, ti, e.target.value)} />
                      </div>
                      <div className="spec-cell type">
                        <SelectField value={tag.type} onChange={(v) => setTagType(fi, ti, v)} options={TAG_TYPE_OPTIONS} />
                      </div>
                      <button type="button" className="spec-del" title="Remove tag"
                        onClick={() => removeTag(fi, ti)} disabled={fam.tags.length === 1}
                        aria-label="Remove tag">
                        <IconTrash size={14} />
                      </button>
                    </div>
                  ))}
                  <button type="button" className="spec-add" onClick={() => addTag(fi)}>
                    <IconPlus size={14} /> Add tag
                  </button>
                </div>
              ))}
            </div>
            <button type="button" className="btn btn-ghost stage-add" onClick={addFamily}>
              <IconPlus size={14} /> Add tag family
            </button>
          </section>

          {/* Fields */}
          {!indexMode && (
            <section className="f-section">
              <div className="f-section-title">Fields <span className="f-optional">optional</span></div>
              <p className="f-section-desc">Numeric values stored per data point.</p>
              {fields.map((field, fi) => (
                <div className="field-row" key={fi}>
                  <div className="spec-cell grow">
                    <input className="f-input mono" type="text" placeholder="field_name"
                      value={field.name} onChange={(e) => setFieldProp(fi, 'name', e.target.value)} />
                  </div>
                  <div className="spec-cell type">
                    <SelectField value={field.fieldType} onChange={(v) => setFieldProp(fi, 'fieldType', v)} options={FIELD_TYPE_OPTIONS} />
                  </div>
                  <div className="spec-cell type">
                    <SelectField value={field.encodingMethod} onChange={(v) => setFieldProp(fi, 'encodingMethod', v)} options={ENCODING_OPTIONS} />
                  </div>
                  <div className="spec-cell type">
                    <SelectField value={field.compressionMethod} onChange={(v) => setFieldProp(fi, 'compressionMethod', v)} options={COMPRESSION_OPTIONS} />
                  </div>
                  <button type="button" className="spec-del" title="Remove field" onClick={() => removeField(fi)}
                    aria-label="Remove field">
                    <IconTrash size={14} />
                  </button>
                </div>
              ))}
              <button type="button" className="spec-add" onClick={addField}>
                <IconPlus size={14} /> Add field
              </button>
            </section>
          )}

          {/* Entity */}
          <section className="f-section">
            <div className="f-section-title">Entity <span className="f-req">*</span></div>
            <p className="f-section-desc">Tags whose values form the series key and determine sharding. Order matters.</p>
            <div className="picker">
              <div className="picker-selected">
                {entityTags.length === 0 ? (
                  <span className="picker-empty">Add tags above, then choose the entity tags.</span>
                ) : (
                  entityTags.map((tag, idx) => (
                    <button key={tag} type="button" className="picker-chip is-on" title="Remove"
                      onClick={() => setEntityTags((prev) => prev.filter((t) => t !== tag))}>
                      <span className="picker-ord">{idx + 1}</span>
                      <span>{tag}</span>
                    </button>
                  ))
                )}
              </div>
              <div className="picker-avail">
                {availableTagNames.length === 0 ? (
                  <span className="picker-all">All tags selected</span>
                ) : (
                  availableTagNames.map((tag) => (
                    <button key={tag} type="button" className="picker-chip"
                      onClick={() => setEntityTags((prev) => [...prev, tag])}>
                      <IconPlus size={11} />
                      <span>{tag}</span>
                    </button>
                  ))
                )}
              </div>
            </div>
          </section>

          {error && <p className="f-error">{error}</p>}
        </form>

        <div className="modal-foot">
          <button className="btn btn-ghost" type="button" onClick={() => onClose()} disabled={isBusy}>Cancel</button>
          <button className="btn btn-primary" type="submit" form="measure-form" disabled={isBusy}>
            {isBusy ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save changes' : 'Create measure')}
          </button>
        </div>
      </div>
    </div>
  );
}
