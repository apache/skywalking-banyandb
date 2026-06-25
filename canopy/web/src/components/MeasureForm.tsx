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
  { value: 'COMPRESSION_METHOD_UNSPECIFIED', label: 'Unspecified' },
  { value: 'COMPRESSION_METHOD_ZSTD',        label: 'Zstd' },
];

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
export function MeasureForm({ mode, groupName, initialName, onClose }: {
  mode: 'create' | 'edit' | 'delete';
  groupName: string;
  initialName?: string;
  onClose: (created?: MeasureSchema) => void;
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
      encodingMethod: 'ENCODING_METHOD_UNSPECIFIED', compressionMethod: 'COMPRESSION_METHOD_UNSPECIFIED',
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
    for (const fam of families) {
      if (!fam.name.trim()) { setError('Each tag family must have a name.'); return; }
      for (const tag of fam.tags) {
        if (!tag.name.trim()) { setError(`All tags in family "${fam.name}" must have names.`); return; }
      }
    }
    if (entityTags.length === 0) { setError('Select at least one entity tag.'); return; }

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

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete measure</span>
            <button className="modal-x btn btn-ghost" onClick={() => onClose()}>✕</button>
          </div>
          <div className="modal-body">
            <p>This will permanently delete measure <span className="mono">{initialName}</span>.</p>
            {error && <p className="f-error">{error}</p>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={deleteMut.isPending}>Cancel</button>
            <button className="btn btn-danger" onClick={() => deleteMut.mutate()} disabled={deleteMut.isPending}>
              {deleteMut.isPending ? 'Deleting…' : 'Delete'}
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
      <div className="modal is-wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">{isEdit ? 'Edit measure' : 'New measure'}</span>
          <button className="modal-x btn btn-ghost" onClick={() => onClose()}>✕</button>
        </div>

        <form id="measure-form" className="modal-body" onSubmit={handleSubmit} noValidate>
          <div className="f-section">
            <div className="f-grid" style={{ gridTemplateColumns: '1fr 1fr' }}>
              <div className="f-field">
                <label className="f-label" htmlFor="m-name">Name {!isEdit && <span className="f-req">*</span>}</label>
                <input id="m-name" className="f-input" type="text" placeholder="my-measure"
                  value={isEdit ? (initialName ?? '') : name}
                  onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                  readOnly={isEdit} autoFocus={!isEdit} />
              </div>
              <div className="f-field">
                <label className="f-label" htmlFor="m-interval">Interval</label>
                <input id="m-interval" className="f-input" type="text" placeholder="1m"
                  value={interval} onChange={(e) => setInterval(e.target.value)} disabled={indexMode} />
                {indexMode && <span className="f-hint dim">Disabled in index mode</span>}
              </div>
            </div>
            <label className="f-check">
              <input type="checkbox" checked={indexMode} onChange={(e) => setIndexMode(e.target.checked)} />
              Index mode
            </label>
          </div>

          <div className="f-section">
            <div className="f-section-title">Tag families <span className="f-req">*</span></div>
            <div className="fam-list">
              {families.map((fam, fi) => (
                <div className="fam-card" key={fi}>
                  <div className="fam-head">
                    <input className="f-input fam-name" type="text" placeholder="Family name"
                      value={fam.name} onChange={(e) => setFamilyName(fi, e.target.value)} />
                    <button type="button" className="btn btn-ghost fam-del" title="Remove family"
                      onClick={() => removeFamily(fi)} disabled={families.length === 1}>
                      <IconTrash size={14} />
                    </button>
                  </div>
                  {fam.tags.map((tag, ti) => (
                    <div className="spec-row" key={ti}>
                      <div className="spec-cell">
                        <input className="f-input" type="text" placeholder="Tag name"
                          value={tag.name} onChange={(e) => setTagName(fi, ti, e.target.value)} />
                      </div>
                      <div className="spec-cell">
                        <SelectField value={tag.type} onChange={(v) => setTagType(fi, ti, v)} options={TAG_TYPE_OPTIONS} />
                      </div>
                      <div className="spec-cell">
                        <button type="button" className="btn btn-ghost" title="Remove tag"
                          onClick={() => removeTag(fi, ti)} disabled={fam.tags.length === 1}>
                          <IconTrash size={14} />
                        </button>
                      </div>
                    </div>
                  ))}
                  <button type="button" className="btn btn-ghost" onClick={() => addTag(fi)}>
                    <IconPlus size={13} /> Add tag
                  </button>
                </div>
              ))}
            </div>
            <button type="button" className="btn btn-ghost" onClick={addFamily}>
              <IconPlus size={14} /> Add family
            </button>
          </div>

          {!indexMode && (
            <div className="f-section">
              <div className="f-section-title">Fields <span className="f-optional dim">(optional)</span></div>
              {fields.map((field, fi) => (
                <div className="field-row" key={fi}>
                  <div className="spec-cell">
                    <input className="f-input" type="text" placeholder="Field name"
                      value={field.name} onChange={(e) => setFieldProp(fi, 'name', e.target.value)} />
                  </div>
                  <div className="spec-cell">
                    <SelectField value={field.fieldType} onChange={(v) => setFieldProp(fi, 'fieldType', v)} options={FIELD_TYPE_OPTIONS} />
                  </div>
                  <div className="spec-cell">
                    <SelectField value={field.encodingMethod} onChange={(v) => setFieldProp(fi, 'encodingMethod', v)} options={ENCODING_OPTIONS} />
                  </div>
                  <div className="spec-cell">
                    <SelectField value={field.compressionMethod} onChange={(v) => setFieldProp(fi, 'compressionMethod', v)} options={COMPRESSION_OPTIONS} />
                  </div>
                  <div className="spec-cell">
                    <button type="button" className="btn btn-ghost" title="Remove field" onClick={() => removeField(fi)}>
                      <IconTrash size={14} />
                    </button>
                  </div>
                </div>
              ))}
              <button type="button" className="btn btn-ghost" onClick={addField}>
                <IconPlus size={14} /> Add field
              </button>
            </div>
          )}

          <div className="f-section">
            <div className="f-section-title">Entity</div>
            <div className="f-section-desc">Select tag names as entity identifiers</div>
            <div className="picker">
              <div className="picker-selected">
                {entityTags.length === 0 ? (
                  <span className="picker-empty dim">No entity tags selected</span>
                ) : (
                  entityTags.map((tag, idx) => (
                    <button key={tag} type="button" className="picker-chip is-on" title="Remove"
                      onClick={() => setEntityTags((prev) => prev.filter((t) => t !== tag))}>
                      <span className="picker-ord">{idx + 1}</span>{tag}
                    </button>
                  ))
                )}
              </div>
              <div className="picker-avail">
                {availableTagNames.length === 0 ? (
                  <span className="picker-empty dim">No available tags</span>
                ) : (
                  <div className="picker-all">
                    {availableTagNames.map((tag) => (
                      <button key={tag} type="button" className="picker-chip"
                        onClick={() => setEntityTags((prev) => [...prev, tag])}>
                        {tag}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>

          {error && <p className="f-error">{error}</p>}
        </form>

        <div className="modal-foot">
          <button className="btn btn-ghost" type="button" onClick={() => onClose()} disabled={isBusy}>Cancel</button>
          <button className="btn btn-primary" type="submit" form="measure-form" disabled={isBusy}>
            {isBusy ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save measure' : 'Create measure')}
          </button>
        </div>
      </div>
    </div>
  );
}
