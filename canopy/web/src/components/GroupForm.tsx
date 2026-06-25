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

import type { Group, CreateGroupRequest, UpdateGroupRequest } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { parseInterval, formatInterval } from '../pages/meta-utils.js';

const CATALOGS = ['CATALOG_MEASURE', 'CATALOG_STREAM', 'CATALOG_PROPERTY', 'CATALOG_TRACE'] as const;
type Catalog = typeof CATALOGS[number];

const CATALOG_LABELS: Record<string, string> = {
  CATALOG_MEASURE: 'Measure',
  CATALOG_STREAM: 'Stream',
  CATALOG_PROPERTY: 'Property',
  CATALOG_TRACE: 'Trace',
};

/** GroupForm renders a create-group modal, an edit-group modal, or a delete-group confirmation dialog. */
export function GroupForm({ mode, initialName, onClose }: {
  mode: 'create' | 'edit' | 'delete';
  initialName?: string;
  onClose: (created?: Group) => void;
}) {
  const qc = useQueryClient();

  const [name, setName] = useState('');
  const [catalog, setCatalog] = useState<Catalog>('CATALOG_MEASURE');
  const [shardNum, setShardNum] = useState(2);
  const [segmentInterval, setSegmentInterval] = useState('1d');
  const [ttl, setTtl] = useState('7d');
  const [error, setError] = useState('');
  const [initialized, setInitialized] = useState(false);

  const { data: groupsData } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
    enabled: mode === 'edit',
  });
  const editGroup = groupsData?.groups.find((g) => g.name === initialName);

  useEffect(() => {
    if (mode === 'edit' && editGroup && !initialized) {
      setShardNum(editGroup.resourceOpts.shardNum);
      setSegmentInterval(formatInterval(editGroup.resourceOpts.segmentInterval));
      setTtl(formatInterval(editGroup.resourceOpts.ttl));
      setInitialized(true);
    }
  }, [mode, editGroup, initialized]);

  const createMut = useMutation({
    mutationFn: (req: CreateGroupRequest) => apiDataSource.createGroup(req),
    onSuccess: (group) => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose(group);
    },
    onError: (e: Error) => setError(e.message),
  });

  const updateMut = useMutation({
    mutationFn: (req: UpdateGroupRequest) => apiDataSource.updateGroup(initialName!, req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => apiDataSource.deleteGroup(initialName!),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose();
    },
    onError: (e: Error) => setError(e.message),
  });

  const isPropertyCatalog = mode === 'create' ? catalog === 'CATALOG_PROPERTY' : editGroup?.catalog === 'CATALOG_PROPERTY';

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError('');

    if (isPropertyCatalog) {
      if (mode === 'edit') {
        updateMut.mutate({ group: { metadata: { name: initialName! }, catalog: editGroup?.catalog, resourceOpts: { shardNum } } });
        return;
      }
      if (!name.trim()) { setError('Name is required.'); return; }
      createMut.mutate({ group: { metadata: { name: name.trim() }, catalog, resourceOpts: { shardNum } } });
      return;
    }

    const parsedSegment = parseInterval(segmentInterval);
    const parsedTtl = parseInterval(ttl);
    if (!parsedSegment) {
      setError('Segment interval must be a number followed by h or d (e.g. 1d, 24h).');
      return;
    }
    if (!parsedTtl) {
      setError('TTL must be a number followed by h or d (e.g. 7d, 168h).');
      return;
    }

    if (mode === 'edit') {
      updateMut.mutate({
        group: {
          metadata: { name: initialName! },
          catalog: editGroup?.catalog,
          resourceOpts: { shardNum, segmentInterval: parsedSegment, ttl: parsedTtl },
        },
      });
      return;
    }

    if (!name.trim()) {
      setError('Name is required.');
      return;
    }
    createMut.mutate({
      group: {
        metadata: { name: name.trim() },
        catalog,
        resourceOpts: { shardNum, segmentInterval: parsedSegment, ttl: parsedTtl },
      },
    });
  }

  const isPending = createMut.isPending || updateMut.isPending || deleteMut.isPending;

  if (mode === 'delete') {
    return (
      <div className="modal-overlay" onClick={() => onClose()}>
        <div className="modal is-danger" onClick={(e) => e.stopPropagation()}>
          <div className="modal-head">
            <span className="modal-title">Delete group</span>
            <button className="modal-x" onClick={() => onClose()} />
          </div>
          <div className="modal-body">
            <p>
              This will permanently delete group{' '}
              <span className="mono">{initialName}</span> and all its resources.
            </p>
            {error && <div className="f-error">{error}</div>}
          </div>
          <div className="modal-foot">
            <button className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
              Cancel
            </button>
            <button
              className="btn btn-danger"
              onClick={() => deleteMut.mutate()}
              disabled={isPending}
            >
              {isPending ? 'Deleting…' : 'Delete'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const isEdit = mode === 'edit';

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()} data-initialized={String(initialized)}>
        <div className="modal-head">
          <span className="modal-title">{isEdit ? 'Edit group' : 'New group'}</span>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          <div className="f-section">
            <label className="f-field">
              <span className="f-label">
                Name {!isEdit && <span className="f-req">*</span>}
              </span>
              <input
                className="f-input"
                type="text"
                value={isEdit ? (initialName ?? '') : name}
                onChange={(e) => { if (!isEdit) setName(e.target.value); }}
                readOnly={isEdit}
                autoFocus={!isEdit}
              />
            </label>
          </div>

          {!isEdit && (
            <div className="f-section">
              <span className="f-label">Catalog</span>
              <div className="f-seg">
                {CATALOGS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    className={`btn${catalog === c ? ' is-on' : ''}`}
                    onClick={() => setCatalog(c)}
                  >
                    {CATALOG_LABELS[c]}
                  </button>
                ))}
              </div>
            </div>
          )}

          {isEdit && editGroup && (
            <div className="f-section">
              <label className="f-field">
                <span className="f-label">Catalog</span>
                <input className="f-input" type="text" value={editGroup.catalog} readOnly />
              </label>
            </div>
          )}

          <div className="f-section">
            <div className="f-grid">
              <label className="f-field">
                <span className="f-label">Shard count</span>
                <input
                  className="f-input"
                  type="number"
                  min={1}
                  max={256}
                  value={shardNum}
                  onChange={(e) => setShardNum(Number(e.target.value))}
                />
              </label>
              {!isPropertyCatalog && (
                <label className="f-field">
                  <span className="f-label">Segment interval</span>
                  <input
                    className="f-input"
                    type="text"
                    placeholder="1d"
                    value={segmentInterval}
                    onChange={(e) => setSegmentInterval(e.target.value)}
                  />
                </label>
              )}
              {!isPropertyCatalog && (
                <label className="f-field">
                  <span className="f-label">TTL</span>
                  <input
                    className="f-input"
                    type="text"
                    placeholder="7d"
                    value={ttl}
                    onChange={(e) => setTtl(e.target.value)}
                  />
                </label>
              )}
            </div>
          </div>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save' : 'Create')}
          </button>
        </div>
      </form>
    </div>
  );
}
