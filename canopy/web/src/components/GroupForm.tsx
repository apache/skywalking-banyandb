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

import type { Group, CreateGroupRequest } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';

const CATALOGS = ['CATALOG_MEASURE', 'CATALOG_STREAM', 'CATALOG_PROPERTY'] as const;
type Catalog = typeof CATALOGS[number];

const CATALOG_LABELS: Record<string, string> = {
  CATALOG_MEASURE: 'Measure',
  CATALOG_STREAM: 'Stream',
  CATALOG_PROPERTY: 'Property',
};

/** GroupForm renders either a create-group modal or a delete-group confirmation dialog. */
export function GroupForm({ mode, initialName, onClose }: {
  mode: 'create' | 'delete';
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

  const createMut = useMutation({
    mutationFn: (req: CreateGroupRequest) => apiDataSource.createGroup(req),
    onSuccess: (group) => {
      qc.invalidateQueries({ queryKey: ['groups'] });
      onClose(group);
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

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) {
      setError('Name is required.');
      return;
    }
    setError('');
    createMut.mutate({
      group: {
        metadata: { name: name.trim() },
        catalog,
        resourceOpts: { shardNum, segmentInterval, ttl },
      },
    });
  }

  const isPending = createMut.isPending || deleteMut.isPending;

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

  return (
    <div className="modal-overlay" onClick={() => onClose()}>
      <form className="modal is-wide" onSubmit={handleSubmit} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="modal-title">New group</span>
          <button type="button" className="modal-x" onClick={() => onClose()} />
        </div>

        <div className="modal-body">
          <div className="f-section">
            <label className="f-field">
              <span className="f-label">
                Name <span className="f-req">*</span>
              </span>
              <input
                className="f-input"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                autoFocus
              />
            </label>
          </div>

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
            </div>
          </div>

          {error && <div className="f-error">{error}</div>}
        </div>

        <div className="modal-foot">
          <button type="button" className="btn btn-ghost" onClick={() => onClose()} disabled={isPending}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={isPending}>
            {isPending ? 'Creating…' : 'Create'}
          </button>
        </div>
      </form>
    </div>
  );
}
