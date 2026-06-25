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

import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type { StreamSchema, MeasureSchema, TraceSchema, PropertySchema } from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconMeasures, IconStreams, IconTraces, IconProperties,
  IconEdit, IconTrash, IconPlay, IconArrowLeft, IconKey, IconAlert, IconEmpty,
} from '../components/icons.js';

import { CATALOG_MAP } from './meta-utils.js';

const KIND_LABEL: Record<string, string> = {
  stream: 'Stream', streams: 'Stream',
  measure: 'Measure', measures: 'Measure',
  trace: 'Trace', traces: 'Trace',
  property: 'Property', properties: 'Property',
};

const TAG_TYPE_LABEL: Record<string, string> = {
  TAG_TYPE_STRING: 'string',
  TAG_TYPE_INT: 'int',
  TAG_TYPE_INT64: 'int64',
  TAG_TYPE_FLOAT64: 'float64',
  TAG_TYPE_STRING_ARRAY: 'string[]',
  TAG_TYPE_INT64_ARRAY: 'int64[]',
  TAG_TYPE_DATA_BINARY: 'binary',
  TAG_TYPE_TIMESTAMP: 'timestamp',
};

const FIELD_TYPE_LABEL: Record<string, string> = {
  FIELD_TYPE_STRING: 'string',
  FIELD_TYPE_INT: 'int',
  FIELD_TYPE_INT64: 'int64',
  FIELD_TYPE_FLOAT64: 'float64',
  FIELD_TYPE_DATA_BINARY: 'binary',
};

function typeIcon(type: string, size: number) {
  const t = type.toLowerCase();
  if (t === 'measure' || t === 'measures') return <IconMeasures size={size} />;
  if (t === 'stream' || t === 'streams') return <IconStreams size={size} />;
  if (t === 'trace' || t === 'traces') return <IconTraces size={size} />;
  return <IconProperties size={size} />;
}

function isStream(r: unknown): r is StreamSchema {
  return 'tagFamilies' in (r as object) && !('fields' in (r as object)) && !('traceIdTagName' in (r as object));
}

function isMeasure(r: unknown): r is MeasureSchema {
  return 'fields' in (r as object);
}

function isTrace(r: unknown): r is TraceSchema {
  return 'traceIdTagName' in (r as object);
}

function isProperty(r: unknown): r is PropertySchema {
  return 'tags' in (r as object) && !('tagFamilies' in (r as object)) && !('traceIdTagName' in (r as object));
}

function stripPrefix(value: string | undefined): string {
  if (!value) return '';
  const parts = value.split('_');
  // Drop leading enum prefix segments (all-caps) and reconstruct lowercase
  // e.g. ENCODING_METHOD_GORILLA → gorilla, COMPRESSION_METHOD_ZSTD → zstd
  return parts[parts.length - 1].toLowerCase();
}

// ── SpecTable ─────────────────────────────────────────────────────────────────

function SpecTable({ head, rows }: { head: string[]; rows: React.ReactNode[][] }) {
  return (
    <div className={'spec-table cols-' + head.length}>
      <div className="spec-thead">
        {head.map((h, i) => <span key={i}>{h}</span>)}
      </div>
      {rows.map((cells, i) => (
        <div key={i} className="spec-trow">
          {cells.map((c, j) => <span key={j}>{c}</span>)}
        </div>
      ))}
    </div>
  );
}

// ── ResourceDetailPage ────────────────────────────────────────────────────────

export function ResourceDetailPage({
  type,
  groupName,
  resourceName,
  onEdit,
  onDelete,
}: {
  type: string;
  groupName: string;
  resourceName: string;
  onEdit?: () => void;
  onDelete?: () => void;
}) {
  const navigate = useNavigate();
  const { session } = useAuth();
  const isAdmin = session?.role === 'admin';

  const { data: resource, isLoading, error } = useQuery({
    queryKey: ['resource', type, groupName, resourceName],
    queryFn: () => apiDataSource.getResource(type, groupName, resourceName),
  });

  const typeTitle = CATALOG_MAP[type]?.label ?? type.toUpperCase();
  const kindLabel = KIND_LABEL[type] ?? type;
  const typeBase = type.replace(/s$/, ''); // streams→stream, measures→measure, etc.

  // ── Loading ──────────────────────────────────────────────────────────────
  if (isLoading) {
    return (
      <div className="page-body">
        <div className="empty">
          <span className="empty-ico spin">{typeIcon(type, 32)}</span>
          <div className="empty-title">Loading…</div>
        </div>
      </div>
    );
  }

  // ── Error ────────────────────────────────────────────────────────────────
  if (error) {
    return (
      <div className="page-body">
        <div className="empty">
          <span className="empty-ico"><IconAlert size={32} /></span>
          <div className="empty-title">Failed to load {kindLabel}</div>
          <p className="empty-text">{(error as Error).message}</p>
        </div>
      </div>
    );
  }

  // ── Not found ────────────────────────────────────────────────────────────
  if (!resource) {
    return (
      <div className="page-body">
        <div className="empty">
          <span className="empty-ico"><IconEmpty size={32} /></span>
          <div className="empty-title">{kindLabel} not found</div>
          <p className="empty-text">{resourceName} does not exist in group {groupName}.</p>
        </div>
      </div>
    );
  }

  // ── Derived data ─────────────────────────────────────────────────────────
  const catalogLabel = isTrace(resource) ? 'TRACE' : isMeasure(resource) ? 'MEASURE' : isStream(resource) ? 'STREAM' : 'PROPERTY';
  const indexMode = isMeasure(resource) ? !!(resource as MeasureSchema).indexMode : false;
  const badgeText = indexMode ? 'INDEX MODE' : catalogLabel;

  let tagFamilyCount = 0;
  if (isStream(resource) || isMeasure(resource) || isTrace(resource)) {
    tagFamilyCount = (resource as StreamSchema).tagFamilies?.length ?? 0;
  }

  // ── Page ─────────────────────────────────────────────────────────────────
  return (
    <div className="page-body">

      {/* Header */}
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb">Metadata</span>
          <span className="crumb-sep" />
          <button
            className="crumb crumb-link"
            onClick={() => navigate(`/metadata/${typeBase}s`)}
          >
            {typeTitle}
          </button>
          <span className="crumb-sep" />
          <button
            className="crumb crumb-link"
            onClick={() => navigate(`/metadata/${typeBase}s/${groupName}`)}
          >
            {groupName}
          </button>
          <span className="crumb-sep" />
          <span className="crumb is-last">{resourceName}</span>
        </div>

        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">{resourceName}</h1>
            <span className="title-badge">{badgeText}</span>
          </div>
          <div className="page-actions">
            <button
              className="btn btn-ghost"
              onClick={() => navigate(`/query?type=${typeBase}&group=${groupName}&name=${resourceName}`)}
            >
              <IconPlay size={14} />
              Query
            </button>
            <button className="btn btn-ghost" onClick={() => navigate(-1)}>
              <IconArrowLeft size={14} />
              Back
            </button>
            {isAdmin && (
              <>
                <button className="btn btn-ghost" onClick={() => onEdit?.()}>
                  <IconEdit size={14} />
                  Edit
                </button>
                <button className="btn btn-danger-ghost" onClick={() => onDelete?.()}>
                  <IconTrash size={14} />
                  Delete
                </button>
              </>
            )}
          </div>
        </div>

        <p className="page-meta">{kindLabel} in group {groupName}</p>
      </header>

      {/* Summary meta chips */}
      <div className="grp-meta">
        <span className="meta-chip">
          <span className="meta-k">kind</span>
          <span className="meta-v">{kindLabel}</span>
        </span>
        {tagFamilyCount > 0 && (
          <span className="meta-chip">
            <span className="meta-k">tag families</span>
            <span className="meta-v">{tagFamilyCount}</span>
          </span>
        )}
        {isMeasure(resource) && (
          <span className="meta-chip">
            <span className="meta-k">fields</span>
            <span className="meta-v">{(resource as MeasureSchema).fields?.length ?? 0}</span>
          </span>
        )}
        {isMeasure(resource) && (resource as MeasureSchema).interval && (
          <span className="meta-chip">
            <span className="meta-k">interval</span>
            <span className="meta-v">{(resource as MeasureSchema).interval}</span>
          </span>
        )}
        {isMeasure(resource) && indexMode && (
          <span className="meta-chip">
            <span className="meta-k">index mode</span>
            <span className="meta-v">on</span>
          </span>
        )}
      </div>

      {/* Tag families (stream / measure / trace) */}
      {(isStream(resource) || isMeasure(resource) || isTrace(resource)) &&
        (resource as StreamSchema).tagFamilies?.map((family) => {
          const entityTagNames = (resource as StreamSchema).entity?.tagNames ?? [];
          const traceResource = isTrace(resource) ? (resource as TraceSchema) : null;

          const rows: React.ReactNode[][] = family.tags.map((tag) => {
            const typeLabel = TAG_TYPE_LABEL[tag.type] ?? tag.type;
            let roleCell: React.ReactNode = <span className="dim">—</span>;

            if (traceResource) {
              if (tag.name === traceResource.traceIdTagName) {
                roleCell = <span className="role-tag is-reserved">trace-id</span>;
              } else if (tag.name === traceResource.spanIdTagName) {
                roleCell = <span className="role-tag is-reserved">span-id</span>;
              } else if (tag.name === traceResource.timestampTagName) {
                roleCell = <span className="role-tag is-reserved">timestamp</span>;
              } else if (entityTagNames.includes(tag.name)) {
                roleCell = <span className="role-tag is-entity">entity</span>;
              }
            } else if (entityTagNames.includes(tag.name)) {
              roleCell = <span className="role-tag is-entity">entity</span>;
            }

            return [<span className="mono">{tag.name}</span>, typeLabel, <span className="role-cell">{roleCell}</span>];
          });

          return (
            <div key={family.name} className="detail-block">
              <div className="detail-h">
                <IconProperties size={15} />
                {' '}Tag family · <span className="mono">{family.name}</span>
              </div>
              <SpecTable head={['Tag', 'Type', 'Role']} rows={rows} />
            </div>
          );
        })
      }

      {/* Fields (measure only, when not indexMode) */}
      {isMeasure(resource) && !indexMode && (
        <div className="detail-block">
          <div className="detail-h">
            <IconMeasures size={15} />
            {' '}Fields
          </div>
          <SpecTable
            head={['Field', 'Type', 'Encoding · Compression']}
            rows={(resource as MeasureSchema).fields?.map((field) => {
              const typeLabel = FIELD_TYPE_LABEL[field.fieldType] ?? field.fieldType;
              const enc = stripPrefix(field.encodingMethod);
              const comp = stripPrefix(field.compressionMethod);
              const encComp = [enc, comp].filter(Boolean).join(' · ') || <span className="dim">—</span>;
              return [<span className="mono">{field.name}</span>, typeLabel, encComp];
            }) ?? []}
          />
        </div>
      )}

      {/* Tags (property only) */}
      {isProperty(resource) && (
        <div className="detail-block">
          <div className="detail-h">
            <IconProperties size={15} />
            {' '}Tags
          </div>
          <SpecTable
            head={['Tag', 'Type']}
            rows={(resource as PropertySchema).tags?.map((tag) => {
              const typeLabel = TAG_TYPE_LABEL[tag.type] ?? tag.type;
              return [<span className="mono">{tag.name}</span>, typeLabel];
            }) ?? []}
          />
        </div>
      )}

      {/* Entity (stream / measure) */}
      {(isStream(resource) || isMeasure(resource)) && (resource as StreamSchema).entity?.tagNames?.length > 0 && (
        <div className="detail-block">
          <div className="detail-h">
            <IconKey size={15} />
            {' '}Entity
          </div>
          <div className="chip-row">
            {(resource as StreamSchema).entity.tagNames.map((tagName, i) => (
              <span key={tagName} className="ord-chip">
                <span className="picker-ord">{i + 1}</span>
                {tagName}
              </span>
            ))}
          </div>
        </div>
      )}

    </div>
  );
}
