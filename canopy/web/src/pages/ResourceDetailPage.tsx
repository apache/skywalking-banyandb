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
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

import type {
  StreamSchema, MeasureSchema, TraceSchema, PropertySchema,
  IndexRuleSchema, IndexRuleBindingSchema, IndexType,
} from 'canopy-shared';
import { apiDataSource } from '../data/api.js';
import { useAuth } from '../auth/AuthContext.js';
import {
  IconMeasures, IconStreams, IconTraces, IconProperties,
  IconEdit, IconTrash, IconPlay, IconArrowLeft, IconKey, IconAlert, IconEmpty,
  IconIndex,
} from '../components/icons.js';

import { TYPE_TITLES } from './meta-utils.js';

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
  TAG_TYPE_FLOAT: 'float',
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
  FIELD_TYPE_FLOAT: 'float',
  FIELD_TYPE_FLOAT64: 'float64',
  FIELD_TYPE_DATA_BINARY: 'binary',
};

// IndexRule.Type from BanyanDB is `TYPE_TREE` / `TYPE_INVERTED` / `TYPE_SKIPPING`
// on the wire (we map to these display labels).
const INDEX_TYPE_LABEL: Record<string, string> = {
  TYPE_TREE: 'tree',
  TYPE_INVERTED: 'inverted',
  TYPE_SKIPPING: 'skipping',
  INDEX_TYPE_TREE: 'tree',
  INDEX_TYPE_INVERTED: 'inverted',
};
const INDEX_TYPE_TONE: Record<string, string> = {
  TYPE_TREE: 'is-tree',
  TYPE_INVERTED: 'is-inv',
  TYPE_SKIPPING: 'is-skip',
  INDEX_TYPE_TREE: 'is-tree',
  INDEX_TYPE_INVERTED: 'is-inv',
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

// ── TagIndexRules ─────────────────────────────────────────────────────────────
// Per-tag chips linking to the IndexRules that index this tag. Hover state is
// shared so the same rule highlights across all rows, making it clear which
// tags share an index rule.

function TagIndexRules({
  rules, tagName, onOpen, hoveredRule, onHoverRule,
}: {
  rules: IndexRuleSchema[];
  tagName: string;
  onOpen: (ruleName: string) => void;
  hoveredRule: string | null;
  onHoverRule: (name: string | null) => void;
}) {
  if (!rules.length) {
    return <span className="tag-noidx">not indexed</span>;
  }
  return (
    <span className="tag-idx-row">
      {rules.map((ir) => {
        const hot = hoveredRule === ir.metadata.name;
        const irTags = ir.tags ?? [];
        const title =
          `Index rule “${ir.metadata.name}” · ${INDEX_TYPE_LABEL[ir.type] ?? stripPrefix(ir.type)}` +
          (ir.analyzer ? ` · ${ir.analyzer}` : '') +
          `\nIndexes tag${irTags.length !== 1 ? 's' : ''}: ${irTags.join(', ')}`;
        return (
          <span className={'tag-idx-pair' + (hot ? ' is-hot' : '')} key={ir.metadata.name}>
            <button
              className={'tag-idx-chip ' + (INDEX_TYPE_TONE[ir.type] ?? '') + (hot ? ' is-highlight' : '')}
              title={title}
              onClick={() => onOpen(ir.metadata.name)}
              onMouseEnter={() => onHoverRule(ir.metadata.name)}
              onMouseLeave={() => onHoverRule(null)}
            >
              <IconIndex size={11} />
              <span className="tag-idx-name mono">{ir.metadata.name}</span>
            </button>
            <span className="tag-idx-tagchip mono" title={'Indexed tag: ' + tagName}>{tagName}</span>
          </span>
        );
      })}
    </span>
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
  const [hoveredRule, setHoveredRule] = useState<string | null>(null);

  const { data: resource, isLoading, error } = useQuery({
    queryKey: ['resource', type, groupName, resourceName],
    queryFn: () => apiDataSource.getResource(type, groupName, resourceName),
  });

  // IndexRules scoped to this group — used for the per-tag "Index rules" column.
  const { data: indexRules = [] } = useQuery<IndexRuleSchema[]>({
    queryKey: ['indexRules', groupName],
    queryFn: () => apiDataSource.listIndexRules(groupName),
  });
  // IndexRuleBindings scoped to this group. A tag on THIS resource is
  // "indexed" by a rule only when a binding explicitly ties that rule to
  // THIS resource (matching subject.name) and is currently active. Without
  // this filter, every resource with the tag would falsely claim the rule
  // is indexing it.
  const { data: indexBindings = [] } = useQuery<IndexRuleBindingSchema[]>({
    queryKey: ['indexRuleBindings', groupName],
    queryFn: () => apiDataSource.listIndexRuleBindings(groupName),
  });
  const rulesForTag = (n: string) => {
    // Rule names that have at least one binding targeting THIS resource.
    const boundRuleNames = new Set(
      indexBindings
        .filter((b) => b.subject.name === resourceName)
        .flatMap((b) => b.rules),
    );
    return indexRules.filter(
      (ir) => boundRuleNames.has(ir.metadata.name) && (ir.tags ?? []).includes(n),
    );
  };

  const typeTitle = TYPE_TITLES[type] ?? type;
  const kindLabel = KIND_LABEL[type] ?? type;
  const typeBase = type.replace(/s$/, '');
  const indexPath = `/metadata/${typeBase}s/${groupName}/Index`;
  const openIndexRule = (ruleName: string) => {
    // Stash the rule name so the Index page can highlight the matching row
    // on arrival, then navigate. The Index page reads-and-clears this slot.
    (window as unknown as { __idxFocus?: { rule?: string } }).__idxFocus = { rule: ruleName };
    navigate(indexPath);
  };

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
  const indexMode = isMeasure(resource) ? !!(resource as MeasureSchema).indexMode : false;
  // Detail-page badge uses Title Case singular ("Measure") so it sits naturally
  // next to the lowercase resource name (e.g. `cpu_usage [Measure]`).
  const kindSingular = isTrace(resource) ? 'Trace'
    : isMeasure(resource) ? 'Measure'
    : isStream(resource) ? 'Stream'
    : 'Property';
  const badgeText = indexMode ? 'INDEX MODE' : kindSingular;

  let tagFamilyCount = 0;
  if (isStream(resource) || isMeasure(resource)) {
    tagFamilyCount = (resource as StreamSchema).tagFamilies?.length ?? 0;
  }

  // ── Page ─────────────────────────────────────────────────────────────────
  return (
    <div className="page-body">

      {/* Header */}
      <header className="page-head">
        <div className="crumbs">
          <span className="crumb">Metadata</span>
          <span className="crumb-sep">/</span>
          <button
            className="crumb crumb-link"
            onClick={() => navigate(`/metadata/${typeBase}s`)}
          >
            {typeTitle}
          </button>
          <span className="crumb-sep">/</span>
          <button
            className="crumb crumb-link"
            onClick={() => navigate(`/metadata/${typeBase}s/${groupName}`)}
          >
            {groupName}
          </button>
          <span className="crumb-sep">/</span>
          <span className="crumb is-last">{resourceName}</span>
        </div>

        <div className="page-title-row">
          <div className="page-title-wrap">
            <h1 className="page-title">
              {resourceName}
              <span className="title-badge">{badgeText}</span>
            </h1>
          </div>
          <div className="page-actions">
            <button
              className="btn btn-primary"
              onClick={() => navigate(`/query?type=${typeBase}&group=${groupName}&name=${resourceName}`)}
            >
              <IconPlay size={15} />
              Query
            </button>
            <button className="btn btn-ghost" onClick={() => navigate(indexPath)}>
              <IconArrowLeft size={14} />
              Back to group
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
        {isTrace(resource) && (
          <span className="meta-chip">
            <span className="meta-k">tags</span>
            <span className="meta-v">{(resource as TraceSchema).tags?.length ?? 0}</span>
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
        {isMeasure(resource) && (
          <span className="meta-chip">
            <span className="meta-k">index mode</span>
            <span className="meta-v">{indexMode ? 'on' : 'off'}</span>
          </span>
        )}
      </div>

      {/* Tag families (stream / measure) — with Index rules column */}
      {(isStream(resource) || isMeasure(resource)) &&
        (resource as StreamSchema).tagFamilies?.map((family) => {
          const entityTagNames = (resource as StreamSchema).entity?.tagNames ?? [];
          const rows: React.ReactNode[][] = family.tags.map((tag) => {
            const typeLabel = TAG_TYPE_LABEL[tag.type] ?? tag.type;
            const roleCell: React.ReactNode = entityTagNames.includes(tag.name)
              ? <span className="role-tag is-entity">entity</span>
              : <span className="dim">—</span>;
            return [
              <span key="name" className="mono">{tag.name}</span>,
              <span key="type" className="type-pill">{typeLabel}</span>,
              <span key="role" className="role-cell">{roleCell}</span>,
              <TagIndexRules key="rules" rules={rulesForTag(tag.name)} tagName={tag.name} onOpen={openIndexRule}
                hoveredRule={hoveredRule} onHoverRule={setHoveredRule} />,
            ];
          });
          return (
            <div key={family.name} className="detail-block">
              <div className="detail-h">
                <IconProperties size={15} />
                {' '}Tag family · <span className="mono">{family.name}</span>
              </div>
              <SpecTable head={['Tag', 'Type', 'Role', 'Index rules']} rows={rows} />
            </div>
          );
        })
      }

      {/* Fields (measure only, when not indexMode) — use type-pill */}
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
              return [
                <span key="name" className="mono">{field.name}</span>,
                <span key="type" className="type-pill">{typeLabel}</span>,
                <span key="enc" className="mono dim">{encComp}</span>,
              ];
            }) ?? []}
          />
        </div>
      )}

      {/* Trace tags — Index rules column too */}
      {isTrace(resource) && (
        <div className="detail-block">
          <div className="detail-h">
            <IconProperties size={15} />
            {' '}Tags
          </div>
          <SpecTable
            head={['Tag', 'Type', 'Role', 'Index rules']}
            rows={(resource as TraceSchema).tags?.map((tag) => {
              const typeLabel = TAG_TYPE_LABEL[tag.type] ?? tag.type;
              let roleCell: React.ReactNode = <span className="dim">—</span>;
              if (tag.name === (resource as TraceSchema).traceIdTagName) {
                roleCell = <span className="role-tag is-reserved">trace id</span>;
              } else if (tag.name === (resource as TraceSchema).spanIdTagName) {
                roleCell = <span className="role-tag is-reserved">span id</span>;
              } else if (tag.name === (resource as TraceSchema).timestampTagName) {
                roleCell = <span className="role-tag is-reserved">timestamp</span>;
              }
              return [
                <span key="name" className="mono">{tag.name}</span>,
                <span key="type" className="type-pill">{typeLabel}</span>,
                <span key="role" className="role-cell">{roleCell}</span>,
                <TagIndexRules key="rules" rules={rulesForTag(tag.name)} tagName={tag.name} onOpen={openIndexRule}
                  hoveredRule={hoveredRule} onHoverRule={setHoveredRule} />,
              ];
            }) ?? []}
          />
        </div>
      )}

      {/* Property tags — no Index rules column (per handoff) */}
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
              return [
                <span key="name" className="mono">{tag.name}</span>,
                <span key="type" className="type-pill">{typeLabel}</span>,
              ];
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

// Suppress unused-import warning for IndexType (kept for type completeness).
export type { IndexType };
