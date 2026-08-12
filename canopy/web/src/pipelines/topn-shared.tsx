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

// topn-shared.tsx — direction<->sort mapping, the RankBadge/CondChip atoms and
// the flat-criteria<->model.v1.Criteria codec shared by TopNForms.tsx,
// TopNList.tsx and TopNDetail.tsx (docs/pipelines-design.md §1/§6).
//
// Ported from the handoff's topn-page.jsx (RankBadge, CondChip, SORT_TONE/
// SORT_RANK helpers) and topn-form.jsx (TOPN_OPS, DEFAULT_COUNTERS), pulled
// into one module so the form and the list/detail pages don't have to import
// from each other. NEW here (the handoff had no wire-codec — its mock
// `criteria` was already a flat array): buildTopNCriteria/flattenTopNCriteria,
// which translate the UI's flat ANDed {tag,op,value} rows to/from the real
// model.v1.Criteria recursive tree BanyanDB expects. This reuses
// PropertyTagValue/encodePropertyTagValue/decodePropertyTagValue from
// data/api.ts — model.v1.Criteria's Condition.value is the same TagValue
// oneof for every catalog (property, measure, ...), so there is nothing
// TopN-specific to add there.

import { useMemo } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
import type { PropertyCriteria, PropertyTagValue, MeasureSchema, TopNAggregationSchema } from 'canopy-shared';
import { apiDataSource, decodePropertyTagValue } from '../data/api.js';
import { IconTopN } from '../components/icons.js';

export type TopNSort = 'SORT_DESC' | 'SORT_ASC' | 'SORT_UNSPECIFIED' | undefined;

// BanyanDB defaults an unset field_value_sort to SORT_DESC (proto3 zero value
// for the Sort enum) — treat undefined the same as SORT_DESC everywhere.
export function topNTone(sort: TopNSort): 'topn' | 'bottomn' | 'bothn' {
  if (sort === 'SORT_ASC') return 'bottomn';
  if (sort === 'SORT_UNSPECIFIED') return 'bothn';
  return 'topn';
}

export function topNRank(sort: TopNSort): 'topN' | 'bottomN' | 'both' {
  if (sort === 'SORT_ASC') return 'bottomN';
  if (sort === 'SORT_UNSPECIFIED') return 'both';
  return 'topN';
}

export function topNSortLabel(sort: TopNSort): string {
  return sort ?? 'SORT_DESC';
}

export const SORT_OPTS: ReadonlyArray<{
  readonly value: 'SORT_DESC' | 'SORT_ASC' | 'SORT_UNSPECIFIED';
  readonly rank: 'topN' | 'bottomN' | 'both';
  readonly label: string;
  readonly hint: string;
}> = [
  { value: 'SORT_DESC', rank: 'topN', label: 'SORT_DESC', hint: 'ranks the largest values first' },
  { value: 'SORT_ASC', rank: 'bottomN', label: 'SORT_ASC', hint: 'ranks the smallest values first' },
  { value: 'SORT_UNSPECIFIED', rank: 'both', label: 'SORT_UNSPECIFIED', hint: 'tracks both top and bottom counters' },
];

export const DEFAULT_COUNTERS = 1000;

/** model.v1.Condition.BinaryOp offered in the flat criteria editor — mirrors
 *  property-bydbql.ts's PROP_OPS (comparison + set membership; MATCH/HAVING
 *  are query-time-only operators, not meaningful for a topn-agg's criteria). */
export const TOPN_OPS: ReadonlyArray<{ readonly value: string; readonly label: string }> = [
  { value: 'BINARY_OP_EQ', label: 'equals  =' },
  { value: 'BINARY_OP_NE', label: 'not equals  ≠' },
  { value: 'BINARY_OP_GT', label: 'greater  >' },
  { value: 'BINARY_OP_GE', label: 'greater or equal  ≥' },
  { value: 'BINARY_OP_LT', label: 'less  <' },
  { value: 'BINARY_OP_LE', label: 'less or equal  ≤' },
  { value: 'BINARY_OP_IN', label: 'in  (a, b)' },
  { value: 'BINARY_OP_NOT_IN', label: 'not in  (a, b)' },
];

const TOPN_OP_SYMBOL: Record<string, string> = {
  BINARY_OP_EQ: '=',
  BINARY_OP_NE: '≠',
  BINARY_OP_GT: '>',
  BINARY_OP_GE: '≥',
  BINARY_OP_LT: '<',
  BINARY_OP_LE: '≤',
  BINARY_OP_IN: 'IN',
  BINARY_OP_NOT_IN: 'NOT IN',
};

export function topnOpSymbol(op: string): string {
  return TOPN_OP_SYMBOL[op] ?? op;
}

/** small shared badge: DESC->topN / ASC->bottomN / UNSPECIFIED->both */
export function RankBadge({ sort, large }: { readonly sort: TopNSort; readonly large?: boolean }) {
  const cls = (large ? 'topn-rank-lg' : 'topn-rank') + ' is-' + topNTone(sort);
  return (
    <span className={cls}>
      <IconTopN size={large ? 14 : 11} />
      {topNRank(sort)}
    </span>
  );
}

/** a single criteria condition rendered as a chip: tag op value */
export function CondChip({ tag, op, value }: { readonly tag: string; readonly op: string; readonly value: string }) {
  return (
    <span className="topn-cond mono">
      <span className="topn-cond-tag">{tag}</span>
      <span className="topn-cond-op">{topnOpSymbol(op)}</span>
      <span className="topn-cond-val">{value === '' ? '∅' : value}</span>
    </span>
  );
}

// ── flat criteria <-> model.v1.Criteria codec ───────────────────────────────

export interface TopNCondition {
  readonly tag: string;
  readonly op: string;
  readonly value: string;
}

// TagValue has no float variant — only integers encode as int; a decimal
// like 1.5 falls through to str instead of producing an invalid int payload.
const PQ_NUM = (s: string): boolean => /^-?\d+$/.test(s.trim());

function topnConditionValue(op: string, raw: string): PropertyTagValue {
  const value = raw.trim();
  if (op === 'BINARY_OP_IN' || op === 'BINARY_OP_NOT_IN') {
    const parts = value.split(',').map((x) => x.trim()).filter(Boolean);
    return parts.length && parts.every(PQ_NUM) ? { intArray: { value: parts } } : { strArray: { value: parts } };
  }
  return PQ_NUM(value) ? { int: { value } } : { str: { value } };
}

/** Build a flat AND chain of Conditions into the model.v1.Criteria tree the
 *  registry expects. Empty/blank-tag rows are dropped. */
export function buildTopNCriteria(conditions: readonly TopNCondition[]): PropertyCriteria | undefined {
  const parts: PropertyCriteria[] = conditions
    .filter((c) => c.tag.trim())
    .map((c) => ({ condition: { name: c.tag.trim(), op: c.op, value: topnConditionValue(c.op, c.value) } }));
  if (!parts.length) return undefined;
  return parts.reduce((acc, part) => ({ le: { op: 'LOGICAL_OP_AND', left: acc, right: part } }));
}

/** Flatten a model.v1.Criteria tree back into the editor's row shape.
 *  Documented simplification (matches the design's "flat ANDed list, not the
 *  recursive WHERE tree"): every `condition` leaf reachable through nested
 *  `le` nodes is surfaced as one AND row regardless of whether the original
 *  tree actually used AND or OR — a schema built outside this UI with OR'd
 *  criteria will round-trip lossily, same tradeoff PropertyForms' editor
 *  accepts for its own criteria tree. */
export function flattenTopNCriteria(criteria: PropertyCriteria | undefined): TopNCondition[] {
  const out: TopNCondition[] = [];
  const walk = (node: PropertyCriteria | undefined): void => {
    if (!node) return;
    if (node.condition) {
      out.push({ tag: node.condition.name, op: node.condition.op, value: decodePropertyTagValue(node.condition.value).value });
      return;
    }
    if (node.le) { walk(node.le.left); walk(node.le.right); }
  };
  walk(criteria);
  return out;
}

// ── measure lookups (mirrors the handoff's data.jsx findMeasure/
// measureFieldNames/measureTagNames, adapted to canopy's fetched-schema
// shape) ─────────────────────────────────────────────────────────────────

/** Field names defined on a measure resource — offered as `field_name` ranking picks. */
export function measureFieldNames(m: MeasureSchema | undefined): string[] {
  return m?.fields ? m.fields.map((f) => f.name) : [];
}

/** Tag names defined on a measure resource — offered for group-by + criteria picks. */
export function measureTagNames(m: MeasureSchema | undefined): string[] {
  if (!m) return [];
  return m.tagFamilies.flatMap((f) => f.tags.map((t) => t.name));
}

/** Resolve a measure resource by {group,name} out of a group-name -> resources map. */
export function findMeasureIn(
  resourcesByGroup: ReadonlyMap<string, readonly MeasureSchema[]>,
  ref: { readonly group: string; readonly name: string } | undefined,
): MeasureSchema | undefined {
  if (!ref?.group || !ref.name) return undefined;
  return resourcesByGroup.get(ref.group)?.find((m) => m.metadata.name === ref.name);
}

// ── shared data hook ────────────────────────────────────────────────────────
//
// Every Pipelines/TopN surface (overview counts, the cross-group list, the
// create/edit form's source pickers) needs the same two things: every measure
// group + its measures (for the source-measure picker and "does the source
// still exist" check), and every measure group's registered TopNAggregations.
// Centralizing the fetch here means the three surfaces share one TanStack
// Query cache (same query keys `['groups']` / `['resources','measures',g]` /
// `['topnAggregations',g]` already used by GroupPage/Sidebar/QueryConsole),
// so navigating between them doesn't re-fetch, and a create/edit/delete
// mutation that invalidates `['topnAggregations', group]` refreshes every
// consumer at once.

export interface TopNRow {
  readonly agg: TopNAggregationSchema;
  readonly group: string;
}

export function topNAggregationsQueryKey(group: string): readonly [string, string] {
  return ['topnAggregations', group] as const;
}

export interface TopNCatalog {
  /** Every CATALOG_MEASURE group. */
  readonly measureGroups: readonly { readonly name: string }[];
  /** Every registered TopNAggregation, across all measure groups. */
  readonly rows: readonly TopNRow[];
  /** Each measure group's measures, keyed by group name — powers the source
   *  picker and the "source measure still exists" check. */
  readonly resourcesByGroup: ReadonlyMap<string, readonly MeasureSchema[]>;
  readonly isLoading: boolean;
}

export function useTopNCatalog(): TopNCatalog {
  const { data: groupsData, isLoading: groupsLoading } = useQuery({
    queryKey: ['groups'],
    queryFn: () => apiDataSource.listGroups(),
  });
  const measureGroups = useMemo(
    () => (groupsData?.groups ?? []).filter((g) => g.catalog === 'CATALOG_MEASURE'),
    [groupsData],
  );

  const aggQueries = useQueries({
    queries: measureGroups.map((g) => ({
      queryKey: topNAggregationsQueryKey(g.name),
      queryFn: () => apiDataSource.listTopNAggregations(g.name),
    })),
  });
  const resourceQueries = useQueries({
    queries: measureGroups.map((g) => ({
      queryKey: ['resources', 'measures', g.name],
      queryFn: () => apiDataSource.listResourcesInGroup('measures', g.name),
    })),
  });

  const rows = useMemo(() => {
    const out: TopNRow[] = [];
    measureGroups.forEach((g, i) => {
      for (const agg of aggQueries[i]?.data ?? []) out.push({ agg, group: g.name });
    });
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- aggQueries is a
    // fresh array of useQueries results every render; depend on the actual
    // fetched data (via measureGroups + JSON) rather than the array identity.
  }, [measureGroups, JSON.stringify(aggQueries.map((q) => q.dataUpdatedAt))]);

  const resourcesByGroup = useMemo(() => {
    const m = new Map<string, readonly MeasureSchema[]>();
    measureGroups.forEach((g, i) => m.set(g.name, (resourceQueries[i]?.data ?? []) as MeasureSchema[]));
    return m;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- see rows above.
  }, [measureGroups, JSON.stringify(resourceQueries.map((q) => q.dataUpdatedAt))]);

  const isLoading = groupsLoading || aggQueries.some((q) => q.isLoading) || resourceQueries.some((q) => q.isLoading);

  return { measureGroups, rows, resourcesByGroup, isLoading };
}
