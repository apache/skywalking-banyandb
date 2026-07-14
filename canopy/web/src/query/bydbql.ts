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

// bydbql.ts — pure BydbQL codegen for the M4 QueryConsole.

import type { Group } from 'canopy-shared';
// Ported verbatim from .handoff-import/banyandb/project/query-builder.jsx
// (qbNodeSQL / buildBydbQL / qbQuote / QB_CATALOGS / QB_OPS / QB_AGGS /
// QB_TIMES / QB_COMBINATORS / QB_TOPN_OPS / QB_TOPN_AGGS).
//
// Top-N DATA vs SCHEMA: this codegen produces queries for the Top-N DATA
// endpoint (POST /v1/measure/topn, leaderboard consumption). The Top-N
// SCHEMA endpoints (/v1/topn-agg/schema/...) are out of M4 scope — they
// ship with the TopN aggregation definition form in a later milestone.

// ── Builder vocabulary (faithful to model.v1.BinaryOp) ──────────────────────

export type QB_CATALOG_VALUE = 'measures' | 'streams' | 'traces' | 'topn';

export interface QB_CATALOG_DEF {
  readonly value: QB_CATALOG_VALUE;
  readonly kw: string;
  readonly label: string;
  readonly blurb: string;
}

export const QB_CATALOGS: readonly QB_CATALOG_DEF[] = [
  { value: 'measures', kw: 'MEASURE', label: 'Measure', blurb: 'Numeric time-series' },
  { value: 'streams', kw: 'STREAM', label: 'Stream', blurb: 'Append-only events & logs' },
  { value: 'traces', kw: 'TRACE', label: 'Trace', blurb: 'Spans grouped by trace' },
  { value: 'topn', kw: 'MEASURE', label: 'Top-N', blurb: 'Ranked series leaderboard' },
];

export const QB_CAT = (v: string): QB_CATALOG_DEF =>
  QB_CATALOGS.find((c) => c.value === v) ?? QB_CATALOGS[0];

// Top-N queries read from a measure; its schema lives in the measures catalog.
export const qbDataCatalog = (catalog: string): 'measures' | 'streams' | 'traces' =>
  catalog === 'topn' ? 'measures' : (catalog as 'measures' | 'streams' | 'traces');

// Map the handoff's short catalog name ('measures' | 'streams' | 'traces' | 'topn')
// to the BanyanDB proto-style enum the REST gateway speaks. Returns undefined
// for 'topn' so callers can detect "TopN uses its own endpoint" without a
// separate equality check.
export type QBProtoCatalog = 'CATALOG_MEASURE' | 'CATALOG_STREAM' | 'CATALOG_TRACE' | undefined;
// BanyanDB's catalog enum is singular ('CATALOG_MEASURE' / 'CATALOG_STREAM' /
// 'CATALOG_TRACE'), NOT plural. The handoff's short name ('measures' /
// 'streams' / 'traces' / 'topn') is the API-level key — when we render
// the REST gateway's Catalog value, the S must be dropped.
export const qbProtoCatalog = (catalog: string): QBProtoCatalog =>
  catalog === 'topn' ? undefined : `CATALOG_${qbDataCatalog(catalog).slice(0, -1).toUpperCase()}` as QBProtoCatalog;

export interface QB_OP_DEF {
  readonly value: string;
  readonly sql: string;
  readonly label: string;
}

export const QB_OPS: readonly QB_OP_DEF[] = [
  { value: 'BINARY_OP_EQ', sql: '=', label: 'equals  =' },
  { value: 'BINARY_OP_NE', sql: '!=', label: 'not equals  ≠' },
  { value: 'BINARY_OP_GT', sql: '>', label: 'greater  >' },
  { value: 'BINARY_OP_GE', sql: '>=', label: 'greater or equal  ≥' },
  { value: 'BINARY_OP_LT', sql: '<', label: 'less  <' },
  { value: 'BINARY_OP_LE', sql: '<=', label: 'less or equal  ≤' },
  { value: 'BINARY_OP_IN', sql: 'IN', label: 'in  (a, b)' },
  { value: 'BINARY_OP_NOT_IN', sql: 'NOT IN', label: 'not in  (a, b)' },
  { value: 'BINARY_OP_MATCH', sql: 'MATCH', label: 'match  (full-text)' },
];

export const QB_OP = (v: string): QB_OP_DEF =>
  QB_OPS.find((o) => o.value === v) ?? QB_OPS[0];

// Top-N criteria support comparison + set ops but not full-text MATCH.
export const QB_TOPN_OPS: readonly QB_OP_DEF[] =
  QB_OPS.filter((o) => o.value !== 'BINARY_OP_MATCH');

export interface QB_AGG_DEF {
  readonly value: string;
  readonly label: string;
}

export const QB_AGGS: readonly QB_AGG_DEF[] = [
  { value: '', label: 'raw — no aggregation' },
  { value: 'MEAN', label: 'MEAN — average' },
  { value: 'SUM', label: 'SUM — total' },
  { value: 'MAX', label: 'MAX — maximum' },
  { value: 'MIN', label: 'MIN — minimum' },
  { value: 'COUNT', label: 'COUNT — points' },
];

export const QB_TOPN_AGGS: readonly QB_AGG_DEF[] = [
  { value: '', label: 'none — pre-aggregated value' },
  { value: 'SUM', label: 'SUM — total over range' },
  { value: 'MEAN', label: 'MEAN — average' },
  { value: 'COUNT', label: 'COUNT — points' },
  { value: 'MAX', label: 'MAX — maximum' },
  { value: 'MIN', label: 'MIN — minimum' },
];

export interface QB_COMBINATOR_DEF {
  readonly value: 'AND' | 'OR';
  readonly label: string;
}

export const QB_COMBINATORS: readonly QB_COMBINATOR_DEF[] = [
  { value: 'AND', label: 'AND' },
  { value: 'OR', label: 'OR' },
];

export const QB_TIMES: readonly { value: string; label: string }[] = [
  { value: '', label: 'All time' },
  { value: '-5m', label: 'Last 5 minutes' },
  { value: '-15m', label: 'Last 15 minutes' },
  { value: '-30m', label: 'Last 30 minutes' },
  { value: '-1h', label: 'Last 1 hour' },
  { value: '-3h', label: 'Last 3 hours' },
  { value: '-6h', label: 'Last 6 hours' },
  { value: '-24h', label: 'Last 24 hours' },
  { value: '-7d', label: 'Last 7 days' },
];

// ── WHERE tree shape (local, decoupled from shared/query.ts WhereLeaf) ─────
//
// The handoff prototype uses a flat leaf shape { tag, op, value }. We mirror
// that here and let the QueryBuilder component convert to/from shared DTOs.

export interface QBWhereLeaf {
  readonly tag: string;
  readonly op: string;
  readonly value: string;
}

export interface QBWhereLeafWithConn extends QBWhereLeaf {
  readonly conn?: 'AND' | 'OR';
}

export interface QBWhereGroup {
  readonly combinator: 'AND' | 'OR';
  readonly children: readonly QBWhereNode[];
}

export interface QBWhereGroupWithConn extends QBWhereGroup {
  readonly children: readonly QBWhereLeafWithConn[];
}

export type QBWhereNode = QBWhereLeafWithConn | QBWhereGroupWithConn;

export const qbIsGroup = (n: QBWhereNode | undefined | null): n is QBWhereGroupWithConn =>
  !!(n && Array.isArray((n as QBWhereGroupWithConn).children));

// connector joining child i to the previous sibling (legacy trees fall back
// to the group-level `combinator`).
const qbConn = (
  node: QBWhereGroupWithConn,
  c: QBWhereLeafWithConn,
): 'AND' | 'OR' => c.conn ?? node.combinator ?? 'AND';

// split a group's rendered parts into OR-separated runs of ANDed parts.
// Returns segments where each segment is a list of items joined by ' AND '.
// A segment is parenthesized only when it has 2+ items (so the outer
// precedence is unambiguous). Single-item segments stay bare.
function qbConnSegments(
  node: QBWhereGroupWithConn,
  items: readonly { readonly sql: string }[],
  children: readonly QBWhereLeafWithConn[],
): readonly (readonly { readonly sql: string }[])[] {
  if (items.length === 0) return [];
  const segs: { readonly sql: string }[][] = [[items[0]]];
  for (let i = 1; i < items.length; i++) {
    if (qbConn(node, children[i]) === 'OR') segs.push([]);
    segs[segs.length - 1] = [...segs[segs.length - 1], items[i]];
  }
  return segs;
}

export const qbConnSummary = (root: QBWhereGroupWithConn): 'AND' | 'OR' | 'mixed' => {
  const conns = root.children.slice(1).map((c) => qbConn(root, c));
  if (conns.length === 0) return 'AND';
  return conns.every((x) => x === conns[0]) ? conns[0] : 'mixed';
};

// ── Quote / literal helpers ────────────────────────────────────────────────

const QB_NUM = (s: string): boolean => /^-?\d+(\.\d+)?$/.test(s.trim());

/** Format a literal value for BydbQL. IN / NOT IN expand a comma list. */
export const qbQuote = (op: string, raw: string | null | undefined): string => {
  const value = (raw == null ? '' : String(raw)).trim();
  if (op === 'BINARY_OP_IN' || op === 'BINARY_OP_NOT_IN') {
    const parts = value.split(',').map((x) => x.trim()).filter(Boolean);
    return '(' + parts.map((p) => (QB_NUM(p) ? p : `'${p}'`)).join(', ') + ')';
  }
  if (value === '') return "''";
  return QB_NUM(value) ? value : `'${value}'`;
};

// ── WHERE expression: recursive groups with () / AND / OR ──────────────────

/** Back-compat: accept either the new `where` tree or a legacy flat `filters` array. */
export const qbWhereRoot = (
  s: {
    readonly where?: unknown;
    readonly filters?: readonly { readonly tag: string; readonly op: string; readonly value: string }[];
  },
): QBWhereGroupWithConn => {
  if (s.where && qbIsGroup(s.where as QBWhereNode)) {
    return s.where as QBWhereGroupWithConn;
  }
  const fs = (s.filters ?? []).filter((f) => f?.tag);
  return {
    combinator: 'AND',
    children: fs.map((f) => ({ tag: f.tag, op: f.op, value: f.value })),
  };
};

/** Recursive WHERE → BydbQL string. depth is used for parens (deeper = more). */
export const qbNodeSQL = (node: QBWhereNode | null | undefined, depth: number): string => {
  if (!node) return '';
  if (qbIsGroup(node)) {
    const items = node.children
      .map((c) => ({ sql: qbNodeSQL(c, depth + 1) }))
      .filter((x) => x.sql);
    if (items.length === 0) return '';
    // AND binds tighter than OR: split into OR-separated AND-runs and
    // parenthesize multi-condition runs so the meaning is explicit.
    const segs = qbConnSegments(node, items, node.children);
    const joined = segs.map((seg) => {
      const j = seg.map((x) => x.sql).join(' AND ');
      return seg.length > 1 ? `(${j})` : j;
    });
    return joined.join(' OR ');
  }
  const leaf = node as QBWhereLeafWithConn;
  if (!leaf.tag || !leaf.op) return '';
  const op = QB_OP(leaf.op);
  return `${leaf.tag} ${op.sql} ${qbQuote(leaf.op, leaf.value)}`;
};

/** Render the time range as a BydbQL TIME clause fragment (or '' when unset).
 *  Per docs/interacting/bydbql.md §2.5.3:
 *    TIME > '-30m'                              relative
 *    TIME BETWEEN '-1h' AND 'now'                relative range
 *    TIME >= '2023-01-01T00:00:00Z'             absolute (single bound)
 *    TIME BETWEEN '2023-01-01' AND '2023-02-01'  absolute range
 */
export const qbTimeSQL = (time: {
  readonly mode: string;
  readonly rel: string;
  readonly from: string;
  readonly to: string;
}): string => {
  if (time.mode === 'relative') {
    if (time.rel && (time.from || time.to)) {
      // ranged relative
      return `TIME BETWEEN '${time.rel}' AND '${time.to || 'now'}'`;
    }
    if (time.rel) return `TIME > '${time.rel}'`;
  }
  if (time.mode === 'absolute') {
    if (time.from && time.to) return `TIME BETWEEN '${time.from}' AND '${time.to}'`;
    if (time.from) return `TIME >= '${time.from}'`;
    if (time.to) return `TIME <= '${time.to}'`;
  }
  return '';
};

// ── Builder state shape (matches the handoff's flat `b` object) ─────────────

export interface QBBuilderState {
  readonly catalog: QB_CATALOG_VALUE;
  readonly group: string;
  readonly resource: string;
  readonly select: readonly { readonly field: string; readonly fn: string }[];
  readonly projection: readonly string[];
  readonly where: QBWhereGroupWithConn;
  readonly groupBy: readonly string[];
  readonly time: {
    readonly mode: 'relative' | 'absolute';
    readonly rel: string;
    readonly from: string;
    readonly to: string;
  };
  readonly orderField: string;
  readonly orderDir: 'ASC' | 'DESC';
  readonly limit: number;
  readonly offset: number;
  readonly trace: boolean;
  readonly topN: number;
  readonly aggFn: string;
  readonly fromAgg: string | null;
  readonly fromResource: string | null;
}

/** Build a BydbQL string from a builder state. Mirrors the handoff's buildBydbQL.
    @param tags Optional list of tag names for the current resource. Used when
    a measure query selects "all tags" (empty projection) so the generated SQL
    can list explicit tags instead of omitting them. */
export const buildBydbQL = (b: QBBuilderState, tags?: readonly string[]): string => {
  const cat = QB_CAT(b.catalog);
  // Top-N is a separate BydbQL shape; route through a different codegen path.
  if (b.catalog === 'topn') {
    return buildTopNBydbQL(b, cat);
  }
  const parts: string[] = [];
  // 1. SELECT projection. Per docs/interacting/bydbql.md line 305/441 the
  //    SELECT clause is required for ALL query types (measures, streams,
  //    traces); "all tags" is expressed as `SELECT *`, not by omitting SELECT.
  //    For measures, mixing `*` with aggregation functions is invalid grammar,
  //    so when "all tags" is chosen (empty projection) we expand to the known
  //    tag list; if tags are unknown we fall back to the original behavior.
  if (b.catalog === 'measures') {
    const projection = b.projection ?? [];
    const tagPart = projection.length
      ? projection.join(', ')
      : (tags?.length ? tags.join(', ') : '');
    const fields = (b.select ?? [])
      .filter((r) => r.field)
      .map((r) => (r.fn ? `${r.fn}(${r.field})` : r.field))
      .join(', ');
    const select = [tagPart, fields].filter(Boolean).join(', ');
    parts.push(`SELECT ${select || '*'}`);
  } else if (b.catalog === 'traces') {
    // BanyanDB's trace query only returns projected tags; SELECT * yields none.
    // Expand to the known tag list so the result view has data to render.
    const projection = b.projection ?? [];
    parts.push(`SELECT ${projection.length ? projection.join(', ') : (tags?.length ? tags.join(', ') : '*')}`);
  } else if ((b.projection ?? []).length) {
    parts.push(`SELECT ${(b.projection ?? []).join(', ')}`);
  } else {
    parts.push('SELECT *');
  }
  // 2. FROM <CATALOG> <resource> IN <group> (grammar lines 48-50).
  parts.push(`FROM ${cat.kw} ${b.resource || '_'}`);
  parts.push(`IN ${b.group || '_'}`);
  // 3. TIME — required for streams/measures/traces per docs/interacting/bydbql.md line 63.
  const t = qbTimeSQL(b.time);
  if (t) parts.push(t);
  // 4. WHERE.
  const where = qbNodeSQL(b.where, 0);
  if (where) parts.push(`WHERE ${where}`);
  // 5. GROUP BY — measures only (grammar line 441).
  if (b.catalog === 'measures' && (b.groupBy ?? []).length) {
    parts.push(`GROUP BY ${(b.groupBy ?? []).join(', ')}`);
  }
  // 6. ORDER BY.
  // BanyanDB's trace analyzer requires either a trace_id filter or an ORDER BY
  // clause. When a trace_id condition is present we skip ORDER BY (the examples
  // in test/cases/trace/data/input/ do the same); otherwise we emit whatever the
  // builder selected. The builder uses 'time' as a generic alias, which for traces
  // maps to the timestamp tag.
  const hasTraceId = qbHasTraceIdCondition(b.where);
  if (b.catalog !== 'traces' || !hasTraceId) {
    const orderField = b.catalog === 'traces' && b.orderField === 'time' ? 'timestamp' : b.orderField;
    if (orderField) parts.push(`ORDER BY ${orderField} ${b.orderDir || 'DESC'}`);
  }
  // 7. WITH QUERY_TRACE. Must appear BEFORE LIMIT/OFFSET in the actual grammar
  // (grammar.go GrammarSelectStatement: Select -> ... -> OrderBy -> WithQueryTrace -> Limit -> Offset).
  if (b.trace) parts.push('WITH QUERY_TRACE');
  // 8. LIMIT.
  if (b.limit && b.limit > 0) parts.push(`LIMIT ${b.limit}`);
  // 8b. OFFSET (server-side paging per docs/interacting/bydbql.md line 305/441).
  if (b.offset && b.offset > 0) parts.push(`OFFSET ${b.offset}`);
  return parts.join('\n');
};

const buildTopNBydbQL = (b: QBBuilderState, cat: QB_CATALOG_DEF): string => {
  const parts: string[] = [];
  // Grammar (docs/interacting/bydbql.md line 620):
  //   topn_query ::= SHOW TOP <n> from_measure_clause TIME time_condition
  //                  [WHERE topn_criteria] [AGGREGATE BY agg_function]
  //                  [ORDER BY value ["ASC"|"DESC"]] [WITH QUERY_TRACE]
  parts.push(`SHOW TOP ${b.topN || 10}`);
  parts.push(`FROM ${cat.kw} ${b.resource || '_'}`);
  parts.push(`IN ${b.group || '_'}`);
  const t = qbTimeSQL(b.time);
  if (t) parts.push(t);
  const where = qbNodeSQL(b.where, 0);
  if (where) parts.push(`WHERE ${where}`);
  if (b.aggFn) parts.push(`AGGREGATE BY ${b.aggFn}`);
  parts.push(`ORDER BY value ${b.orderDir || 'DESC'}`);
  if (b.limit && b.limit > 0) parts.push(`LIMIT ${b.limit}`);
  if (b.offset && b.offset > 0) parts.push(`OFFSET ${b.offset}`);
  if (b.trace) parts.push('WITH QUERY_TRACE');
  return parts.join('\n');
};

// ── WHERE clause helpers used by the builder UI ────────────────────────────

/** A fresh empty WHERE leaf. */
export const qbNewCond = (tags: readonly string[]): QBWhereLeafWithConn => ({
  tag: tags[0] ?? '',
  op: 'BINARY_OP_EQ',
  value: '',
});

/** A fresh group wrapping one empty condition. */
export const qbNewGroup = (tags: readonly string[]): QBWhereGroupWithConn => ({
  combinator: 'AND',
  children: [qbNewCond(tags)],
});

/** An empty root WHERE group. */
export const qbEmptyWhere = (): QBWhereGroupWithConn => ({
  combinator: 'AND',
  children: [],
});

/** True when the WHERE tree contains an equality filter on trace_id with a non-empty value. */
export const qbHasTraceIdFilter = (node: QBWhereNode | null | undefined): boolean => {
  if (!node) return false;
  if (qbIsGroup(node)) {
    return node.children.some((c) => qbHasTraceIdFilter(c));
  }
  const leaf = node as QBWhereLeafWithConn;
  return leaf.tag === 'trace_id' && leaf.op === 'BINARY_OP_EQ' && leaf.value.trim() !== '';
};

/** True when the WHERE tree already has a trace_id equality condition (value may be empty). */
export const qbHasTraceIdCondition = (node: QBWhereNode | null | undefined): boolean => {
  if (!node) return false;
  if (qbIsGroup(node)) {
    return node.children.some((c) => qbHasTraceIdCondition(c));
  }
  const leaf = node as QBWhereLeafWithConn;
  return leaf.tag === 'trace_id' && leaf.op === 'BINARY_OP_EQ';
};

/** True when the WHERE tree has at least one condition with a non-empty value. */
export const qbHasAnyFilter = (node: QBWhereNode | null | undefined): boolean => {
  if (!node) return false;
  if (qbIsGroup(node)) {
    return node.children.some((c) => qbHasAnyFilter(c));
  }
  const leaf = node as QBWhereLeafWithConn;
  return leaf.tag !== '' && leaf.op !== '' && leaf.value.trim() !== '';
};

/** Drop any condition whose tag no longer exists on the chosen resource. */
export const qbPruneWhere = (
  node: QBWhereNode | null | undefined,
  tags: readonly string[],
): QBWhereGroupWithConn => {
  if (!node || !Array.isArray((node as QBWhereGroupWithConn).children)) {
    return qbEmptyWhere();
  }
  const group = node as QBWhereGroupWithConn;
  const pruned: QBWhereNode[] = [];
  for (const c of group.children) {
    if (qbIsGroup(c)) {
      const p = qbPruneWhere(c, tags);
      if (p.children.length > 0) pruned.push(p);
    } else if (tags.includes(c.tag)) {
      pruned.push(c);
    }
  }
  return { ...group, children: pruned as readonly QBWhereLeafWithConn[] };
};

// ── Fuzzy resource search (From-row search box) ─────────────────────────────

/** A resource row in the search index, before scoring. */
export interface QBSearchHitRaw {
  readonly catalog: QB_CATALOG_VALUE;
  readonly group: string;
  readonly resource: string;
  readonly label: string;
}

/** A scored search hit with the matched character indices highlighted. */
export interface QBSearchHit extends QBSearchHitRaw {
  readonly score: number;
  readonly marks: readonly number[];
  readonly gmarks: readonly number[];
}

/** Map of `${dataCatalog}/${groupName}` → resource names for that group. */
export type GroupResourcesMap = ReadonlyMap<string, readonly string[]>;

/** Map BanyanDB's `CATALOG_*` enum to our short catalog key. */
const protoCatalogToData = (c: Group['catalog']): 'measures' | 'streams' | 'traces' | null => {
  switch (c) {
    case 'CATALOG_MEASURE':
      return 'measures';
    case 'CATALOG_STREAM':
      return 'streams';
    case 'CATALOG_TRACE':
      return 'traces';
    default:
      return null;
  }
};

/**
 * Flatten every queryable target (measures / streams / traces / top-n) into one
 * searchable index. Top-N entries are derived per-measure (their schema lives
 * in the measures catalog).
 */
export const qbSearchIndex = (
  groups: readonly Group[],
  groupResources: GroupResourcesMap,
): readonly QBSearchHitRaw[] => {
  const out: QBSearchHitRaw[] = [];
  for (const c of QB_CATALOGS) {
    const src = qbDataCatalog(c.value); // 'topn' → 'measures'
    for (const g of groups) {
      const dataKey = protoCatalogToData(g.catalog);
      if (dataKey !== src) continue;
      const resources = groupResources.get(`${src}/${g.name}`) ?? [];
      for (const r of resources) {
        out.push({ catalog: c.value, group: g.name, resource: r, label: c.label });
      }
    }
  }
  return out;
};

/**
 * Subsequence fuzzy match. Returns null on miss, else `{score, marks}` where
 * `marks` are the matched character indices in `text`. Rewards contiguous runs
 * and word-boundary hits; favours short, early matches.
 */
export const qbFuzzy = (
  query: string,
  text: string,
): { score: number; marks: readonly number[] } | null => {
  const q = (query || '').toLowerCase();
  const t = (text || '').toLowerCase();
  if (!q) return { score: 0, marks: [] };
  let qi = 0;
  let score = 0;
  let prev = -2;
  const marks: number[] = [];
  for (let ti = 0; ti < t.length && qi < q.length; ti++) {
    if (t[ti] === q[qi]) {
      marks.push(ti);
      score += ti === prev + 1 ? 6 : 1; // contiguous run
      if (ti === 0 || /[^a-z0-9]/i.test(t[ti - 1])) score += 4; // word boundary
      prev = ti;
      qi++;
    }
  }
  if (qi < q.length) return null;
  score += Math.max(0, 12 - text.length / 4) + (marks[0] === 0 ? 8 : 0);
  return { score, marks };
};

/** Rank the index against a query. Matches both resource name and group name. */
export const qbSearchResults = (
  index: readonly QBSearchHitRaw[],
  query: string,
  limit: number,
): readonly QBSearchHit[] => {
  const q = (query || '').trim();
  if (!q) return [];
  const scored: QBSearchHit[] = [];
  for (const it of index) {
    const m = qbFuzzy(q, it.resource);
    const gm = qbFuzzy(q, it.group);
    if (!m && !gm) continue;
    scored.push({
      ...it,
      marks: m ? m.marks : [],
      gmarks: !m && gm ? gm.marks : [],
      score: (m ? m.score : 0) + (gm ? gm.score * 0.4 : 0),
    });
  }
  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, limit || 8);
};