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

/** Render the time range as a BydbQL TIME clause fragment (or '' when unset). */
export const qbTimeSQL = (time: {
  readonly mode: string;
  readonly rel: string;
  readonly from: string;
  readonly to: string;
}): string => {
  if (time.mode === 'relative' && time.rel) return `TIME > NOW() ${time.rel}`;
  if (time.mode === 'absolute') {
    if (time.from && time.to) return `TIME >= '${time.from}' AND TIME <= '${time.to}'`;
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
  readonly trace: boolean;
  readonly topN: number;
  readonly aggFn: string;
  readonly fromAgg: string | null;
  readonly fromResource: string | null;
}

/** Build a BydbQL string from a builder state. Mirrors the handoff's buildBydbQL. */
export const buildBydbQL = (b: QBBuilderState): string => {
  const cat = QB_CAT(b.catalog);
  // Top-N is a separate BydbQL shape; route through a different codegen path.
  if (b.catalog === 'topn') {
    return buildTopNBydbQL(b, cat);
  }
  const parts: string[] = [];
  parts.push(`${cat.kw} ${b.resource || '_'}`);
  parts.push(`IN ${b.group || '_'}`);
  // SELECT (measures): tags + optional fields with aggregation
  if (b.catalog === 'measures') {
    const tags = (b.projection ?? []).join(', ');
    const fields = (b.select ?? [])
      .filter((r) => r.field)
      .map((r) => (r.fn ? `${r.fn}(${r.field})` : r.field))
      .join(', ');
    const select = [tags, fields].filter(Boolean).join(', ');
    parts.push(`SELECT ${select || '*'}`);
    if ((b.groupBy ?? []).length) parts.push(`GROUP BY ${(b.groupBy ?? []).join(', ')}`);
  } else if ((b.projection ?? []).length) {
    parts.push(`SELECT ${(b.projection ?? []).join(', ')}`);
  }
  const where = qbNodeSQL(b.where, 0);
  if (where) parts.push(`WHERE ${where}`);
  const t = qbTimeSQL(b.time);
  if (t) parts.push(t);
  if (b.orderField) parts.push(`ORDER BY ${b.orderField} ${b.orderDir || 'DESC'}`);
  if (b.limit && b.limit > 0) parts.push(`LIMIT ${b.limit}`);
  if (b.trace) parts.push('WITH QUERY_TRACE');
  return parts.join('\n');
};

const buildTopNBydbQL = (b: QBBuilderState, cat: QB_CATALOG_DEF): string => {
  const parts: string[] = [];
  parts.push(`${cat.kw} ${b.resource || '_'}`);
  parts.push(`IN ${b.group || '_'}`);
  parts.push(`TOP ${b.topN || 10}`);
  if (b.aggFn) parts.push(`AGGREGATE BY ${b.aggFn}`);
  const where = qbNodeSQL(b.where, 0);
  if (where) parts.push(`WHERE ${where}`);
  const t = qbTimeSQL(b.time);
  if (t) parts.push(t);
  parts.push(`ORDER BY value ${b.orderDir || 'DESC'}`);
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