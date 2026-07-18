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

// property-bydbql.ts — pure BydbQL codegen/parsing + the builder-state -> the
// property/v1 Query RPC request translation, for the property-scoped query
// console (PropertyQuery.tsx). Ported from
// .handoff-import/banyandb/project/property-query.jsx's non-React parts
// (PROP_OPS, buildPropertyBydbQL, pqParseCode et al) plus a NEW
// pqBuildQueryRequest that the handoff didn't need (it filtered an in-memory
// `entries` array instead of calling a real Query RPC — see
// docs/property-design.md §5).
//
// The WHERE tree reuses bydbql.ts's node shape (QBWhereNode /
// QBWhereGroupWithConn / QBWhereLeafWithConn) verbatim — the handoff's own
// comment on pqNodeSQL says children's `conn` follows "qbConn in
// query-builder.jsx", i.e. property's WHERE tree was always meant to be the
// same shape as the main builder's, just with a different (smaller) operator
// set and a special PQ_ID sentinel tag. Reusing the type avoids a parallel
// tree implementation; qbConnSegments/qbConnSummary/QB_COMBINATORS are
// imported from bydbql.ts rather than re-implemented.

import {
  QB_COMBINATORS, qbIsGroup, qbConn, qbConnSegments,
  type QBWhereNode, type QBWhereGroupWithConn, type QBWhereLeafWithConn,
} from './bydbql.js';
import type { PropertyCriteria, PropertyQueryRequest, PropertyQueryOrder, PropertyTagValue } from 'canopy-shared';

export { QB_COMBINATORS, qbIsGroup, qbConn };
export type { QBWhereNode, QBWhereGroupWithConn, QBWhereLeafWithConn };

/** Sentinel tag identifier for the document id (maps to the request's `ids`,
 *  never to `criteria`). Rendered/parsed as the literal "ID" — mirrors the
 *  handoff's PQ_ID exactly. */
export const PQ_ID = 'ID';

export interface PropOpDef {
  readonly value: string;
  readonly sql: string;
  readonly label: string;
}

/** model.v1.Condition.BinaryOp minus MATCH/HAVING/NOT_HAVING — property
 *  criteria only supports comparison + set membership (see
 *  pkg/query/logical/parser.go's ParseExpr, which property's
 *  BuildPropertyQuery routes through). */
export const PROP_OPS: readonly PropOpDef[] = [
  { value: 'BINARY_OP_EQ', sql: '=', label: 'equals  =' },
  { value: 'BINARY_OP_NE', sql: '!=', label: 'not equals  ≠' },
  { value: 'BINARY_OP_GT', sql: '>', label: 'greater  >' },
  { value: 'BINARY_OP_GE', sql: '>=', label: 'greater or equal  ≥' },
  { value: 'BINARY_OP_LT', sql: '<', label: 'less  <' },
  { value: 'BINARY_OP_LE', sql: '<=', label: 'less or equal  ≤' },
  { value: 'BINARY_OP_IN', sql: 'IN', label: 'in  (a, b)' },
  { value: 'BINARY_OP_NOT_IN', sql: 'NOT IN', label: 'not in  (a, b)' },
];

export const PROP_OP = (v: string): PropOpDef => PROP_OPS.find((o) => o.value === v) ?? PROP_OPS[0];

// ── builder leaf / group factories ──────────────────────────────────────────

export function pqNewCond(): QBWhereLeafWithConn {
  return { tag: PQ_ID, op: 'BINARY_OP_EQ', value: '' };
}
export function pqNewGroup(): QBWhereGroupWithConn {
  return { combinator: 'AND', children: [pqNewCond()] };
}
export function pqWhereRoot(s: { readonly where?: QBWhereNode }): QBWhereGroupWithConn {
  if (s.where && qbIsGroup(s.where)) return s.where;
  return { combinator: 'AND', children: [] };
}

// ── value helpers ────────────────────────────────────────────────────────────

const PQ_NUM = (s: string): boolean => /^-?\d+(\.\d+)?$/.test(String(s).trim());
function pqStrip(v: string | null | undefined): string {
  return String(v == null ? '' : v).trim().replace(/^['"]|['"]$/g, '');
}
function pqQuote(op: string, raw: string | null | undefined): string {
  const value = (raw == null ? '' : String(raw)).trim();
  if (op === 'BINARY_OP_IN' || op === 'BINARY_OP_NOT_IN') {
    const parts = value.split(',').map((x) => x.trim()).filter(Boolean);
    return '(' + parts.map((p) => (PQ_NUM(pqStrip(p)) ? pqStrip(p) : "'" + pqStrip(p) + "'")).join(', ') + ')';
  }
  if (value === '') return "''";
  return PQ_NUM(value) ? value : "'" + pqStrip(value) + "'";
}

// ── BydbQL generation (Property grammar — no catalog keyword, no TIME) ──────

function pqNodeSQL(node: QBWhereNode | null | undefined, depth: number): string {
  if (!node) return '';
  if (qbIsGroup(node)) {
    const items = node.children
      .map((c) => ({ sql: pqNodeSQL(c, depth + 1), node: c }))
      .filter((x) => x.sql);
    if (!items.length) return '';
    const segs = qbConnSegments(node, items);
    const joined = segs.map((seg) => {
      const j = seg.map((x) => x.sql).join(' AND ');
      return segs.length > 1 && seg.length > 1 ? '(' + j + ')' : j;
    }).join(' OR ');
    return depth === 0 ? joined : (items.length > 1 ? '(' + joined + ')' : joined);
  }
  const leaf = node as QBWhereLeafWithConn;
  if (!leaf.tag) return '';
  return leaf.tag + ' ' + PROP_OP(leaf.op).sql + ' ' + pqQuote(leaf.op, leaf.value);
}

export interface PQBuilderState {
  readonly projection: readonly string[];
  readonly where: QBWhereGroupWithConn;
  readonly orderField: string;
  readonly orderDir: 'ASC' | 'DESC';
  readonly limit: number | string;
}

export function pqDefault(): PQBuilderState {
  return { projection: [], where: { combinator: 'AND', children: [] }, orderField: '', orderDir: 'ASC', limit: '' };
}

export function buildPropertyBydbQL(s: PQBuilderState, propName: string, groupName: string): string {
  const proj = (s.projection ?? []).length ? s.projection.join(', ') : '*';
  const lines = [
    `SELECT ${proj}`,
    `FROM PROPERTY ${propName || '<property>'} IN ${groupName || '<group>'}`,
  ];
  const whereExpr = pqNodeSQL(pqWhereRoot(s), 0);
  if (whereExpr) lines.push(`WHERE ${whereExpr}`);
  if (s.orderField) lines.push(`ORDER BY ${s.orderField} ${s.orderDir || 'ASC'}`);
  if (s.limit) lines.push(`LIMIT ${s.limit}`);
  return lines.join('\n') + ';';
}

// ── tolerant parser for raw-code mode (Property subset) ─────────────────────

function pqSplitTop(str: string, kw: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let cur = '';
  let i = 0;
  while (i < str.length) {
    const ch = str[i];
    if (ch === '(') { depth++; cur += ch; i++; continue; }
    if (ch === ')') { depth--; cur += ch; i++; continue; }
    if (depth === 0 && (i === 0 || /\s/.test(str[i - 1]))) {
      const m = new RegExp('^' + kw + '\\b', 'i').exec(str.slice(i));
      if (m) { out.push(cur); cur = ''; i += m[0].length; continue; }
    }
    cur += ch;
    i++;
  }
  out.push(cur);
  return out.map((x) => x.trim()).filter(Boolean);
}

function pqParseCond(seg: string): QBWhereLeafWithConn {
  const m = /^(ID|[A-Za-z_][A-Za-z0-9_.]*)\s*(=|!=|>=|<=|>|<|NOT\s+IN|IN)\s*([\s\S]+)$/i.exec(seg.trim());
  if (!m) throw new Error('Cannot parse condition: ' + seg.trim());
  const tag = m[1].toUpperCase() === 'ID' ? PQ_ID : m[1];
  const opTxt = m[2].toUpperCase().replace(/\s+/g, ' ');
  const opMap: Record<string, string> = {
    '=': 'BINARY_OP_EQ', '!=': 'BINARY_OP_NE', '>': 'BINARY_OP_GT', '>=': 'BINARY_OP_GE',
    '<': 'BINARY_OP_LT', '<=': 'BINARY_OP_LE', IN: 'BINARY_OP_IN', 'NOT IN': 'BINARY_OP_NOT_IN',
  };
  const op = opMap[opTxt] ?? 'BINARY_OP_EQ';
  const raw = m[3].trim();
  let value: string;
  if (op === 'BINARY_OP_IN' || op === 'BINARY_OP_NOT_IN') {
    value = raw.replace(/^\(|\)$/g, '').split(',').map((x) => pqStrip(x)).filter(Boolean).join(', ');
  } else {
    value = pqStrip(raw);
  }
  return { tag, op, value };
}

function pqParseWhere(text: string): QBWhereGroupWithConn {
  const t = (text ?? '').trim();
  if (!t) return { combinator: 'AND', children: [] };
  const ors = pqSplitTop(t, 'OR');
  const orChildren: QBWhereNode[] = ors.map((seg) => {
    const ands = pqSplitTop(seg, 'AND');
    const kids: QBWhereNode[] = ands.map((a) => {
      const x = a.trim();
      if (x.startsWith('(') && x.endsWith(')')) return pqParseWhere(x.slice(1, -1));
      return pqParseCond(x);
    });
    return kids.length === 1 ? kids[0] : { combinator: 'AND', children: kids };
  });
  if (orChildren.length === 1) {
    const only = orChildren[0];
    return qbIsGroup(only) ? only : { combinator: 'AND', children: [only] };
  }
  return { combinator: 'OR', children: orChildren };
}

/** Parse a PROPERTY-flavored BydbQL string (the subset buildPropertyBydbQL
 *  emits, and the shape used by test/cases/property/data/input/*.ql) back
 *  into a PQBuilderState — code-mode Run() calls this so hand-edited queries
 *  still execute. */
export function pqParseCode(code: string): PQBuilderState {
  const src = (code ?? '').replace(/;?\s*$/, '').replace(/\bWITH\s+QUERY_TRACE\b/i, '').trim();
  if (!/from\s+property/i.test(src)) throw new Error('Expected FROM PROPERTY …');
  const selM = /select\s+([\s\S]*?)\s+from\s+property/i.exec(src);
  const projRaw = selM ? selM[1].trim() : '*';
  const projection = projRaw === '*' ? [] : projRaw.split(',').map((x) => x.trim()).filter((x) => x && x.toUpperCase() !== PQ_ID);
  const whereM = /\bwhere\b([\s\S]*?)(?:\border\s+by\b|\blimit\b|$)/i.exec(src);
  const where = whereM ? pqParseWhere(whereM[1]) : { combinator: 'AND' as const, children: [] };
  const ordM = /\border\s+by\s+(ID|[A-Za-z_][A-Za-z0-9_.]*)\s*(ASC|DESC)?/i.exec(src);
  const orderField = ordM ? (ordM[1].toUpperCase() === 'ID' ? PQ_ID : ordM[1]) : '';
  const orderDir = (ordM && ordM[2] ? ordM[2].toUpperCase() : 'ASC') as 'ASC' | 'DESC';
  const limM = /\blimit\s+(\d+)/i.exec(src);
  const limit = limM ? parseInt(limM[1], 10) : '';
  return { projection, where, orderField, orderDir, limit };
}

// ── builder-state -> structured property/v1 Query request translation ──────
// (docs/property-design.md §5 table.) The Query RPC, not BydbQL, is what
// actually executes — the Code tab is display-only in v1 (§5 note).

/** Encode a leaf's raw text value into a model.v1.TagValue-shaped Criteria
 *  condition value: IN/NOT_IN need an array-typed value (see
 *  pkg/query/logical/parser.go's ParseExpr, which requires TagValue_StrArray
 *  / TagValue_IntArray for BINARY_OP_IN); everything else is a scalar. */
function pqConditionValue(op: string, raw: string): PropertyTagValue {
  const value = (raw ?? '').trim();
  if (op === 'BINARY_OP_IN' || op === 'BINARY_OP_NOT_IN') {
    const parts = value.split(',').map((x) => pqStrip(x)).filter(Boolean);
    return parts.length && parts.every(PQ_NUM)
      ? { intArray: { value: parts } }
      : { strArray: { value: parts } };
  }
  return PQ_NUM(value) ? { int: { value } } : { str: { value } };
}

/** Collect every ID-tag EQ/IN leaf's value(s), anywhere in the tree, into a
 *  flat deduplicated list for the request's `ids` field — regardless of
 *  AND/OR nesting. This is a documented simplification: `ids` (server-side,
 *  effectively OR-matched) and `criteria` (the remaining tree) are both
 *  ultimately ANDed together by the request shape, so a WHERE tree that ORs
 *  an id filter against unrelated tag conditions cannot be represented
 *  exactly — the id restriction still applies. Straightforward trees (id
 *  filters not mixed with OR against other tags — the common case, matching
 *  the property .ql corpus) translate exactly. */
function pqExtractIds(node: QBWhereNode | null | undefined, out: Set<string>): void {
  if (!node) return;
  if (qbIsGroup(node)) {
    for (const c of node.children) pqExtractIds(c, out);
    return;
  }
  const leaf = node as QBWhereLeafWithConn;
  if (leaf.tag !== PQ_ID) return;
  if (leaf.op === 'BINARY_OP_EQ') {
    const v = pqStrip(leaf.value);
    if (v) out.add(v);
  } else if (leaf.op === 'BINARY_OP_IN') {
    for (const p of leaf.value.split(',')) {
      const v = pqStrip(p);
      if (v) out.add(v);
    }
  }
}

/** Build the `criteria` tree from every non-ID leaf, preserving the same
 *  "AND binds tighter than OR" grouping used for BydbQL text rendering — but
 *  as an actual nested model.v1.Criteria (LogicalExpression) tree, not text. */
function pqBuildCriteria(node: QBWhereNode | null | undefined): PropertyCriteria | undefined {
  if (!node) return undefined;
  if (qbIsGroup(node)) {
    const items = node.children
      .map((c) => ({ criteria: pqBuildCriteria(c), node: c }))
      .filter((x): x is { criteria: PropertyCriteria; node: QBWhereNode } => x.criteria !== undefined);
    if (!items.length) return undefined;
    const segs = qbConnSegments(node, items);
    const orParts: PropertyCriteria[] = [];
    for (const seg of segs) {
      const anded = seg.reduce<PropertyCriteria | undefined>((acc, item) => {
        if (!acc) return item.criteria;
        return { le: { op: 'LOGICAL_OP_AND', left: acc, right: item.criteria } };
      }, undefined);
      if (anded) orParts.push(anded);
    }
    if (!orParts.length) return undefined;
    return orParts.reduce((acc, part) => (acc ? { le: { op: 'LOGICAL_OP_OR', left: acc, right: part } } : part));
  }
  const leaf = node as QBWhereLeafWithConn;
  if (!leaf.tag || leaf.tag === PQ_ID) return undefined;
  return { condition: { name: leaf.tag, op: leaf.op, value: pqConditionValue(leaf.op, leaf.value) } };
}

/** Translate builder state (or a parsed code-mode state) into the
 *  property/v1 QueryRequest that queryPropertyDocuments actually sends. */
export function pqBuildQueryRequest(s: PQBuilderState, group: string, name: string): PropertyQueryRequest {
  const where = pqWhereRoot(s);
  const ids = new Set<string>();
  pqExtractIds(where, ids);
  const criteria = pqBuildCriteria(where);
  const limit = typeof s.limit === 'number' ? s.limit : parseInt(String(s.limit), 10);
  let orderBy: PropertyQueryOrder | undefined;
  if (s.orderField) {
    // Best-effort: ordering by the document id maps to the internal `_id`
    // index field (see banyand/property/db/shard.go's idField constant) —
    // there is no documented public alias for it. Flagged as uncertain.
    orderBy = {
      tagName: s.orderField === PQ_ID ? '_id' : s.orderField,
      sort: s.orderDir === 'ASC' ? 'SORT_ASC' : 'SORT_DESC',
    };
  }
  return {
    groups: [group],
    name,
    ...(ids.size ? { ids: [...ids] } : {}),
    ...(criteria ? { criteria } : {}),
    ...((s.projection ?? []).length ? { tagProjection: [...s.projection] } : {}),
    ...(Number.isFinite(limit) && limit > 0 ? { limit } : {}),
    ...(orderBy ? { orderBy } : {}),
  };
}
