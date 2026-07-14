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

import { describe, it, expect } from 'vitest';
import {
  QB_CAT, QB_OP, QB_OPS, QB_CATALOGS, qbDataCatalog,
  buildBydbQL, qbNodeSQL, qbQuote, qbTimeSQL,
  qbNewCond, qbEmptyWhere, qbPruneWhere, qbConnSummary,
  type QBBuilderState, type QBWhereGroupWithConn, type QBWhereLeafWithConn,
} from './bydbql.js';

const baseState: QBBuilderState = {
  catalog: 'measures',
  group: 'g1',
  resource: 'cpu',
  select: [],
  projection: ['host_id'],
  where: qbEmptyWhere(),
  groupBy: [],
  time: { mode: 'relative', rel: '-30m', from: '', to: '' },
  orderField: 'time',
  orderDir: 'DESC',
  limit: 100,
  offset: 0,
  trace: false,
  topN: 10,
  aggFn: '',
  fromAgg: null,
  fromResource: null,
};

describe('qbDataCatalog', () => {
  it('maps topn to measures (data lives in measure)', () => {
    expect(qbDataCatalog('topn')).toBe('measures');
  });
  it('passes through measures / streams / traces', () => {
    expect(qbDataCatalog('measures')).toBe('measures');
    expect(qbDataCatalog('streams')).toBe('streams');
    expect(qbDataCatalog('traces')).toBe('traces');
  });
});

describe('QB vocabulary', () => {
  it('QB_CAT returns the right catalog', () => {
    expect(QB_CAT('topn').kw).toBe('MEASURE');
    expect(QB_CAT('streams').kw).toBe('STREAM');
  });
  it('QB_OP maps every BinaryOp to its sql fragment', () => {
    for (const op of QB_OPS) {
      expect(QB_OP(op.value).sql).toBe(op.sql);
    }
  });
  it('QB_CATALOGS has 4 entries (measures/streams/traces/topn)', () => {
    expect(QB_CATALOGS.map((c) => c.value).sort()).toEqual(['measures', 'streams', 'topn', 'traces']);
  });
});

describe('qbQuote', () => {
  it('quotes strings and leaves numbers bare', () => {
    expect(qbQuote('BINARY_OP_EQ', 'hello')).toBe("'hello'");
    expect(qbQuote('BINARY_OP_EQ', '42')).toBe('42');
    expect(qbQuote('BINARY_OP_EQ', '-3.14')).toBe('-3.14');
  });
  it('expands IN / NOT IN to a parenthesized list', () => {
    expect(qbQuote('BINARY_OP_IN', 'a, b, 1')).toBe("('a', 'b', 1)");
    expect(qbQuote('BINARY_OP_NOT_IN', 'x')).toBe("('x')");
  });
  it('renders empty values as empty-string literal', () => {
    expect(qbQuote('BINARY_OP_EQ', '')).toBe("''");
  });
});

describe('qbNodeSQL', () => {
  it('renders a flat AND WHERE (parens wrap a multi-condition AND segment per handoff semantics)', () => {
    // The handoff joins multi-item segments with AND and parenthesizes them
    // to make precedence explicit when mixed with OR segments elsewhere.
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [
        { tag: 'host_id', op: 'BINARY_OP_EQ', value: 'h1' },
        { tag: 'region', op: 'BINARY_OP_EQ', value: 'us' },
      ],
    };
    expect(qbNodeSQL(where, 0)).toBe("(host_id = 'h1' AND region = 'us')");
  });
  it('joins an OR group of single-condition segments with OR', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'OR',
      children: [
        { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
        { tag: 'b', op: 'BINARY_OP_EQ', value: '2' },
      ],
    };
    expect(qbNodeSQL(where, 0)).toBe('a = 1 OR b = 2');
  });
  it('per-child conn=OR starts a new OR segment, conn=AND (or absent) continues the AND run', () => {
    // The handoff segments the children by per-child `conn`: each OR begins
    // a new segment; each AND (or absent, falling back to the group combinator)
    // appends to the current segment. Segments are joined by OR.
    const mixed: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [
        { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
        { tag: 'b', op: 'BINARY_OP_EQ', value: '2', conn: 'OR' },
        { tag: 'c', op: 'BINARY_OP_EQ', value: '3' },
      ],
    };
    expect(qbNodeSQL(mixed, 0)).toBe('a = 1 OR (b = 2 AND c = 3)');
  });
  it('drops empty leaves (no tag)', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [
        { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
        { tag: '', op: 'BINARY_OP_EQ', value: '' },
      ],
    };
    // After dropping the empty leaf, the segment has one item — no parens.
    expect(qbNodeSQL(where, 0)).toBe('a = 1');
  });
});

describe('qbTimeSQL', () => {
  it('renders relative time as a quoted duration', () => {
    expect(qbTimeSQL({ mode: 'relative', rel: '-5m', from: '', to: '' })).toBe("TIME > '-5m'");
  });
  it('renders absolute range with BETWEEN', () => {
    expect(qbTimeSQL({ mode: 'absolute', rel: '', from: '2026-01-01', to: '2026-02-01' }))
      .toBe("TIME BETWEEN '2026-01-01' AND '2026-02-01'");
  });
  it('renders one-sided absolute bounds', () => {
    expect(qbTimeSQL({ mode: 'absolute', rel: '', from: '2026-01-01', to: '' })).toBe("TIME >= '2026-01-01'");
    expect(qbTimeSQL({ mode: 'absolute', rel: '', from: '', to: '2026-02-01' })).toBe("TIME <= '2026-02-01'");
  });
  it('returns empty for all-time', () => {
    expect(qbTimeSQL({ mode: 'relative', rel: '', from: '', to: '' })).toBe('');
  });
});

describe('buildBydbQL', () => {
  it('builds a measure query with SELECT tags + field aggregate', () => {
    const s: QBBuilderState = {
      ...baseState,
      select: [{ field: 'value', fn: 'MEAN' }],
    };
    const out = buildBydbQL(s);
    expect(out).toContain('MEASURE cpu');
    expect(out).toContain('IN g1');
    expect(out).toContain('SELECT host_id, MEAN(value)');
    expect(out).toContain("TIME > '-30m'");
    expect(out).toContain('ORDER BY time DESC');
    expect(out).toContain('LIMIT 100');
  });
  it('expands "all tags" to explicit tag list for measures with aggregation', () => {
    // `SELECT *, MEAN(total)` is invalid BydbQL, so empty projection must be
    // expanded to the known tag list when tags are supplied.
    const s: QBBuilderState = {
      ...baseState,
      projection: [],
      select: [{ field: 'value', fn: 'MEAN' }],
    };
    const out = buildBydbQL(s, ['host_id', 'region']);
    expect(out).toContain('SELECT host_id, region, MEAN(value)');
  });
  it('builds a stream query with SELECT * (all tags)', () => {
    const s: QBBuilderState = { ...baseState, catalog: 'streams', projection: [], resource: 'logs' };
    expect(buildBydbQL(s)).toContain('STREAM logs');
    // Per grammar line 305, SELECT is required even for "all columns" — emitted as `SELECT *`.
    expect(buildBydbQL(s)).toContain('SELECT *');
  });
  it('builds a stream query with projected tags', () => {
    const s: QBBuilderState = { ...baseState, catalog: 'streams', projection: ['level', 'msg'], resource: 'logs' };
    expect(buildBydbQL(s)).toContain('SELECT level, msg');
  });
  it('builds a trace query with WITH QUERY_TRACE', () => {
    const s: QBBuilderState = { ...baseState, catalog: 'traces', projection: ['span_id'], resource: 'spans', trace: true };
    expect(buildBydbQL(s)).toContain('TRACE spans');
    expect(buildBydbQL(s)).toContain('WITH QUERY_TRACE');
  });
  it('orders trace queries by timestamp when no trace_id filter is present', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [{ tag: 'service', op: 'BINARY_OP_EQ', value: 'gateway' }],
    };
    const s: QBBuilderState = { ...baseState, catalog: 'traces', resource: 'spans', where };
    const out = buildBydbQL(s);
    expect(out).toContain('WHERE service = \'gateway\'');
    expect(out).toContain('ORDER BY timestamp DESC');
  });
  it('skips ORDER BY for trace queries when a trace_id filter is present', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [{ tag: 'trace_id', op: 'BINARY_OP_EQ', value: 't1' }],
    };
    const s: QBBuilderState = { ...baseState, catalog: 'traces', resource: 'spans', where };
    const out = buildBydbQL(s);
    expect(out).toContain('WHERE trace_id = \'t1\'');
    expect(out).not.toContain('ORDER BY');
  });
  it('places WITH QUERY_TRACE before LIMIT/OFFSET to match the parser grammar', () => {
    const s: QBBuilderState = {
      ...baseState,
      catalog: 'measures',
      projection: ['host_id'],
      resource: 'cpu',
      trace: true,
      limit: 50,
      offset: 10,
    };
    const out = buildBydbQL(s);
    const withIdx = out.indexOf('WITH QUERY_TRACE');
    const limitIdx = out.indexOf('LIMIT 50');
    const offsetIdx = out.indexOf('OFFSET 10');
    expect(withIdx).toBeGreaterThan(-1);
    expect(limitIdx).toBeGreaterThan(withIdx);
    expect(offsetIdx).toBeGreaterThan(withIdx);
  });
  it('builds a Top-N query with TOP / AGGREGATE BY / ORDER BY value', () => {
    const s: QBBuilderState = {
      ...baseState,
      catalog: 'topn',
      projection: [],
      select: [],
      topN: 5,
      aggFn: 'MEAN',
      time: { mode: 'relative', rel: '-1h', from: '', to: '' },
    };
    const out = buildBydbQL(s);
    expect(out).toContain('TOP 5');
    expect(out).toContain('AGGREGATE BY MEAN');
    expect(out).toContain('ORDER BY value DESC');
  });
  it('renders a WHERE clause when conditions exist', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [{ tag: 'host_id', op: 'BINARY_OP_EQ', value: 'h1' }],
    };
    const s: QBBuilderState = { ...baseState, where };
    expect(buildBydbQL(s)).toContain("WHERE host_id = 'h1'");
  });
  it('does not invent routes or features outside the M4 handoff table', () => {
    // sanity: TopN data path uses a separate generation function
    const s: QBBuilderState = { ...baseState, catalog: 'topn', projection: [], select: [], topN: 3 };
    const out = buildBydbQL(s);
    expect(out).toContain('TOP 3');
    // Top-N starts with `SHOW TOP`, not `SELECT` — see line 620 grammar.
    expect(out.startsWith('SHOW TOP')).toBe(true);
  });
  it('starts with SELECT (or SHOW for Top-N) per BydbQL grammar', () => {
    // Regression: BanyanDB's parser rejects queries that start with MEASURE/STREAM/TRACE
    // (the handoff's buildBydbQL had this bug; see docs/interacting/bydbql.md line 305/441/620).
    const cases: Array<{ catalog: QBBuilderState['catalog']; firstToken: string }> = [
      { catalog: 'measures', firstToken: 'SELECT' },
      { catalog: 'streams', firstToken: 'SELECT' },
      { catalog: 'traces', firstToken: 'SELECT' },
      { catalog: 'topn', firstToken: 'SHOW' },
    ];
    for (const { catalog, firstToken } of cases) {
      const s: QBBuilderState = {
        ...baseState,
        catalog,
        projection: catalog === 'measures' ? ['host_id'] : [],
        select: [],
        ...(catalog === 'topn' ? { topN: 5, aggFn: 'MEAN' } : {}),
      };
      const out = buildBydbQL(s);
      const firstLine = out.split('\n').find((l) => l.trim().length > 0) ?? '';
      expect(firstLine.startsWith(firstToken)).toBe(true);
    }
  });
  it('orders clauses SELECT / FROM / TIME / WHERE / GROUP BY / ORDER BY / LIMIT', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [{ tag: 'host_id', op: 'BINARY_OP_EQ', value: 'h1' }],
    };
    const s: QBBuilderState = {
      ...baseState,
      select: [{ field: 'value', fn: 'MEAN' }],
      groupBy: ['host_id'],
      where,
    };
    const out = buildBydbQL(s);
    // Tokenize each non-empty line's leading keyword; assert the sequence.
    const order: string[] = [];
    for (const line of out.split('\n')) {
      const kw = line.trim().split(/\s+/)[0];
      if (kw) order.push(kw);
    }
    expect(order).toEqual(['SELECT', 'FROM', 'IN', 'TIME', 'WHERE', 'GROUP', 'ORDER', 'LIMIT']);
  });
});

describe('WHERE tree helpers', () => {
  it('qbNewCond defaults to first tag + BINARY_OP_EQ', () => {
    const c = qbNewCond(['a', 'b']);
    expect(c.tag).toBe('a');
    expect(c.op).toBe('BINARY_OP_EQ');
    expect(c.value).toBe('');
  });
  it('qbEmptyWhere has no children', () => {
    const e = qbEmptyWhere();
    expect(e.combinator).toBe('AND');
    expect(e.children).toHaveLength(0);
  });
  it('qbPruneWhere drops leaves whose tag is not in the tag set', () => {
    const where: QBWhereGroupWithConn = {
      combinator: 'AND',
      children: [
        { tag: 'keep', op: 'BINARY_OP_EQ', value: '1' },
        { tag: 'gone', op: 'BINARY_OP_EQ', value: '2' },
      ],
    };
    const p = qbPruneWhere(where, ['keep']);
    expect(p.children.map((c) => (c as QBWhereLeafWithConn).tag)).toEqual(['keep']);
  });
  it('qbConnSummary reports AND / OR / mixed', () => {
    expect(qbConnSummary({ combinator: 'AND', children: [] })).toBe('AND');
    expect(qbConnSummary({ combinator: 'AND', children: [
      { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
      { tag: 'b', op: 'BINARY_OP_EQ', value: '2' },
    ] })).toBe('AND');
    // Mixed requires two siblings joined by DIFFERENT connectors.
    expect(qbConnSummary({ combinator: 'AND', children: [
      { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
      { tag: 'b', op: 'BINARY_OP_EQ', value: '2', conn: 'OR' },
      { tag: 'c', op: 'BINARY_OP_EQ', value: '3' },
    ] })).toBe('mixed');
  });
});