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

// property-bydbql.test.ts — coverage for buildPropertyBydbQL / pqParseCode
// (round-trip, and against the BanyanDB property BydbQL conformance corpus at
// test/cases/property/data/input/*.ql — transcribed here as literal strings,
// same convention as where.test.ts) and the NEW builder-state -> property/v1
// Query request translation (pqBuildQueryRequest), cross-checked against the
// corpus's *.yaml expected-request fixtures (test/cases/property/data/input/*.yaml).

import { describe, it, expect } from 'vitest';
import {
  PQ_ID, PROP_OPS, pqNewCond, pqWhereRoot, pqDefault,
  buildPropertyBydbQL, pqParseCode, pqBuildQueryRequest,
  type PQBuilderState,
} from './property-bydbql.js';

describe('PROP_OPS', () => {
  it('excludes MATCH/HAVING/NOT_HAVING (property criteria only supports comparison + set membership)', () => {
    const values = PROP_OPS.map((o) => o.value);
    expect(values).toEqual([
      'BINARY_OP_EQ', 'BINARY_OP_NE', 'BINARY_OP_GT', 'BINARY_OP_GE',
      'BINARY_OP_LT', 'BINARY_OP_LE', 'BINARY_OP_IN', 'BINARY_OP_NOT_IN',
    ]);
  });
});

describe('buildPropertyBydbQL', () => {
  it('renders SELECT * with no WHERE/ORDER/LIMIT for an empty state (test/cases/property/data/input/all.ql)', () => {
    const s = pqDefault();
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw;');
  });

  it('renders projected tags (all.ql select list)', () => {
    const s: PQBuilderState = { ...pqDefault(), projection: ['menu_name', 'configuration', 'update_time'] };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT menu_name, configuration, update_time\nFROM PROPERTY ui_menu IN sw;');
  });

  it('renders a WHERE id = leaf (query_by_ids.ql, modulo the ID sentinel case — see PQ_ID doc)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: PQ_ID, op: 'BINARY_OP_EQ', value: '2' }] },
    };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nWHERE ID = 2;');
  });

  it('renders a WHERE tag = leaf (query_by_criteria.ql)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test1' }] },
    };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe("SELECT *\nFROM PROPERTY ui_menu IN sw\nWHERE menu_name = 'test1';");
  });

  it('renders WHERE + ORDER BY (query_with_order.ql)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test2' }] },
      orderField: 'update_time',
      orderDir: 'ASC',
    };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe("SELECT *\nFROM PROPERTY ui_menu IN sw\nWHERE menu_name = 'test2'\nORDER BY update_time ASC;");
  });

  it('renders ORDER BY ASC/DESC with no WHERE (order_by_asc.ql / order_by_desc.ql)', () => {
    const asc: PQBuilderState = { ...pqDefault(), orderField: 'update_time', orderDir: 'ASC' };
    const desc: PQBuilderState = { ...pqDefault(), orderField: 'update_time', orderDir: 'DESC' };
    expect(buildPropertyBydbQL(asc, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nORDER BY update_time ASC;');
    expect(buildPropertyBydbQL(desc, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nORDER BY update_time DESC;');
  });

  it('renders ORDER BY + LIMIT (order_by_with_limit.ql)', () => {
    const s: PQBuilderState = { ...pqDefault(), orderField: 'update_time', orderDir: 'DESC', limit: 1 };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nORDER BY update_time DESC\nLIMIT 1;');
  });

  it('renders LIMIT with no WHERE/ORDER (limit.ql)', () => {
    const s: PQBuilderState = { ...pqDefault(), limit: 1 };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nLIMIT 1;');
  });

  it('renders projection + ORDER BY with no WHERE (order_without_projection.ql)', () => {
    const s: PQBuilderState = { ...pqDefault(), projection: ['menu_name', 'configuration'], orderField: 'update_time', orderDir: 'ASC' };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT menu_name, configuration\nFROM PROPERTY ui_menu IN sw\nORDER BY update_time ASC;');
  });

  it('renders IN / NOT IN membership lists', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'menu_name', op: 'BINARY_OP_IN', value: 'a, b, 3' }] },
    };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe("SELECT *\nFROM PROPERTY ui_menu IN sw\nWHERE menu_name IN ('a', 'b', 3);");
  });

  it('parenthesizes an AND-run inside an OR (AND binds tighter than OR)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: {
        combinator: 'OR',
        children: [
          { tag: 'a', op: 'BINARY_OP_EQ', value: '1' },
          { tag: 'b', op: 'BINARY_OP_EQ', value: '2', conn: 'OR' },
          { tag: 'c', op: 'BINARY_OP_EQ', value: '3', conn: 'AND' },
        ],
      },
    };
    expect(buildPropertyBydbQL(s, 'ui_menu', 'sw')).toBe('SELECT *\nFROM PROPERTY ui_menu IN sw\nWHERE a = 1 OR (b = 2 AND c = 3);');
  });
});

describe('pqParseCode', () => {
  const parseAndCheck = (code: string, expected: Partial<PQBuilderState>) => {
    const parsed = pqParseCode(code);
    expect(parsed).toMatchObject(expected);
  };

  it('parses all.ql (SELECT list, no WHERE/ORDER/LIMIT)', () => {
    parseAndCheck('SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw', {
      projection: ['menu_name', 'configuration', 'update_time'],
      orderField: '',
      limit: '',
    });
  });

  it('parses query_by_ids.ql (WHERE id = ...)', () => {
    const parsed = pqParseCode("SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw\nWHERE id = '2'");
    expect(pqWhereRoot(parsed).children).toEqual([{ tag: PQ_ID, op: 'BINARY_OP_EQ', value: '2' }]);
  });

  it('parses query_by_criteria.ql (WHERE tag = value)', () => {
    const parsed = pqParseCode("SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw\nWHERE menu_name = 'test1'");
    expect(pqWhereRoot(parsed).children).toEqual([{ tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test1' }]);
  });

  it('parses query_with_order.ql (WHERE + ORDER BY)', () => {
    const parsed = pqParseCode("SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw\nWHERE menu_name = 'test2'\nORDER BY update_time ASC");
    expect(pqWhereRoot(parsed).children).toEqual([{ tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test2' }]);
    expect(parsed.orderField).toBe('update_time');
    expect(parsed.orderDir).toBe('ASC');
  });

  it('parses order_by_with_limit.ql (ORDER BY + LIMIT, no WHERE)', () => {
    parseAndCheck('SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw ORDER BY update_time DESC LIMIT 1', {
      orderField: 'update_time', orderDir: 'DESC', limit: 1,
    });
  });

  it('parses limit.ql (LIMIT, no WHERE/ORDER)', () => {
    parseAndCheck('SELECT menu_name, configuration, update_time FROM PROPERTY ui_menu IN sw\nLIMIT 1', { limit: 1, orderField: '' });
  });

  it('parses order_without_projection.ql (projection + ORDER BY, no WHERE)', () => {
    parseAndCheck('SELECT menu_name, configuration FROM PROPERTY ui_menu IN sw ORDER BY update_time ASC', {
      projection: ['menu_name', 'configuration'], orderField: 'update_time', orderDir: 'ASC',
    });
  });

  it('throws a readable error for a non-PROPERTY query', () => {
    expect(() => pqParseCode('SELECT * FROM MEASURE m IN g')).toThrow(/PROPERTY/);
  });

  it('round-trips through buildPropertyBydbQL for a builder-authored state', () => {
    const original: PQBuilderState = {
      projection: ['menu_name'],
      where: { combinator: 'AND', children: [pqNewCond()] },
      orderField: 'update_time',
      orderDir: 'DESC',
      limit: 5,
    };
    const generated = buildPropertyBydbQL(original, 'ui_menu', 'sw');
    const reparsed = pqParseCode(generated);
    expect(buildPropertyBydbQL(reparsed, 'ui_menu', 'sw')).toBe(generated);
  });
});

describe('pqBuildQueryRequest', () => {
  it('translates an empty state to the bare groups/name request (all.ql / all.yaml)', () => {
    const req = pqBuildQueryRequest(pqDefault(), 'sw', 'ui_menu');
    expect(req).toEqual({ groups: ['sw'], name: 'ui_menu' });
  });

  it('adds tagProjection when SELECT chips are chosen (all.yaml)', () => {
    const s: PQBuilderState = { ...pqDefault(), projection: ['menu_name', 'configuration', 'update_time'] };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.tagProjection).toEqual(['menu_name', 'configuration', 'update_time']);
  });

  it('maps WHERE id = leaf to `ids`, not `criteria` (query_by_ids.yaml)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      projection: ['menu_name', 'configuration', 'update_time'],
      where: { combinator: 'AND', children: [{ tag: PQ_ID, op: 'BINARY_OP_EQ', value: '2' }] },
    };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.ids).toEqual(['2']);
    expect(req.criteria).toBeUndefined();
  });

  it('maps a WHERE tag leaf to `criteria` as a Condition (query_by_criteria.yaml)', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      projection: ['menu_name', 'configuration', 'update_time'],
      where: { combinator: 'AND', children: [{ tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test1' }] },
    };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.ids).toBeUndefined();
    expect(req.criteria).toEqual({
      condition: { name: 'menu_name', op: 'BINARY_OP_EQ', value: { str: { value: 'test1' } } },
    });
  });

  it('encodes a numeric-looking condition value as an int TagValue', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'total', op: 'BINARY_OP_GT', value: '5' }] },
    };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.criteria).toEqual({ condition: { name: 'total', op: 'BINARY_OP_GT', value: { int: { value: '5' } } } });
  });

  it('encodes IN with an intArray when every value is numeric, else strArray', () => {
    const numeric: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'total', op: 'BINARY_OP_IN', value: '1, 2, 3' }] },
    };
    const mixed: PQBuilderState = {
      ...pqDefault(),
      where: { combinator: 'AND', children: [{ tag: 'name', op: 'BINARY_OP_IN', value: 'a, 2, c' }] },
    };
    expect(pqBuildQueryRequest(numeric, 'sw', 'ui_menu').criteria).toEqual({
      condition: { name: 'total', op: 'BINARY_OP_IN', value: { intArray: { value: ['1', '2', '3'] } } },
    });
    expect(pqBuildQueryRequest(mixed, 'sw', 'ui_menu').criteria).toEqual({
      condition: { name: 'name', op: 'BINARY_OP_IN', value: { strArray: { value: ['a', '2', 'c'] } } },
    });
  });

  it('maps ORDER BY to orderBy.tagName/sort (order_without_projection.yaml)', () => {
    const s: PQBuilderState = { ...pqDefault(), projection: ['menu_name', 'configuration'], orderField: 'update_time', orderDir: 'ASC' };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.orderBy).toEqual({ tagName: 'update_time', sort: 'SORT_ASC' });
  });

  it('maps DESC to SORT_DESC', () => {
    const s: PQBuilderState = { ...pqDefault(), orderField: 'update_time', orderDir: 'DESC' };
    expect(pqBuildQueryRequest(s, 'sw', 'ui_menu').orderBy).toEqual({ tagName: 'update_time', sort: 'SORT_DESC' });
  });

  it('maps LIMIT to a numeric limit field (limit.yaml)', () => {
    const s: PQBuilderState = { ...pqDefault(), limit: 1 };
    expect(pqBuildQueryRequest(s, 'sw', 'ui_menu').limit).toBe(1);
  });

  it('combines an AND group of two non-id tag conditions into a nested criteria tree', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: {
        combinator: 'AND',
        children: [
          { tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test1' },
          { tag: 'total', op: 'BINARY_OP_GT', value: '5', conn: 'AND' },
        ],
      },
    };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.criteria).toEqual({
      le: {
        op: 'LOGICAL_OP_AND',
        left: { condition: { name: 'menu_name', op: 'BINARY_OP_EQ', value: { str: { value: 'test1' } } } },
        right: { condition: { name: 'total', op: 'BINARY_OP_GT', value: { int: { value: '5' } } } },
      },
    });
  });

  it('drops an ID leaf from criteria while keeping sibling tag conditions', () => {
    const s: PQBuilderState = {
      ...pqDefault(),
      where: {
        combinator: 'AND',
        children: [
          { tag: PQ_ID, op: 'BINARY_OP_EQ', value: '2' },
          { tag: 'menu_name', op: 'BINARY_OP_EQ', value: 'test1', conn: 'AND' },
        ],
      },
    };
    const req = pqBuildQueryRequest(s, 'sw', 'ui_menu');
    expect(req.ids).toEqual(['2']);
    expect(req.criteria).toEqual({ condition: { name: 'menu_name', op: 'BINARY_OP_EQ', value: { str: { value: 'test1' } } } });
  });
});
