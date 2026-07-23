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

// WHERE-clause coverage suite. Every operator family and tree shape found in
// the BanyanDB BydbQL conformance corpus (test/cases/{measure,stream,trace,
// property,topn}/**/*.ql) is exercised here, asserting the QueryBuilder's WHERE
// generator (qbLeafSQL / qbNodeSQL) produces the matching BydbQL. The expected
// strings are the builder's canonical form: multi-condition AND runs are
// parenthesized to make precedence explicit (AND binds tighter than OR), which
// is semantically identical to the corpus.

import { describe, it, expect } from 'vitest';
import {
  qbLeafSQL,
  qbNodeSQL,
  type QBWhereLeafWithConn,
  type QBWhereGroupWithConn,
  type QBWhereNode,
} from './bydbql.js';

// Schema-STRING tags: numeric-looking values must stay quoted for these.
const STR = new Set([
  'id', 'entity_id', 'service_id', 'service_instance_id', 'trace_id', 'span_id',
  'endpoint_id', 'endpoint_name', 'name', 'menu_name', 'db.instance', 'http.uri',
  'query', 'extended_tags', 'non_indexed_tags', 'local_endpoint_ipv4', 'message',
]);

const L = (
  tag: string,
  op: string,
  value = '',
  extra: Partial<QBWhereLeafWithConn> = {},
): QBWhereLeafWithConn => ({ tag, op, value, ...extra });

const G = (
  combinator: 'AND' | 'OR',
  children: readonly QBWhereNode[],
  conn?: 'AND' | 'OR',
): QBWhereGroupWithConn => ({ combinator, children, ...(conn ? { conn } : {}) });

// ── Leaf operators ──────────────────────────────────────────────────────────
describe('WHERE leaf operators', () => {
  const cases: Array<[string, QBWhereLeafWithConn, string]> = [
    // Comparison on string tags (quoted, incl. numeric-looking strings).
    ['EQ string', L('id', 'BINARY_OP_EQ', 'svc1'), "id = 'svc1'"],
    ['NE string', L('service_id', 'BINARY_OP_NE', 'auth_service'), "service_id != 'auth_service'"],
    ['LT string', L('id', 'BINARY_OP_LT', 'svc2'), "id < 'svc2'"],
    ['LE string', L('id', 'BINARY_OP_LE', 'svc2'), "id <= 'svc2'"],
    ['GT string', L('id', 'BINARY_OP_GT', 'svc1'), "id > 'svc1'"],
    ['GE string', L('id', 'BINARY_OP_GE', 'svc2'), "id >= 'svc2'"],
    // Comparison on integer tags (unquoted, incl. negative + big int).
    ['EQ int', L('duration', 'BINARY_OP_EQ', '500'), 'duration = 500'],
    ['LT int', L('duration', 'BINARY_OP_LT', '1000'), 'duration < 1000'],
    ['GE int', L('duration', 'BINARY_OP_GE', '200'), 'duration >= 200'],
    ['EQ negative int', L('layer', 'BINARY_OP_EQ', '-1'), 'layer = -1'],
    ['GE zero', L('state', 'BINARY_OP_GE', '0'), 'state >= 0'],
    ['LT int (status)', L('status_code', 'BINARY_OP_LT', '200'), 'status_code < 200'],
    ['EQ big int (nanos)', L('start_time', 'BINARY_OP_EQ', '1622933202000000000'), 'start_time = 1622933202000000000'],
    // NULL comparisons — bare literal, never quoted, even for string tags.
    ['EQ NULL (string tag)', L('id', 'BINARY_OP_EQ', 'NULL'), 'id = NULL'],
    ['NE NULL', L('service_id', 'BINARY_OP_NE', 'NULL'), 'service_id != NULL'],
    ['EQ NULL (dotted tag)', L('http.uri', 'BINARY_OP_EQ', 'NULL'), 'http.uri = NULL'],
    // IN / NOT IN — membership list; strings quoted, ints bare, empty allowed.
    ['IN single string', L('id', 'BINARY_OP_IN', 'svc1'), "id IN ('svc1')"],
    ['IN multi string', L('trace_id', 'BINARY_OP_IN', 'trace_001,trace_002'), "trace_id IN ('trace_001', 'trace_002')"],
    ['IN multi int', L('layer', 'BINARY_OP_IN', '1,2'), 'layer IN (1, 2)'],
    ['NOT IN single', L('id', 'BINARY_OP_NOT_IN', 'svc2'), "id NOT IN ('svc2')"],
    ['IN empty list', L('span_id', 'BINARY_OP_IN', ''), 'span_id IN ()'],
    // HAVING / NOT HAVING — array-contains; single bare, multiple parenthesized.
    ['HAVING single', L('non_indexed_tags', 'BINARY_OP_HAVING', 'c'), "non_indexed_tags HAVING 'c'"],
    ['HAVING list', L('extended_tags', 'BINARY_OP_HAVING', 'c,b'), "extended_tags HAVING ('c', 'b')"],
    ['NOT HAVING list', L('non_indexed_tags', 'BINARY_OP_NOT_HAVING', 'b,c'), "non_indexed_tags NOT HAVING ('b', 'c')"],
    // MATCH — full-text function-call syntax.
    ['MATCH single', L('name', 'BINARY_OP_MATCH', 'nodea'), "name MATCH('nodea')"],
    ['MATCH int value', L('layer', 'BINARY_OP_MATCH', '1'), 'layer MATCH(1)'],
    ['MATCH multi values', L('message', 'BINARY_OP_MATCH', 'error,warning'), "message MATCH(('error', 'warning'))"],
    ['MATCH with analyzer + operator', L('endpoint_name', 'BINARY_OP_MATCH', 'endpoint-1', { analyzer: '', matchOp: 'AND' }), "endpoint_name MATCH('endpoint-1', '', 'AND')"],
    ['MATCH multi + analyzer + operator', L('message', 'BINARY_OP_MATCH', 'error,warning', { analyzer: 'standard', matchOp: 'OR' }), "message MATCH(('error', 'warning'), 'standard', 'OR')"],
    // Quote-escaping: a value with an apostrophe switches to double quotes.
    ['EQ value with apostrophe', L('name', 'BINARY_OP_EQ', "O'Brien"), 'name = "O\'Brien"'],
  ];
  for (const [name, leaf, expected] of cases) {
    it(name, () => {
      expect(qbLeafSQL(leaf, STR)).toBe(expected);
    });
  }

  it('renders an empty leaf (no tag/op) as an empty string', () => {
    expect(qbLeafSQL(L('', 'BINARY_OP_EQ', 'x'), STR)).toBe('');
  });
});

// ── Tree structure & AND/OR precedence ────────────────────────────────────────
describe('WHERE tree structure', () => {
  const cases: Array<[string, QBWhereNode, string]> = [
    ['single leaf (no parens)', G('AND', [L('duration', 'BINARY_OP_LT', '1000')]), 'duration < 1000'],
    [
      'AND pair (parenthesized run)',
      G('AND', [L('duration', 'BINARY_OP_GE', '200'), L('duration', 'BINARY_OP_LE', '1000')]),
      '(duration >= 200 AND duration <= 1000)',
    ],
    [
      'OR pair',
      G('OR', [L('id', 'BINARY_OP_EQ', 'svc1'), L('entity_id', 'BINARY_OP_EQ', 'entity_2')]),
      "id = 'svc1' OR entity_id = 'entity_2'",
    ],
    [
      'three-way AND run',
      G('AND', [
        L('state', 'BINARY_OP_EQ', '0'),
        L('endpoint_id', 'BINARY_OP_EQ', '/home_endpoint'),
        L('service_id', 'BINARY_OP_EQ', 'webapp_service'),
      ]),
      "(state = 0 AND endpoint_id = '/home_endpoint' AND service_id = 'webapp_service')",
    ],
    [
      'precedence: A AND B OR C',
      G('AND', [
        L('status_code', 'BINARY_OP_LT', '200'),
        L('service_id', 'BINARY_OP_EQ', 'webapp_id', { conn: 'AND' }),
        L('duration', 'BINARY_OP_LT', '100', { conn: 'OR' }),
      ]),
      "(status_code < 200 AND service_id = 'webapp_id') OR duration < 100",
    ],
    [
      'nested: OR of AND groups (depth 2)',
      G('OR', [
        G('AND', [L('id', 'BINARY_OP_EQ', 'svc1'), L('entity_id', 'BINARY_OP_EQ', 'entity_2')]),
        G('AND', [L('id', 'BINARY_OP_EQ', 'svc3'), L('entity_id', 'BINARY_OP_EQ', 'entity_4')], 'OR'),
      ]),
      "(id = 'svc1' AND entity_id = 'entity_2') OR (id = 'svc3' AND entity_id = 'entity_4')",
    ],
    [
      'nested: AND of OR groups (depth 2)',
      G('AND', [
        G('OR', [L('id', 'BINARY_OP_EQ', 'svc1'), L('entity_id', 'BINARY_OP_EQ', 'entity_2')]),
        G('OR', [L('id', 'BINARY_OP_EQ', 'svc3'), L('entity_id', 'BINARY_OP_EQ', 'entity_4')], 'AND'),
      ]),
      "((id = 'svc1' OR entity_id = 'entity_2') AND (id = 'svc3' OR entity_id = 'entity_4'))",
    ],
    [
      'mixed leaf + group',
      G('OR', [
        G('AND', [L('id', 'BINARY_OP_EQ', 'svc1'), L('entity_id', 'BINARY_OP_EQ', 'entity_1')]),
        L('entity_id', 'BINARY_OP_EQ', 'entity_6', { conn: 'OR' }),
      ]),
      "(id = 'svc1' AND entity_id = 'entity_1') OR entity_id = 'entity_6'",
    ],
    [
      'range + equality run (duration + ipv4)',
      G('AND', [
        L('duration', 'BINARY_OP_GE', '100'),
        L('duration', 'BINARY_OP_LE', '1000', { conn: 'AND' }),
        L('local_endpoint_ipv4', 'BINARY_OP_EQ', '192.168.1.10', { conn: 'AND' }),
      ]),
      "(duration >= 100 AND duration <= 1000 AND local_endpoint_ipv4 = '192.168.1.10')",
    ],
    [
      'HAVING combined with equality',
      G('AND', [
        L('extended_tags', 'BINARY_OP_HAVING', 'c,b'),
        L('trace_id', 'BINARY_OP_EQ', '5', { conn: 'AND' }),
      ]),
      "(extended_tags HAVING ('c', 'b') AND trace_id = '5')",
    ],
  ];
  for (const [name, node, expected] of cases) {
    it(name, () => {
      expect(qbNodeSQL(node, 0, STR)).toBe(expected);
    });
  }

  it('drops empty leaves before rendering', () => {
    const where = G('AND', [L('a', 'BINARY_OP_EQ', '1'), L('', 'BINARY_OP_EQ', '')]);
    expect(qbNodeSQL(where, 0)).toBe('a = 1');
  });
});
