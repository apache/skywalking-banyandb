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

// Unit tests for topn-shared.tsx's pure helpers (direction<->sort mapping,
// the flat-criteria<->model.v1.Criteria codec) and TopNForms.tsx's
// validateTopN. Colocated in one file per the Pipelines/TopN test plan —
// validateTopN lives in TopNForms.tsx (it needs TopNDraft/TopNValidationCtx,
// which are declared there), but its rules are the same "shared TopN
// domain logic" this file is about, so it's exercised alongside the
// direction/criteria helpers rather than split into a same-named file.

import { describe, it, expect } from 'vitest';
import {
  topNTone, topNRank, topNSortLabel, topnOpSymbol, SORT_OPTS, TOPN_OPS, DEFAULT_COUNTERS,
  buildTopNCriteria, flattenTopNCriteria, type TopNCondition, type TopNSort,
} from './topn-shared.js';
import { validateTopN, type TopNDraft, type TopNValidationCtx } from './TopNForms.js';

describe('topNTone / topNRank — direction<->sort mapping', () => {
  it('SORT_DESC -> topN / topn tone', () => {
    expect(topNRank('SORT_DESC')).toBe('topN');
    expect(topNTone('SORT_DESC')).toBe('topn');
  });
  it('SORT_ASC -> bottomN / bottomn tone', () => {
    expect(topNRank('SORT_ASC')).toBe('bottomN');
    expect(topNTone('SORT_ASC')).toBe('bottomn');
  });
  it('SORT_UNSPECIFIED -> both / bothn tone', () => {
    expect(topNRank('SORT_UNSPECIFIED')).toBe('both');
    expect(topNTone('SORT_UNSPECIFIED')).toBe('bothn');
  });
  it('undefined is treated the same as SORT_DESC (proto3 zero value)', () => {
    const sort: TopNSort = undefined;
    expect(topNRank(sort)).toBe('topN');
    expect(topNTone(sort)).toBe('topn');
    expect(topNSortLabel(sort)).toBe('SORT_DESC');
  });
  it('topNSortLabel echoes a defined sort verbatim', () => {
    expect(topNSortLabel('SORT_ASC')).toBe('SORT_ASC');
    expect(topNSortLabel('SORT_UNSPECIFIED')).toBe('SORT_UNSPECIFIED');
  });
});

describe('SORT_OPTS', () => {
  it('offers exactly the three Sort enum values with the correct rank mapping', () => {
    expect(SORT_OPTS.map((o) => o.value)).toEqual(['SORT_DESC', 'SORT_ASC', 'SORT_UNSPECIFIED']);
    expect(SORT_OPTS.find((o) => o.value === 'SORT_DESC')?.rank).toBe('topN');
    expect(SORT_OPTS.find((o) => o.value === 'SORT_ASC')?.rank).toBe('bottomN');
    expect(SORT_OPTS.find((o) => o.value === 'SORT_UNSPECIFIED')?.rank).toBe('both');
  });
});

describe('topnOpSymbol', () => {
  it('maps every TOPN_OPS entry to a symbol', () => {
    for (const op of TOPN_OPS) {
      expect(topnOpSymbol(op.value)).not.toBe(op.value === 'BINARY_OP_EQ' ? undefined : '');
    }
    expect(topnOpSymbol('BINARY_OP_EQ')).toBe('=');
    expect(topnOpSymbol('BINARY_OP_IN')).toBe('IN');
  });
  it('falls back to the raw op string for an unknown operator', () => {
    expect(topnOpSymbol('BINARY_OP_HAVING')).toBe('BINARY_OP_HAVING');
  });
});

describe('buildTopNCriteria / flattenTopNCriteria — flat AND chain <-> model.v1.Criteria codec', () => {
  it('returns undefined for an empty condition list', () => {
    expect(buildTopNCriteria([])).toBeUndefined();
  });

  it('drops blank-tag rows and returns undefined if nothing remains', () => {
    const rows: TopNCondition[] = [{ tag: '   ', op: 'BINARY_OP_EQ', value: 'x' }];
    expect(buildTopNCriteria(rows)).toBeUndefined();
  });

  it('builds a single condition leaf for one row', () => {
    const rows: TopNCondition[] = [{ tag: 'service', op: 'BINARY_OP_EQ', value: 'checkout' }];
    const tree = buildTopNCriteria(rows);
    expect(tree).toEqual({ condition: { name: 'service', op: 'BINARY_OP_EQ', value: { str: { value: 'checkout' } } } });
  });

  it('numeric-looking values are encoded as int, not str', () => {
    const rows: TopNCondition[] = [{ tag: 'status', op: 'BINARY_OP_GT', value: '200' }];
    const tree = buildTopNCriteria(rows);
    expect(tree).toEqual({ condition: { name: 'status', op: 'BINARY_OP_GT', value: { int: { value: '200' } } } });
  });

  it('IN/NOT_IN values are split on commas into an array TagValue', () => {
    const strRows: TopNCondition[] = [{ tag: 'service', op: 'BINARY_OP_IN', value: 'checkout, cart' }];
    expect(buildTopNCriteria(strRows)).toEqual({
      condition: { name: 'service', op: 'BINARY_OP_IN', value: { strArray: { value: ['checkout', 'cart'] } } },
    });
    const intRows: TopNCondition[] = [{ tag: 'status', op: 'BINARY_OP_NOT_IN', value: '200, 500' }];
    expect(buildTopNCriteria(intRows)).toEqual({
      condition: { name: 'status', op: 'BINARY_OP_NOT_IN', value: { intArray: { value: ['200', '500'] } } },
    });
  });

  it('decimals encode as str — TagValue has no float variant', () => {
    // 1.5 as TagValue.int would be an invalid payload the server rejects.
    const rows: TopNCondition[] = [{ tag: 'rate', op: 'BINARY_OP_GT', value: '1.5' }];
    expect(buildTopNCriteria(rows)).toEqual({
      condition: { name: 'rate', op: 'BINARY_OP_GT', value: { str: { value: '1.5' } } },
    });
    const mixed: TopNCondition[] = [{ tag: 'rate', op: 'BINARY_OP_IN', value: '1.5, 2' }];
    expect(buildTopNCriteria(mixed)).toEqual({
      condition: { name: 'rate', op: 'BINARY_OP_IN', value: { strArray: { value: ['1.5', '2'] } } },
    });
  });

  it('chains multiple rows with LOGICAL_OP_AND, left-associatively', () => {
    const rows: TopNCondition[] = [
      { tag: 'service', op: 'BINARY_OP_EQ', value: 'checkout' },
      { tag: 'region', op: 'BINARY_OP_EQ', value: 'us' },
      { tag: 'status', op: 'BINARY_OP_GT', value: '200' },
    ];
    const tree = buildTopNCriteria(rows);
    expect(tree).toEqual({
      le: {
        op: 'LOGICAL_OP_AND',
        left: {
          le: {
            op: 'LOGICAL_OP_AND',
            left: { condition: { name: 'service', op: 'BINARY_OP_EQ', value: { str: { value: 'checkout' } } } },
            right: { condition: { name: 'region', op: 'BINARY_OP_EQ', value: { str: { value: 'us' } } } },
          },
        },
        right: { condition: { name: 'status', op: 'BINARY_OP_GT', value: { int: { value: '200' } } } },
      },
    });
  });

  it('round-trips a flat AND chain through build -> flatten', () => {
    const rows: TopNCondition[] = [
      { tag: 'service', op: 'BINARY_OP_EQ', value: 'checkout' },
      { tag: 'region', op: 'BINARY_OP_NE', value: 'eu' },
      { tag: 'latency_ms', op: 'BINARY_OP_LE', value: '500' },
    ];
    const tree = buildTopNCriteria(rows);
    expect(flattenTopNCriteria(tree)).toEqual(rows);
  });

  it('flattenTopNCriteria returns an empty array for undefined criteria', () => {
    expect(flattenTopNCriteria(undefined)).toEqual([]);
  });
});

// ── validateTopN — TopNForms.tsx ────────────────────────────────────────────
//
// Signature: validateTopN(v: TopNDraft, ctx: TopNValidationCtx): TopNValidationErrors
//   TopNDraft = { name, sourceGroup, sourceName, fieldName, fieldValueSort,
//                 groupByTagNames, criteria, countersNumber, lruSize }
//   TopNValidationCtx = { mode: 'create' | 'edit', existingNames?, fieldOptions? }

function baseDraft(overrides: Partial<TopNDraft> = {}): TopNDraft {
  return {
    name: 'svc_cpm_topn',
    sourceGroup: 'sw_metric',
    sourceName: 'service_cpm_minute',
    fieldName: 'value',
    fieldValueSort: 'SORT_DESC',
    groupByTagNames: [],
    criteria: [],
    countersNumber: DEFAULT_COUNTERS,
    lruSize: 10,
    ...overrides,
  };
}

const createCtx: TopNValidationCtx = { mode: 'create', existingNames: new Set(), fieldOptions: ['value', 'latency'] };
const editCtx: TopNValidationCtx = { mode: 'edit', fieldOptions: ['value', 'latency'] };

describe('validateTopN', () => {
  it('returns no errors for a fully valid create draft', () => {
    expect(validateTopN(baseDraft(), createCtx)).toEqual({});
  });

  it('returns no errors for a fully valid edit draft (name not checked in edit mode)', () => {
    expect(validateTopN(baseDraft({ name: '' }), editCtx)).toEqual({});
  });

  it('flags a missing name in create mode', () => {
    const errs = validateTopN(baseDraft({ name: '' }), createCtx);
    expect(errs.name).toMatch(/required/i);
  });

  it('does not flag a missing name in edit mode (name is immutable there)', () => {
    const errs = validateTopN(baseDraft({ name: '' }), editCtx);
    expect(errs.name).toBeUndefined();
  });

  it('flags a name that violates the allowed character pattern', () => {
    const errs = validateTopN(baseDraft({ name: 'bad name!' }), createCtx);
    expect(errs.name).toMatch(/letters, digits/i);
  });

  it('flags a name over 255 characters', () => {
    const errs = validateTopN(baseDraft({ name: 'a'.repeat(256) }), createCtx);
    expect(errs.name).toMatch(/255/);
  });

  it('flags a duplicate name (case-insensitive) against existingNames', () => {
    const ctx: TopNValidationCtx = { mode: 'create', existingNames: new Set(['svc_cpm_topn']) };
    const errs = validateTopN(baseDraft({ name: 'SVC_CPM_TOPN' }), ctx);
    expect(errs.name).toMatch(/already exists/i);
  });

  it('flags a missing source group', () => {
    const errs = validateTopN(baseDraft({ sourceGroup: '' }), createCtx);
    expect(errs.sourceGroup).toMatch(/select a source group/i);
  });

  it('flags a missing source measure', () => {
    const errs = validateTopN(baseDraft({ sourceName: '' }), createCtx);
    expect(errs.sourceMeasure).toMatch(/select a source measure/i);
  });

  it('flags a missing ranked field', () => {
    const errs = validateTopN(baseDraft({ fieldName: '' }), createCtx);
    expect(errs.fieldName).toMatch(/required/i);
  });

  it('flags a ranked field that is not one of the source measure fieldOptions', () => {
    const errs = validateTopN(baseDraft({ fieldName: 'not_a_field' }), createCtx);
    expect(errs.fieldName).toMatch(/must be a field/i);
  });

  it('does not flag fieldName against fieldOptions when fieldOptions is empty/unknown', () => {
    const ctx: TopNValidationCtx = { mode: 'create', existingNames: new Set(), fieldOptions: [] };
    const errs = validateTopN(baseDraft({ fieldName: 'anything' }), ctx);
    expect(errs.fieldName).toBeUndefined();
  });

  it('flags an out-of-range countersNumber (zero or non-integer)', () => {
    expect(validateTopN(baseDraft({ countersNumber: 0 }), createCtx).countersNumber).toMatch(/greater than 0/i);
    expect(validateTopN(baseDraft({ countersNumber: 1.5 }), createCtx).countersNumber).toMatch(/whole number/i);
    expect(validateTopN(baseDraft({ countersNumber: -5 }), createCtx).countersNumber).toMatch(/greater than 0/i);
  });

  it('allows an empty countersNumber (falls back to the default at submit time)', () => {
    expect(validateTopN(baseDraft({ countersNumber: '' }), createCtx).countersNumber).toBeUndefined();
  });

  it('flags an out-of-range lruSize (negative or non-integer)', () => {
    expect(validateTopN(baseDraft({ lruSize: -1 }), createCtx).lruSize).toMatch(/0 or a positive/i);
    expect(validateTopN(baseDraft({ lruSize: 2.2 }), createCtx).lruSize).toMatch(/0 or a positive/i);
  });

  it('allows lruSize of exactly 0 (optional field, 0 is a valid LRU size)', () => {
    expect(validateTopN(baseDraft({ lruSize: 0 }), createCtx).lruSize).toBeUndefined();
  });

  it('flags a criteria row missing its tag', () => {
    const errs = validateTopN(baseDraft({ criteria: [{ tag: '', op: 'BINARY_OP_EQ', value: 'x' }] }), createCtx);
    expect(errs.criteria?.[0]?.tag).toMatch(/required/i);
    expect(errs.criteria?.[0]?.value).toBeUndefined();
  });

  it('flags a criteria row missing its value', () => {
    const errs = validateTopN(baseDraft({ criteria: [{ tag: 'service', op: 'BINARY_OP_EQ', value: '' }] }), createCtx);
    expect(errs.criteria?.[0]?.value).toMatch(/required/i);
    expect(errs.criteria?.[0]?.tag).toBeUndefined();
  });

  it('leaves the criteria error slot undefined for a fully-filled row among several', () => {
    const errs = validateTopN(
      baseDraft({
        criteria: [
          { tag: 'service', op: 'BINARY_OP_EQ', value: 'checkout' },
          { tag: '', op: 'BINARY_OP_EQ', value: '' },
        ],
      }),
      createCtx,
    );
    expect(errs.criteria?.[0]).toBeUndefined();
    expect(errs.criteria?.[1]?.tag).toMatch(/required/i);
    expect(errs.criteria?.[1]?.value).toMatch(/required/i);
  });
});
