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
  validateInterval,
  validateStages,
  validateGroup,
  validateStream,
  validateMeasure,
  validateTrace,
  validateIndexRule,
  validateIndexRuleBinding,
} from './validation.js';

const goodInterval = { num: 1, unit: 'UNIT_DAY' as const };
const goodStreamFamilies = [
  { name: 'default', tags: [{ name: 'host', type: 'TAG_TYPE_STRING' }] },
];

describe('1. validateInterval', () => {
  it('accepts positive num + UNIT_DAY', () => {
    expect(validateInterval({ num: 7, unit: 'UNIT_DAY' })).toBeUndefined();
  });
  it('accepts positive num + UNIT_HOUR', () => {
    expect(validateInterval({ num: 24, unit: 'UNIT_HOUR' })).toBeUndefined();
  });
  it('rejects num=0', () => {
    expect(validateInterval({ num: 0, unit: 'UNIT_DAY' })).toMatch(/greater than 0/);
  });
  it('rejects negative num', () => {
    expect(validateInterval({ num: -3, unit: 'UNIT_DAY' })).toMatch(/greater than 0/);
  });
  it('rejects empty num', () => {
    expect(validateInterval({ num: '', unit: 'UNIT_DAY' })).toMatch(/greater than 0/);
  });
  it('rejects invalid unit', () => {
    expect(validateInterval({ num: 1, unit: 'UNIT_WEEK' })).toMatch(/UNIT_DAY or UNIT_HOUR/);
  });
  it('rejects undefined', () => {
    expect(validateInterval(undefined)).toMatch(/required/);
  });
});

describe('2. validateStages', () => {
  const goodStage = {
    name: 'warm',
    shardNum: 2,
    segmentInterval: { num: 1, unit: 'UNIT_DAY' as const },
    ttl: { num: 7, unit: 'UNIT_DAY' as const },
    nodeSelector: 'tier=warm',
  };

  it('returns no errors for a valid stage', () => {
    expect(validateStages([goodStage])).toEqual([]);
  });
  it('returns no errors for empty stages array', () => {
    expect(validateStages([])).toEqual([]);
  });
  it('rejects empty stage name', () => {
    const errs = validateStages([{ ...goodStage, name: '' }]);
    expect(errs[0]?.name).toMatch(/required/);
  });
  it('rejects stage name with bad chars', () => {
    const errs = validateStages([{ ...goodStage, name: 'hot tier' }]);
    expect(errs[0]?.name).toMatch(/letters, digits/);
  });
  it('rejects shardNum=0', () => {
    const errs = validateStages([{ ...goodStage, shardNum: 0 }]);
    expect(errs[0]?.shardNum).toMatch(/greater than 0/);
  });
  it('rejects missing node selector', () => {
    const errs = validateStages([{ ...goodStage, nodeSelector: '' }]);
    expect(errs[0]?.nodeSelector).toMatch(/required/);
  });
  it('rejects negative replicas', () => {
    const errs = validateStages([{ ...goodStage, replicas: -1 }]);
    expect(errs[0]?.replicas).toMatch(/≥ 0/);
  });
  it('accepts replicas=0', () => {
    expect(validateStages([{ ...goodStage, replicas: 0 }])).toEqual([]);
  });
  it('indexes errors by stage position', () => {
    const errs = validateStages([goodStage, { ...goodStage, name: '' }]);
    expect(errs[0]).toBeUndefined();
    expect(errs[1]?.name).toMatch(/required/);
  });
});

describe('3. validateGroup', () => {
  const base = {
    isEdit: false,
    name: 'sw_metric',
    catalog: 'CATALOG_MEASURE',
    shardNum: 2,
    segmentInterval: goodInterval,
    ttl: { num: 7, unit: 'UNIT_DAY' as const },
  };

  it('accepts a fully-valid group', () => {
    expect(isGroupValid(validateGroup(base))).toBe(true);
  });
  it('rejects empty name in create mode', () => {
    const e = validateGroup({ ...base, name: '' });
    expect(e.name).toMatch(/required/);
  });
  it('skips name check in edit mode', () => {
    const e = validateGroup({ ...base, isEdit: true, name: '' });
    expect(e.name).toBeUndefined();
  });
  it('rejects name with bad chars', () => {
    const e = validateGroup({ ...base, name: 'has spaces' });
    expect(e.name).toMatch(/letters, digits/);
  });
  it('rejects shardNum=0', () => {
    const e = validateGroup({ ...base, shardNum: 0 });
    expect(e.shardNum).toMatch(/greater than 0/);
  });
  it('rejects missing segment interval for non-property', () => {
    const e = validateGroup({ ...base, segmentInterval: undefined });
    expect(e.segmentInterval).toMatch(/required/);
  });
  it('skips segment/ttl for property catalog', () => {
    const e = validateGroup({ ...base, catalog: 'CATALOG_PROPERTY', segmentInterval: undefined, ttl: undefined });
    expect(e.segmentInterval).toBeUndefined();
    expect(e.ttl).toBeUndefined();
  });
  it('reports per-stage errors when stages are present', () => {
    const e = validateGroup({
      ...base,
      stages: [
        {
          name: '',
          shardNum: 1,
          segmentInterval: goodInterval,
          ttl: { num: 1, unit: 'UNIT_DAY' },
          nodeSelector: 'tier=warm',
        },
      ],
    });
    expect(e.stages?.[0]?.name).toMatch(/required/);
  });
});

function isGroupValid(e: ReturnType<typeof validateGroup>): boolean {
  return Object.keys(e).length === 0;
}

describe('4. validateStream', () => {
  const good = {
    isEdit: false,
    name: 'sw_logs',
    families: goodStreamFamilies,
    entityTagNames: ['host'],
  };

  it('accepts a valid stream', () => {
    expect(validateStream(good)).toBeUndefined();
  });
  it('rejects empty name', () => {
    expect(validateStream({ ...good, name: '' })).toMatch(/Name is required/);
  });
  it('rejects tag name containing #', () => {
    const e = validateStream({ ...good, families: [{ name: 'f', tags: [{ name: 'has#hash', type: 'TAG_TYPE_STRING' }] }] });
    expect(e).toMatch(/#/);
  });
  it('rejects empty entity tags', () => {
    expect(validateStream({ ...good, entityTagNames: [] })).toMatch(/entity tag is required/);
  });
  it('rejects entity tag that is not defined', () => {
    expect(validateStream({ ...good, entityTagNames: ['notdefined'] })).toMatch(/not defined/);
  });
  it('rejects duplicate tag names across families', () => {
    const e = validateStream({
      ...good,
      families: [
        { name: 'a', tags: [{ name: 'shared', type: 'TAG_TYPE_STRING' }] },
        { name: 'b', tags: [{ name: 'shared', type: 'TAG_TYPE_STRING' }] },
      ],
    });
    expect(e).toMatch(/Duplicate tag/);
  });
});

describe('5. validateMeasure', () => {
  const good = {
    isEdit: false,
    name: 'sw_metric',
    families: goodStreamFamilies,
    fields: [{ name: 'value', fieldType: 'FIELD_TYPE_INT64' }],
    entityTagNames: ['host'],
    indexMode: false,
    interval: '1d',
  };

  it('accepts a valid measure', () => {
    expect(validateMeasure(good)).toBeUndefined();
  });
  it('rejects empty fields when not index mode', () => {
    expect(validateMeasure({ ...good, fields: [] })).toMatch(/At least one field/);
  });
  it('rejects non-empty fields when index mode is on', () => {
    expect(validateMeasure({ ...good, indexMode: true, fields: good.fields })).toMatch(/fields must be empty/);
  });
  it('accepts index mode with empty fields', () => {
    expect(validateMeasure({ ...good, indexMode: true, fields: [] })).toBeUndefined();
  });
  it('rejects empty interval', () => {
    expect(validateMeasure({ ...good, interval: '' })).toMatch(/Interval is required/);
  });
  it('rejects empty name', () => {
    expect(validateMeasure({ ...good, name: '' })).toMatch(/Name is required/);
  });
});

describe('6. validateTrace', () => {
  const good = {
    isEdit: false,
    name: 'sw_trace',
    tags: [
      { name: 'trace_id', type: 'TAG_TYPE_STRING' },
      { name: 'span_id', type: 'TAG_TYPE_STRING' },
      { name: 'ts', type: 'TAG_TYPE_TIMESTAMP' },
    ],
    traceIdTagName: 'trace_id',
    spanIdTagName: 'span_id',
    timestampTagName: 'ts',
  };

  it('accepts a valid trace', () => {
    expect(validateTrace(good)).toBeUndefined();
  });
  it('rejects when traceIdTagName is not in tags', () => {
    expect(validateTrace({ ...good, traceIdTagName: 'unknown' })).toMatch(/Trace ID tag "unknown" is not defined/);
  });
  it('rejects when spanIdTagName is not in tags', () => {
    expect(validateTrace({ ...good, spanIdTagName: 'unknown' })).toMatch(/Span ID tag "unknown" is not defined/);
  });
  it('rejects when timestampTagName is not in tags', () => {
    expect(validateTrace({ ...good, timestampTagName: 'unknown' })).toMatch(/Timestamp tag "unknown" is not defined/);
  });
  it('rejects when role tags collide', () => {
    expect(validateTrace({ ...good, traceIdTagName: 'span_id' })).toMatch(/Trace ID and Span ID must reference different tags/);
  });
  it('rejects duplicate tag names', () => {
    expect(validateTrace({ ...good, tags: [
      { name: 'x', type: 'TAG_TYPE_STRING' },
      { name: 'x', type: 'TAG_TYPE_STRING' },
    ] })).toMatch(/Duplicate tag/);
  });
  it('rejects tag name containing #', () => {
    expect(validateTrace({ ...good, tags: [
      { name: 'has#hash', type: 'TAG_TYPE_STRING' },
      { name: 'span_id', type: 'TAG_TYPE_STRING' },
      { name: 'ts', type: 'TAG_TYPE_TIMESTAMP' },
    ], traceIdTagName: 'has#hash' })).toMatch(/#/);
  });
});

describe('7. validateIndexRule', () => {
  const good = {
    isEdit: false,
    name: 'by_host',
    tags: ['host'],
    type: 'TYPE_TREE' as const,
  };

  it('accepts a valid TREE rule', () => {
    expect(validateIndexRule(good)).toBeUndefined();
  });
  it('accepts a valid INVERTED rule', () => {
    expect(validateIndexRule({ ...good, type: 'TYPE_INVERTED' })).toBeUndefined();
  });
  it('rejects empty name', () => {
    expect(validateIndexRule({ ...good, name: '' })).toMatch(/Name is required/);
  });
  it('rejects empty tags', () => {
    expect(validateIndexRule({ ...good, tags: [] })).toMatch(/At least one tag is required/);
  });
  it('rejects unknown index type', () => {
    expect(validateIndexRule({ ...good, type: 'TYPE_BLOOM' })).toMatch(/Index type must be/);
  });
  it('rejects duplicate tags', () => {
    expect(validateIndexRule({ ...good, tags: ['a', 'a'] })).toMatch(/Duplicate tag/);
  });
  it('rejects names longer than 255 chars', () => {
    expect(validateIndexRule({ ...good, name: 'a'.repeat(256) })).toMatch(/255 characters or fewer/);
  });
  it('rejects duplicate names against existingNames (case-insensitive)', () => {
    const existingNames = new Set(['by_host']);
    expect(validateIndexRule({ ...good, existingNames })).toMatch(/already exists/);
    expect(validateIndexRule({ ...good, name: 'BY_HOST', existingNames })).toMatch(/already exists/);
  });
  it('accepts the same name in edit mode even when it is in existingNames', () => {
    const existingNames = new Set(['by_host']);
    expect(validateIndexRule({ ...good, isEdit: true, existingNames })).toBeUndefined();
  });
});

describe('8. validateIndexRuleBinding', () => {
  const good = {
    isEdit: false,
    name: 'binding-1',
    rules: ['by_host'],
    subject: { name: 'sw_metric', catalog: 'CATALOG_MEASURE' },
    beginAt: Date.parse('2026-01-01T00:00:00Z'),
    expireAt: Date.parse('2026-02-01T00:00:00Z'),
  };

  it('accepts a valid binding', () => {
    expect(validateIndexRuleBinding(good)).toBeUndefined();
  });
  it('rejects empty rules', () => {
    expect(validateIndexRuleBinding({ ...good, rules: [] })).toMatch(/At least one rule is required/);
  });
  it('rejects missing subject', () => {
    expect(validateIndexRuleBinding({ ...good, subject: { name: '', catalog: '' } })).toMatch(/Subject resource is required/);
  });
  it('rejects expireAt ≤ beginAt', () => {
    expect(validateIndexRuleBinding({ ...good, expireAt: good.beginAt })).toMatch(/Expire time must be after/);
  });
  it('rejects null beginAt', () => {
    expect(validateIndexRuleBinding({ ...good, beginAt: null })).toMatch(/Begin time is required/);
  });
  it('rejects null expireAt', () => {
    expect(validateIndexRuleBinding({ ...good, expireAt: null })).toMatch(/Expire time is required/);
  });
  it('rejects names longer than 255 chars', () => {
    expect(validateIndexRuleBinding({ ...good, name: 'a'.repeat(256) })).toMatch(/255 characters or fewer/);
  });
  it('rejects duplicate names against existingNames', () => {
    const existingNames = new Set(['binding-1']);
    expect(validateIndexRuleBinding({ ...good, existingNames })).toMatch(/already exists/);
  });
  it('accepts the same name in edit mode even when it is in existingNames', () => {
    const existingNames = new Set(['binding-1']);
    expect(validateIndexRuleBinding({ ...good, isEdit: true, existingNames })).toBeUndefined();
  });
});
