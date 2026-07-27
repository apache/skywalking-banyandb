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
  srRoleFromType, srRoleFromValue, srRoleFromConvention, srRoleFromOverride,
  srInferRole, srInferRoleFromSchema, srColStats,
  srHttpColor, srSeverityColor, srRenderValue, srCatColor,
  type SR_ROLE,
} from './role-infer.js';

const NO_OVERRIDES: Readonly<Record<string, SR_ROLE>> = Object.freeze({});

describe('Layer 1 — srRoleFromType', () => {
  it('DATA_BINARY → binary', () => {
    expect(srRoleFromType('TAG_TYPE_DATA_BINARY')).toBe('binary');
  });
  it('STRING_ARRAY / INT_ARRAY → list', () => {
    expect(srRoleFromType('TAG_TYPE_STRING_ARRAY')).toBe('list');
    expect(srRoleFromType('TAG_TYPE_INT_ARRAY')).toBe('list');
  });
  it('TIMESTAMP → time', () => {
    expect(srRoleFromType('TAG_TYPE_TIMESTAMP')).toBe('time');
  });
  it('INT / INT64 → numeric', () => {
    expect(srRoleFromType('TAG_TYPE_INT')).toBe('numeric');
    expect(srRoleFromType('TAG_TYPE_INT64')).toBe('numeric');
  });
  it('STRING (and unknown) → text', () => {
    expect(srRoleFromType('TAG_TYPE_STRING')).toBe('text');
    expect(srRoleFromType('something-weird')).toBe('text');
  });
});

describe('Layer 2 — srRoleFromValue', () => {
  it('hex / uuid strings → id', () => {
    expect(srRoleFromValue('TAG_TYPE_STRING', ['cb2f9b0583567d4e', 'e466554881174205'], 'text')).toBe('id');
    expect(srRoleFromValue('TAG_TYPE_STRING', ['550e8400-e29b-41d4-a716-446655440000'], 'text')).toBe('id');
  });
  it('long string → body', () => {
    const longBody = 'a'.repeat(50);
    expect(srRoleFromValue('TAG_TYPE_STRING', [longBody, longBody + 'x'], 'text')).toBe('body');
  });
  it('closed string set → cat', () => {
    expect(srRoleFromValue('TAG_TYPE_STRING', ['INFO', 'DEBUG', 'WARN', 'INFO', 'DEBUG', 'WARN'], 'text')).toBe('cat');
  });
  it('plain short strings stay text', () => {
    expect(srRoleFromValue('TAG_TYPE_STRING', ['a', 'b', 'c', 'd'], 'text')).toBe('text');
  });
  it('tiny int set → cat', () => {
    expect(srRoleFromValue('TAG_TYPE_INT', [1, 2, 1, 2, 1, 2], 'numeric')).toBe('cat');
  });
  it('returns hint when no refinement applies', () => {
    expect(srRoleFromValue('TAG_TYPE_INT', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'numeric')).toBe('numeric');
  });
});

describe('Layer 3 — srRoleFromConvention (name-based)', () => {
  it('level / severity → cat', () => {
    expect(srRoleFromConvention('level')).toBe('cat');
    expect(srRoleFromConvention('Level')).toBe('cat');
    expect(srRoleFromConvention('log_level')).toBe('cat');
    expect(srRoleFromConvention('log.level')).toBe('cat');
    expect(srRoleFromConvention('severity')).toBe('cat');
  });
  it('status / http_status_code → cat', () => {
    expect(srRoleFromConvention('status_code')).toBe('cat');
    expect(srRoleFromConvention('http_status_code')).toBe('cat');
    expect(srRoleFromConvention('http.status_code')).toBe('cat');
  });
  it('span_layer → cat', () => {
    expect(srRoleFromConvention('span_layer')).toBe('cat');
  });
  it('trace_id / span_id → id', () => {
    expect(srRoleFromConvention('trace_id')).toBe('id');
    expect(srRoleFromConvention('span_id')).toBe('id');
  });
  it('returns null for names with no convention', () => {
    expect(srRoleFromConvention('duration')).toBeNull();
    expect(srRoleFromConvention('service')).toBeNull();
    expect(srRoleFromConvention('service_name')).toBeNull();
    expect(srRoleFromConvention('body')).toBeNull();
    expect(srRoleFromConvention('definitely_not_a_known_field')).toBeNull();
  });
});

describe('Layer 4 — srRoleFromOverride', () => {
  it('returns the override for known names', () => {
    expect(srRoleFromOverride('foo', { foo: 'cat' })).toBe('cat');
    const m = new Map<string, SR_ROLE>([['foo', 'id']]);
    expect(srRoleFromOverride('foo', m)).toBe('id');
  });
  it('returns null for unknown names', () => {
    expect(srRoleFromOverride('foo', {})).toBeNull();
    expect(srRoleFromOverride('foo', new Map())).toBeNull();
  });
});

describe('srInferRole — full 4-layer ladder', () => {
  it('layer 4 (override) wins over convention / type / value', () => {
    expect(srInferRole('level', 'TAG_TYPE_STRING', ['INFO'], { level: 'id' })).toEqual({
      role: 'id', layer: 4,
    });
  });
  it('layer 3 (convention) wins over type / value', () => {
    expect(srInferRole('level', 'TAG_TYPE_STRING', ['INFO'], NO_OVERRIDES)).toEqual({
      role: 'cat', layer: 3,
    });
  });
  it('layer 2 (value) refines layer 1 (type)', () => {
    expect(srInferRole('request_id', 'TAG_TYPE_STRING', ['cb2f9b0583567d4e', 'e466554881174205'], NO_OVERRIDES)).toEqual({
      role: 'id', layer: 2,
    });
  });
  it('layer 1 (type) is the fallback', () => {
    expect(srInferRole('payload', 'TAG_TYPE_DATA_BINARY', [], NO_OVERRIDES)).toEqual({
      role: 'binary', layer: 1,
    });
  });
});

describe('srInferRoleFromSchema — when no sample values are available', () => {
  it('uses override > convention > type', () => {
    expect(srInferRoleFromSchema('level', 'TAG_TYPE_STRING', {})).toEqual({ role: 'cat', layer: 3 });
    expect(srInferRoleFromSchema('level', 'TAG_TYPE_STRING', { level: 'id' })).toEqual({ role: 'id', layer: 4 });
    expect(srInferRoleFromSchema('payload', 'TAG_TYPE_STRING', {})).toEqual({ role: 'text', layer: 1 });
  });
});

describe('srColStats', () => {
  it('returns distinct + total + length for strings', () => {
    const s = srColStats(['a', 'b', 'a', 'c']);
    expect(s.distinct).toBe(3);
    expect(s.total).toBe(4);
    expect(s.avgLen).toBeCloseTo(1);
  });
  it('returns distinct + total + min/max for numbers', () => {
    const s = srColStats([1, 2, 3, 4]);
    expect(s.distinct).toBe(4);
    expect(s.total).toBe(4);
    expect(s.min).toBe(1);
    expect(s.max).toBe(4);
  });
  it('flags hex/uuid identifiers as idLike', () => {
    const s = srColStats(['cb2f9b0583567d4e', 'e466554881174205']);
    expect(s.idLike).toBe(true);
  });
  it('returns zeros for an empty sample', () => {
    expect(srColStats([])).toEqual({ distinct: 0, total: 0 });
  });
});

describe('semantic colors', () => {
  it('srHttpColor maps 5xx → danger, 4xx → warn, 2xx → accent', () => {
    expect(srHttpColor(503)).toBe('var(--danger)');
    expect(srHttpColor(404)).toBe('var(--warn)');
    expect(srHttpColor(200)).toBe('var(--accent)');
    expect(srHttpColor('503')).toBe('var(--danger)');
    expect(srHttpColor('nope')).toBe('var(--text-dim)');
  });
  it('srSeverityColor maps ERROR → danger, WARN → warn, INFO → accent', () => {
    expect(srSeverityColor('ERROR')).toBe('var(--danger)');
    expect(srSeverityColor('warn')).toBe('var(--warn)');
    expect(srSeverityColor('INFO')).toBe('var(--accent)');
    expect(srSeverityColor('???')).toBe('var(--text)');
  });
  it('srCatColor uses convention maps', () => {
    expect(srCatColor('level', 'ERROR')).toBe('var(--danger)');
    expect(srCatColor('status_code', 503)).toBe('var(--danger)');
  });
});

describe('srRenderValue', () => {
  it('renders list as comma-list', () => {
    expect(srRenderValue('list', ['a', 'b', 'c']).display).toBe('a, b, c');
  });
  it('renders id truncated', () => {
    expect(srRenderValue('id', 'cb2f9b0583567d4e').display).toMatch(/^cb2f9b0…7d4e$/);
  });
  it('renders time as ISO', () => {
    const r = srRenderValue('time', 1700000000000);
    expect(r.display.startsWith('2023-')).toBe(true);
  });
  it('renders numeric with locale', () => {
    expect(srRenderValue('numeric', 2500).display).toBe('2,500');
  });
  it('renders duration-named numeric as ms/s', () => {
    expect(srRenderValue('numeric', 250, 'duration_ms').display).toBe('250 ms');
    expect(srRenderValue('numeric', 2500, 'duration_ms').display).toBe('2.50 s');
  });
  it('renders binary as the binary placeholder with a tooltip', () => {
    expect(srRenderValue('binary', null).display).toBe('binary');
    expect(srRenderValue('binary', null).title).toContain('DATA_BINARY');
  });
  it('renders cat with a color when name is provided', () => {
    const r = srRenderValue('cat', 'ERROR', 'level');
    expect(r.display).toBe('ERROR');
    expect(r.color).toBe('var(--danger)');
  });
});
