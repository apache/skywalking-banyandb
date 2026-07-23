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
import measure from './measure-query-response.json';
import stream from './stream-query-response.json';
import trace from './trace-query-response.json';
import topn from './topn-data-response.json';
import manifest from './manifest.json';

describe('recorded-real query fixtures', () => {
  it('manifest references all four response fixtures', () => {
    expect(manifest.fixtures).toEqual([
      'measure-query-response.json',
      'stream-query-response.json',
      'trace-query-response.json',
      'topn-data-response.json',
    ]);
    expect(manifest.banyandbVersion).toBeTruthy();
    expect(manifest.capturedAt).toBeTruthy();
  });

  it('measure fixture uses BanyanDB monorepo wire shape: data_points[].tag_families + fields', () => {
    const dps = (measure as { data_points: unknown[] }).data_points;
    expect(dps.length).toBeGreaterThan(0);
    const first = dps[0] as { timestamp: string; tag_families: { tags: { key: string; value: string }[] }[]; fields: { name: string; value: { float?: number } }[] };
    expect(first.timestamp).toBeTruthy();
    const tagKeys = first.tag_families.flatMap((f) => f.tags.map((t) => t.key));
    expect(tagKeys).toContain('host_id');
    expect(tagKeys).toContain('region');
    expect(first.fields[0].name).toBe('cpu');
    expect(typeof first.fields[0].value.float).toBe('number');
  });

  it('stream fixture uses BanyanDB monorepo wire shape: elements[].tag_families[].tags', () => {
    const elements = (stream as { elements: { element_id: string; tag_families: { tags: { key: string; value: string }[] }[] }[] }).elements;
    expect(elements.length).toBeGreaterThanOrEqual(8);
    const tagKeys = elements.flatMap((e) => e.tag_families.flatMap((f) => f.tags.map((t) => t.key)));
    // Distinct keys must include the role-inference probes.
    expect(new Set(tagKeys)).toEqual(new Set(['service', 'level', 'trace_id', 'duration_ms', 'body']));
    const distinctLevels = new Set(elements.flatMap((e) => e.tag_families.flatMap((f) => f.tags.filter((t) => t.key === 'level').map((t) => t.value))));
    expect(distinctLevels.size).toBeGreaterThanOrEqual(3); // INFO / WARN / ERROR / DEBUG
    const distinctServices = new Set(elements.flatMap((e) => e.tag_families.flatMap((f) => f.tags.filter((t) => t.key === 'service').map((t) => t.value))));
    expect(distinctServices.size).toBeGreaterThanOrEqual(3);
  });

  it('trace fixture uses BanyanDB monorepo wire shape: elements[].span_id/trace_id + optional tag_families.bytes', () => {
    const elements = (trace as { elements: { span_id: string; trace_id: string; tag_families?: { tags: { key: string; value: string }[] }[] }[] }).elements;
    expect(elements.length).toBeGreaterThanOrEqual(8);
    const withBytes = elements.filter((e) => e.tag_families?.some((f) => f.tags.some((t) => t.key === 'bytes')));
    expect(withBytes.length).toBeGreaterThan(0);
  });

  it('topn fixture uses BanyanDB monorepo wire shape: lists[].items[].entity + .value', () => {
    const lists = (topn as { lists: { items: { entity: { key: string; value: string }[]; value: { float?: number } }[] }[] }).lists;
    expect(lists.length).toBeGreaterThan(0);
    expect(lists[0].items.length).toBeGreaterThan(0);
    const first = lists[0].items[0];
    const entityKeys = first.entity.map((t) => t.key);
    expect(entityKeys).toContain('host_id');
    expect(typeof first.value.float).toBe('number');
  });

  it('all fixtures are version-stamped against the current monorepo (no 0.9.x pin)', () => {
    for (const f of [measure, stream, trace, topn]) {
      expect((f as { banyandbVersion?: string }).banyandbVersion).toBe('monorepo-current');
      expect((f as { wireShape?: string }).wireShape).toBeTruthy();
    }
  });
});