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
import { parsePrometheusText, sumOf } from '../src/lib/prometheus.js';
import { parseMonitorRoles } from '../src/config.js';

describe('parsePrometheusText', () => {
  it('reads names, labels and values and skips comments', () => {
    const samples = parsePrometheusText([
      '# HELP go_goroutines Number of goroutines.',
      '# TYPE go_goroutines gauge',
      'go_goroutines 175',
      'banyandb_system_disk{kind="used",path="/data/a,b"} 3.39689472e+08',
      'weird{msg="say \\"hi\\"\\n",x="\\\\"} +Inf',
      'not a sample',
      '',
    ].join('\n'));
    expect(samples).toEqual([
      { name: 'go_goroutines', labels: {}, value: 175 },
      { name: 'banyandb_system_disk', labels: { kind: 'used', path: '/data/a,b' }, value: 339689472 },
      { name: 'weird', labels: { msg: 'say "hi"\n', x: '\\' }, value: Infinity },
    ]);
  });

  it('sums the samples whose labels match, and is undefined when none do', () => {
    const samples = parsePrometheusText('m{method="query"} 2\nm{method="query",g="b"} 3\nm{method="write"} 7\n');
    expect(sumOf(samples, 'm', { method: 'query' })).toBe(5);
    expect(sumOf(samples, 'm')).toBe(12);
    expect(sumOf(samples, 'missing')).toBeUndefined();
  });
});

describe('parseMonitorRoles', () => {
  it('defaults to admin and maps role aliases', () => {
    expect(parseMonitorRoles(undefined)).toEqual(['admin']);
    expect(parseMonitorRoles('admin, viewer')).toEqual(['admin', 'readonly']);
  });

  it('rejects unknown or empty role lists', () => {
    expect(() => parseMonitorRoles('root')).toThrow(/unknown role "root"/);
    expect(() => parseMonitorRoles(' , ')).toThrow(/at least one role/);
  });
});
