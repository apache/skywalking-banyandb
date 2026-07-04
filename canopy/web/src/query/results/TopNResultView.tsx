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

// TopNResultView.tsx — Top-N data result view (leaderboard).
// Ported from .handoff-import/banyandb/project/topn-results.jsx.
// Consumes the TopN DATA endpoint output (NOT TopN SCHEMA — that's a later
// milestone). Flat ranked rows with entity tags + value; per-interval vs
// rolled-up is reflected by row timestamps.

import React from 'react';
import type { QueryResponse } from 'canopy-shared';

interface Props {
  readonly response: QueryResponse;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
}

type Elem = Record<string, unknown>;

export function TopNResultView({ response, showTrace, setShowTrace }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  if (elements.length === 0) {
    return <div className="rv-empty">No leaderboard data.</div>;
  }
  // Stable column set: all keys except value/timestamp, sorted alphabetically
  const allKeys = new Set<string>();
  for (const e of elements) for (const k of Object.keys(e)) allKeys.add(k);
  const tagCols = [...allKeys].filter((k) => k !== 'value' && k !== 'timestamp').sort();
  const max = Math.max(...elements.map((e) => Number(e.value ?? 0)));
  return (
    <div className="rv-root">
      <div className="rv-tabs">
        <button type="button" className="rv-tab is-on">Leaderboard</button>
        {showTrace && (
          <button type="button" className="rv-tab is-on" onClick={() => setShowTrace(!showTrace)}>Trace</button>
        )}
      </div>
      <div className="rv-table-wrap">
        <table className="rv-table rv-topn-table">
          <thead>
            <tr>
              <th>#</th>
              {tagCols.map((c) => <th key={c}>{c}</th>)}
              <th>value</th>
            </tr>
          </thead>
          <tbody>
            {elements.map((e, i) => {
              const v = Number(e.value ?? 0);
              const pct = max ? Math.max(0, Math.min(100, (v / max) * 100)) : 0;
              return (
                <tr key={i}>
                  <td>{i + 1}</td>
                  {tagCols.map((c) => <td key={c}>{String(e[c] ?? '')}</td>)}
                  <td className="rv-topn-val">
                    <span className="rv-topn-bar" style={{ width: `${pct.toFixed(1)}%` }} />
                    <span className="rv-topn-vtxt">{v.toFixed(2)}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}