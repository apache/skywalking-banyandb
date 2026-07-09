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

import React, { useState } from 'react';
import type { QueryResponse } from 'canopy-shared';
import { ResultPanel } from './ResultPanel.js';
import { ResultEmpty } from './ResultEmpty.js';
import { TraceView } from './TraceView.js';

interface Props {
  readonly response: QueryResponse;
  readonly showTrace: boolean;
  readonly setShowTrace: (v: boolean) => void;
  readonly execMs?: number;
}

type Elem = Record<string, unknown>;

export function TopNResultView({ response, showTrace, setShowTrace, execMs }: Props) {
  const elements = (response.elements ?? []) as readonly Elem[];
  const [view, setView] = useState<'leaderboard' | 'json'>('leaderboard');

  const onExport = () => {
    const blob = new Blob([JSON.stringify(response, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `topn-result-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const actions = (
    <>
      <button type="button" className={'result-view-btn' + (view === 'leaderboard' ? ' is-on' : '')} onClick={() => setView('leaderboard')}>Leaderboard</button>
      <button type="button" className={'result-view-btn' + (view === 'json' ? ' is-on' : '')} onClick={() => setView('json')}>JSON</button>
      <button type="button" className="result-view-btn" onClick={onExport}>Export</button>
    </>
  );

  return (
    <ResultPanel catalog="topn" response={response} execMs={execMs} traceEnabled={showTrace} showTrace={showTrace} setShowTrace={setShowTrace} actions={actions}>
      {showTrace ? (
        <TraceView response={response} />
      ) : view === 'json' ? (
        <div className="rv-trace"><pre>{JSON.stringify(response, null, 2)}</pre></div>
      ) : elements.length === 0 ? (
        <ResultEmpty title="No leaderboard data" text="The Top-N query returned no ranked series for this measure and window." />
      ) : (
        <LeaderboardTable elements={elements} />
      )}
    </ResultPanel>
  );
}

function LeaderboardTable({ elements }: { elements: readonly Elem[] }) {
  const allKeys = new Set<string>();
  for (const e of elements) for (const k of Object.keys(e)) allKeys.add(k);
  const tagCols = [...allKeys].filter((k) => k !== 'value' && k !== 'timestamp').sort();
  const max = Math.max(...elements.map((e) => Number(e.value ?? 0)));
  return (
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
  );
}