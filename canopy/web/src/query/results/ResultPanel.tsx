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

// ResultPanel.tsx — shared result-panel chrome for every query catalog.
// Renders the Result/Trace tab bar, execution metadata, and an optional
// right-side slot for catalog-specific view-mode pills (Table/JSON/Export,
// Console/Raw JSON/Wrap, Spans/Group by trace/Raw JSON, etc.).

import React from 'react';
import type { QueryResponse } from 'canopy-shared';

type Catalog = 'measures' | 'streams' | 'traces' | 'topn';

const CATALOG_LABEL: Record<Catalog, string> = {
  measures: 'data points',
  streams: 'elements',
  traces: 'spans',
  topn: 'rows',
};

interface ResultPanelProps {
  readonly catalog: Catalog;
  readonly response: QueryResponse;
  /** Milliseconds the query took to execute. */
  readonly execMs?: number;
  /** Whether the query was run with WITH QUERY_TRACE. */
  readonly traceEnabled: boolean;
  /** Whether the Trace tab is currently active. */
  readonly showTrace: boolean;
  /** Toggle between Result and Trace tabs. */
  readonly setShowTrace: (v: boolean) => void;
  /** Optional right-side content, usually view-mode segmented pills. */
  readonly actions?: React.ReactNode;
  /** Optional content rendered below the result bar (e.g., a secondary toolbar). */
  readonly subBar?: React.ReactNode;
  readonly children: React.ReactNode;
}

export function ResultPanel({
  catalog,
  response,
  execMs,
  traceEnabled,
  showTrace,
  setShowTrace,
  actions,
  subBar,
  children,
}: ResultPanelProps) {
  const count = response.elements?.length ?? 0;
  const metaParts: string[] = [];
  if (execMs !== undefined && execMs >= 0) {
    metaParts.push(`executed in ${execMs < 1 ? execMs.toFixed(2) : execMs.toFixed(1)} ms`);
  }
  if (count > 0) {
    metaParts.push(`${count.toLocaleString('en-US')} ${CATALOG_LABEL[catalog]}`);
  }

  return (
    <div className="result-card">
      <div className="result-bar">
        <div className="result-tabs">
          <button
            type="button"
            className={'result-tab' + (!showTrace ? ' is-active' : '')}
            onClick={() => setShowTrace(false)}
          >
            Result
          </button>
          <button
            type="button"
            className={'result-tab' + (showTrace ? ' is-active' : '')}
            onClick={() => setShowTrace(true)}
          >
            Trace
            {traceEnabled && <span className="mr-trace-on" title="WITH QUERY_TRACE enabled" />}
          </button>
        </div>
        {metaParts.length > 0 && (
          <span className="result-status"><span className="rs meta">{metaParts.join(' · ')}</span></span>
        )}
        {actions && <div className="result-actions">{actions}</div>}
      </div>
      {subBar}
      <div className="rv-root">{children}</div>
    </div>
  );
}
