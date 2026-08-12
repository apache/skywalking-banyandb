/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TraceView.tsx — shared query-trace renderer for all catalog result views.
//
// BanyanDB nests the query trace inside the catalog-specific result
// (measureResult.trace, streamResult.trace, traceResult.trace, topnResult.trace)
// and represents it as a tree of spans, not a flat stages list.

import React from 'react';
import type { QueryResponse, QueryTrace, QueryTraceSpan } from 'canopy-shared';

interface SpanNode extends QueryTraceSpan {
  readonly id: string;
  readonly depth: number;
  readonly durationMs: number;
}

function nanoToMs(nanos: string | undefined): number {
  if (!nanos) return 0;
  const n = Number(nanos);
  return Number.isFinite(n) ? n / 1_000_000 : 0;
}

function flattenSpans(spans: ReadonlyArray<QueryTraceSpan> | undefined, depth = 0, prefix = ''): SpanNode[] {
  if (!spans) return [];
  const out: SpanNode[] = [];
  for (let i = 0; i < spans.length; i++) {
    const span = spans[i];
    const id = prefix ? `${prefix}-${i}` : String(i);
    out.push({
      ...span,
      id,
      depth,
      durationMs: nanoToMs(span.duration),
    });
    if (span.children && span.children.length > 0) {
      out.push(...flattenSpans(span.children, depth + 1, id));
    }
  }
  return out;
}

function maxDuration(nodes: SpanNode[]): number {
  return Math.max(...nodes.map((n) => n.durationMs), 0.001);
}

/** Extract the query-trace object from any catalog-specific BydbQL response.
 *  Handles both protojson camelCase and the typed response aliases. */
export function extractTrace(response: QueryResponse): QueryTrace | undefined {
  const r = response as unknown as Record<string, unknown>;
  const measure = (r.measureResult ?? r.measure_result) as { trace?: QueryTrace } | undefined;
  if (measure?.trace) return measure.trace;
  const stream = (r.streamResult ?? r.stream_result) as { trace?: QueryTrace } | undefined;
  if (stream?.trace) return stream.trace;
  const trace = (r.traceResult ?? r.trace_result) as { trace?: QueryTrace } | undefined;
  if (trace?.trace) return trace.trace;
  const topn = (r.topnResult ?? r.topn_result) as { trace?: QueryTrace } | undefined;
  if (topn?.trace) return topn.trace;
  return undefined;
}

export function TraceDisabled() {
  return (
    <div className="mtrace-off">
      <span className="mono">WITH QUERY_TRACE</span> is not enabled for this query. Turn on the query-trace toggle (or add it in code) to inspect span timings here.
    </div>
  );
}

export function TraceView({ response }: { readonly response: QueryResponse }) {
  const trace = extractTrace(response);
  const nodes = flattenSpans(trace?.spans);
  const total = nodes.length > 0 ? nodes[0].durationMs : 0;
  const maxDur = maxDuration(nodes);

  if (!trace || nodes.length === 0) {
    return (
      <div className="mtrace-off">
        No trace data returned. The query may have run without <span className="mono">WITH QUERY_TRACE</span> or the upstream trace was empty.
      </div>
    );
  }

  return (
    <div className="mtrace">
      <div className="mtrace-head">
        <span className="mono">query_trace</span>
        <span className="mtrace-sub">{nodes.length} spans · {total.toFixed(2)} ms total · traceId {trace.traceId ?? '—'}</span>
      </div>
      <div className="mtrace-rows">
        {nodes.map((node) => (
          <TraceRow key={node.id} node={node} maxDur={maxDur} />
        ))}
      </div>
    </div>
  );
}

function TraceRow({ node, maxDur }: { readonly node: SpanNode; readonly maxDur: number }) {
  const tags = node.tags ?? [];
  const detailParts: string[] = [];
  for (const tag of tags) {
    if (tag.key === 'plan') detailParts.push(`plan: ${tag.value}`);
    else if (tag.key === 'rows_in' || tag.key === 'rows_out') detailParts.push(`${tag.key}: ${tag.value}`);
    else if (tag.key === 'limit_n') detailParts.push(`limit ${tag.value}`);
  }
  const detail = detailParts.length > 0 ? detailParts.join(' · ') : (tags.length > 0 ? `${tags.length} tags` : 'span');
  const dur = node.durationMs;
  const width = maxDur > 0 ? (dur / maxDur) * 100 : 0;

  return (
    <div className={`mtrace-row depth-${Math.min(node.depth, 2)}`} title={tags.map((t) => `${t.key}=${t.value}`).join('\n')}>
      <span className="mtrace-name mono">{node.message ?? '—'}</span>
      <span className="mtrace-detail">{detail}</span>
      <span className="mtrace-bar-wrap">
        <span className="mtrace-bar" style={{ width: `${Math.max(3, width)}%` }} />
      </span>
      <span className="mtrace-dur mono">{dur.toFixed(2)} ms</span>
    </div>
  );
}
