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

// TopNTracePanel.tsx — Top-N query-trace view.
//
// Delegates to the shared TraceView, which renders BanyanDB's actual
// query-trace spans when WITH QUERY_TRACE is enabled and the response
// carries a trace object. When the trace is missing the TraceView falls
// back to its own "WITH QUERY_TRACE is not enabled" message — keeping
// the disabled-state UI in a single place rather than duplicating it
// here.

import type { QueryResponse } from 'canopy-shared';
import { TraceView } from './TraceView.js';

interface Props {
  readonly response: QueryResponse;
}

export function TopNTracePanel({ response }: Props) {
  return <TraceView response={response} />;
}