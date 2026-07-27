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

// hooks.ts — TanStack Query hooks for M4 query execution.
// v1 uses useMutation (no cache) — queries are user-initiated and re-run on
// every builder change; caching per (catalog, group, resource, BydbQL) is a
// future optimization. The Plan §M4 §"useRunQuery mutation" calls this out
// as the consumer-facing hook so QueryConsole's Run button doesn't have to
// own mutation state.

import { useMutation, type UseMutationResult } from '@tanstack/react-query';
import { apiDataSource } from './api.js';
import type { QueryRequest, QueryResponse } from 'canopy-shared';

/**
 * useRunQuery — TanStack Query mutation hook that calls the BFF-backed
 * ApiDataSource.runQuery. Returns the standard useMutation shape so callers
 * can drive their UI off `isPending` / `isError` / `data`.
 */
export function useRunQuery(): UseMutationResult<QueryResponse, Error, QueryRequest> {
  return useMutation<QueryResponse, Error, QueryRequest>({
    mutationFn: (request: QueryRequest) => apiDataSource.runQuery(request),
  });
}