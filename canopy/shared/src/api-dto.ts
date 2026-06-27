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

// DTOs will be derived from live BanyanDB responses in M2.
// Placeholders here to satisfy TypeScript imports.

export interface ApiResponse<T> {
  readonly data?: T;
  readonly error?: string;
}

export interface GroupListResponse {
  readonly groups: import('./schema').Group[];
}

export interface QueryRequest {
  readonly groups: string[];
  readonly projection?: { readonly tagFamilies: Array<{ readonly name: string; readonly tags: string[] }> };
  readonly criteria?: unknown;
  readonly orderBy?: { readonly indexRuleName: string; readonly sort: string };
  readonly limit?: number;
  readonly offset?: number;
  readonly timeRange?: { readonly begin: string; readonly end: string };
}

export interface QueryResponse {
  readonly elements?: unknown[];
}
