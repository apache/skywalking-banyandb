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

import type {
  Group, StreamSchema, MeasureSchema, TraceSchema, PropertySchema,
  TagFamilySpec, FieldSpec, TagType, FieldType, CompressMethod, EncodingMethod,
} from './schema.js';

export interface ApiResponse<T> {
  readonly data?: T;
  readonly error?: string;
}

export interface GroupListResponse {
  readonly groups: Group[];
}

export interface ResourceListResponse<T> {
  readonly items: T[];
}

// Group CRUD

export interface CreateGroupRequest {
  readonly group: {
    readonly metadata: { readonly name: string };
    readonly catalog: Group['catalog'];
    readonly resourceOpts: {
      readonly shardNum: number;
      readonly segmentInterval: string;
      readonly ttl: string;
    };
  };
}

export interface UpdateGroupRequest {
  readonly group: {
    readonly metadata: { readonly name: string };
    readonly resourceOpts: {
      readonly shardNum: number;
      readonly segmentInterval: string;
      readonly ttl: string;
    };
  };
}

// Stream CRUD

export interface CreateStreamRequest {
  readonly stream: Omit<StreamSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateStreamRequest {
  readonly stream: StreamSchema;
}

// Measure CRUD

export interface CreateMeasureRequest {
  readonly measure: Omit<MeasureSchema, 'metadata'> & {
    readonly metadata: { readonly name: string; readonly group: string };
  };
}

export interface UpdateMeasureRequest {
  readonly measure: MeasureSchema;
}

// Query

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

// Re-export schema types for convenience
export type {
  Group, StreamSchema, MeasureSchema, TraceSchema, PropertySchema,
  TagFamilySpec, FieldSpec, TagType, FieldType, CompressMethod, EncodingMethod,
};
