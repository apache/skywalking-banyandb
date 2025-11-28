/**
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

interface TagValue {
  str?: { value: string };
  int?: { value: number };
  float?: { value: number };
  binaryData?: unknown;
}

interface Tag {
  key: string;
  value: TagValue;
}

interface TagFamily {
  tags?: Tag[];
}

interface FieldValue {
  int?: { value: number };
  float?: { value: number };
  str?: { value: string };
  binaryData?: unknown;
}

interface Field {
  name: string;
  value: FieldValue;
}

interface DataPoint {
  timestamp?: string | number;
  sid?: string;
  version?: string | number;
  tagFamilies?: TagFamily[];
  fields?: Field[];
}

interface StreamResult {
  elements?: unknown[];
}

interface MeasureResult {
  dataPoints?: DataPoint[];
  data_points?: DataPoint[];
}

interface TraceResult {
  elements?: unknown[];
}

interface PropertyResult {
  items?: unknown[];
}

interface TopNResult {
  lists?: unknown[];
}

export interface QueryRequest {
  query: string;
}

export interface QueryResponse {
  // Response can be either wrapped in result or direct
  result?: {
    streamResult?: StreamResult;
    measureResult?: MeasureResult;
    traceResult?: TraceResult;
    propertyResult?: PropertyResult;
    topnResult?: TopNResult;
  };
  // Or directly at top level
  streamResult?: StreamResult;
  measureResult?: MeasureResult;
  traceResult?: TraceResult;
  propertyResult?: PropertyResult;
  topnResult?: TopNResult;
}

export interface Group {
  metadata?: {
    name?: string;
  };
}

export interface ResourceMetadata {
  metadata?: {
    name?: string;
    group?: string;
  };
  noSort: boolean;
}
