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

// DataSource is a thin test seam only.
// The ONLY production data source is ApiDataSource (api.ts).
// NO MockDataSource exists — live-first by design.

import type {
  GroupListResponse, CreateGroupRequest, UpdateGroupRequest,
  CreateStreamRequest, UpdateStreamRequest,
  CreateMeasureRequest, UpdateMeasureRequest,
  CreateTraceRequest, UpdateTraceRequest,
  CreateIndexRuleRequest, UpdateIndexRuleRequest,
  CreateIndexRuleBindingRequest, UpdateIndexRuleBindingRequest,
  QueryRequest, QueryResponse,
  StreamSchema, MeasureSchema, TraceSchema, PropertySchema, Group,
  IndexRuleSchema, IndexRuleBindingSchema, TopNAggregationSchema,
} from 'canopy-shared';

export interface DataSource {
  // Groups
  listGroups(): Promise<GroupListResponse>;
  createGroup(req: CreateGroupRequest): Promise<Group>;
  updateGroup(name: string, req: UpdateGroupRequest): Promise<Group>;
  deleteGroup(name: string): Promise<void>;

  // Resources (read)
  listResourcesInGroup(type: string, group: string): Promise<(StreamSchema | MeasureSchema | TraceSchema | PropertySchema)[]>;
  getResource(type: string, group: string, name: string): Promise<StreamSchema | MeasureSchema | TraceSchema | PropertySchema>;

  // TopNAggregation (Top-N schema) — read
  // Returns the precomputed leaderboard definitions registered under the
  // given group. The query builder's FROM row uses this list (instead of
  // the measure list) when the user is composing a Top-N query.
  listTopNAggregations(group: string): Promise<TopNAggregationSchema[]>;

  // Stream CRUD
  createStream(req: CreateStreamRequest): Promise<StreamSchema>;
  updateStream(group: string, name: string, req: UpdateStreamRequest): Promise<StreamSchema>;

  // Measure CRUD
  createMeasure(req: CreateMeasureRequest): Promise<MeasureSchema>;
  updateMeasure(group: string, name: string, req: UpdateMeasureRequest): Promise<MeasureSchema>;

  // Trace CRUD
  createTrace(req: CreateTraceRequest): Promise<TraceSchema>;
  updateTrace(group: string, name: string, req: UpdateTraceRequest): Promise<TraceSchema>;

  // IndexRule CRUD
  listIndexRules(group: string): Promise<IndexRuleSchema[]>;
  getIndexRule(group: string, name: string): Promise<IndexRuleSchema>;
  createIndexRule(req: CreateIndexRuleRequest): Promise<IndexRuleSchema>;
  updateIndexRule(group: string, name: string, req: UpdateIndexRuleRequest): Promise<IndexRuleSchema>;
  deleteIndexRule(group: string, name: string): Promise<void>;

  // IndexRuleBinding CRUD
  listIndexRuleBindings(group: string): Promise<IndexRuleBindingSchema[]>;
  getIndexRuleBinding(group: string, name: string): Promise<IndexRuleBindingSchema>;
  createIndexRuleBinding(req: CreateIndexRuleBindingRequest): Promise<IndexRuleBindingSchema>;
  updateIndexRuleBinding(group: string, name: string, req: UpdateIndexRuleBindingRequest): Promise<IndexRuleBindingSchema>;
  deleteIndexRuleBinding(group: string, name: string): Promise<void>;

  // Generic delete
  deleteResource(type: string, group: string, name: string): Promise<void>;

  runQuery(request: QueryRequest): Promise<QueryResponse>;
}
