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

export type Catalog = 'CATALOG_STREAM' | 'CATALOG_MEASURE' | 'CATALOG_PROPERTY';

export type SortOrder = 'SORT_ASC' | 'SORT_DESC' | 'SORT_UNSPECIFIED';

export type ConditionOp =
  | 'BINARY_OP_EQ'
  | 'BINARY_OP_NE'
  | 'BINARY_OP_LT'
  | 'BINARY_OP_GT'
  | 'BINARY_OP_LE'
  | 'BINARY_OP_GE'
  | 'BINARY_OP_HAVING'
  | 'BINARY_OP_NOT_HAVING'
  | 'BINARY_OP_IN'
  | 'BINARY_OP_NOT_IN'
  | 'BINARY_OP_MATCH';

export type LogicalOp = 'AND' | 'OR';

export interface WhereLeaf {
  readonly type: 'leaf';
  readonly tagFamily: string;
  readonly name: string;
  readonly op: ConditionOp;
  readonly value: string | string[];
}

export interface WhereGroup {
  readonly type: 'group';
  readonly op: LogicalOp;
  readonly children: WhereNode[];
}

export type WhereNode = WhereLeaf | WhereGroup;

export interface TimeRange {
  readonly begin: string;
  readonly end: string;
}

export interface QueryBuilderState {
  readonly catalog: Catalog;
  readonly group: string;
  readonly name: string;
  readonly tagProjection: Array<{ readonly family: string; readonly tags: string[] }>;
  readonly where: WhereGroup;
  readonly orderBy?: { readonly indexRuleName: string; readonly sort: SortOrder };
  readonly limit: number;
  readonly offset: number;
  readonly timeRange: TimeRange;
}
