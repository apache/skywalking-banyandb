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

export enum TagType {
  UNSPECIFIED = 'TAG_TYPE_UNSPECIFIED',
  STRING = 'TAG_TYPE_STRING',
  INT64 = 'TAG_TYPE_INT64',
  FLOAT64 = 'TAG_TYPE_FLOAT64',
  STRING_ARRAY = 'TAG_TYPE_STRING_ARRAY',
  INT64_ARRAY = 'TAG_TYPE_INT64_ARRAY',
  DATA_BINARY = 'TAG_TYPE_DATA_BINARY',
}

export enum FieldType {
  UNSPECIFIED = 'FIELD_TYPE_UNSPECIFIED',
  STRING = 'FIELD_TYPE_STRING',
  INT64 = 'FIELD_TYPE_INT64',
  FLOAT64 = 'FIELD_TYPE_FLOAT64',
  DATA_BINARY = 'FIELD_TYPE_DATA_BINARY',
}

export enum CompressMethod {
  UNSPECIFIED = 'COMPRESSION_METHOD_UNSPECIFIED',
  ZSTD = 'COMPRESSION_METHOD_ZSTD',
}

export enum EncodingMethod {
  UNSPECIFIED = 'ENCODING_METHOD_UNSPECIFIED',
  GORILLA = 'ENCODING_METHOD_GORILLA',
}

export enum IndexType {
  TREE = 'INDEX_TYPE_TREE',
  INVERTED = 'INDEX_TYPE_INVERTED',
}

export interface Group {
  readonly name: string;
  readonly catalog: 'CATALOG_STREAM' | 'CATALOG_MEASURE' | 'CATALOG_PROPERTY' | 'CATALOG_UNSPECIFIED';
  readonly resourceOpts: {
    readonly shardNum: number;
    readonly segmentInterval: string;
    readonly ttl: string;
  };
  readonly updatedAt?: string;
}

export interface TagSpec {
  readonly name: string;
  readonly type: TagType;
  readonly indexedOnly?: boolean;
}

export interface TagFamilySpec {
  readonly name: string;
  readonly tags: TagSpec[];
}

export interface FieldSpec {
  readonly name: string;
  readonly fieldType: FieldType;
  readonly compressionMethod?: CompressMethod;
  readonly encodingMethod?: EncodingMethod;
}

export interface StreamSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly tagFamilies: TagFamilySpec[];
  readonly entity: {
    readonly tagNames: string[];
  };
}

export interface MeasureSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly tagFamilies: TagFamilySpec[];
  readonly fields: FieldSpec[];
  readonly entity: {
    readonly tagNames: string[];
  };
  readonly interval: string;
  readonly indexMode?: boolean;
}

export interface TraceSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly tagFamilies: TagFamilySpec[];
  readonly entity: {
    readonly tagNames: string[];
  };
  readonly traceIdTagName: string;
  readonly spanIdTagName: string;
  readonly timestampTagName: string;
}

export interface PropertySchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly tags: TagSpec[];
}

export interface IndexRuleSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly tags: string[];
  readonly type: IndexType;
  readonly analyzer?: string;
}

export interface IndexRuleBindingSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly rules: string[];
  readonly subject: {
    readonly name: string;
    readonly catalog: string;
  };
  readonly beginAt: string;
  readonly expireAt: string;
}

export interface TopNSchema {
  readonly metadata: {
    readonly name: string;
    readonly group: string;
  };
  readonly sourceMeasure: string;
  readonly fieldName: string;
  readonly fieldValueSort: 'SORT_ASC' | 'SORT_DESC' | 'SORT_UNSPECIFIED';
  readonly groupByTagNames: string[];
  readonly countersNumber: number;
}
