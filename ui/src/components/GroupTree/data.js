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

export const StageFields = [
  { label: 'Name', key: 'name' },
  { label: 'Shard Number', key: 'shardNum' },
  { label: 'TTL Unit', key: 'ttlUnit' },
  { label: 'TTL Number', key: 'ttlNum' },
  { label: 'Segment Interval Unit', key: 'segmentIntervalUnit' },
  { label: 'Segment Interval Number', key: 'segmentIntervalNum' },
  { label: 'Node Selector', key: 'nodeSelector' },
  { label: 'Close', key: 'close' },
];
export const StageConfig = {
  name: '',
  shardNum: 1,
  ttlUnit: 'UNIT_DAY',
  ttlNum: 3,
  segmentIntervalUnit: 'UNIT_DAY',
  segmentIntervalNum: 1,
  NodeSelector: '',
};

export const Rules = {
  name: [
    {
      required: true,
      message: 'Please enter the name of the group',
      trigger: 'blur',
    },
  ],
  catalog: [
    {
      required: true,
      message: 'Please select the type of the group',
      trigger: 'blur',
    },
  ],
  shardNum: [
    {
      required: true,
      message: 'Please select the shard num of the group',
      trigger: 'blur',
    },
  ],
  segmentIntervalUnit: [
    {
      required: true,
      message: 'Please select the segment interval unit of the group',
      trigger: 'blur',
    },
  ],
  segmentIntervalNum: [
    {
      required: true,
      message: 'Please select the segment Interval num of the group',
      trigger: 'blur',
    },
  ],
  ttlUnit: [
    {
      required: true,
      message: 'Please select the ttl unit of the group',
      trigger: 'blur',
    },
  ],
  ttlNum: [
    {
      required: true,
      message: 'Please select the ttl num of the group',
      trigger: 'blur',
    },
  ],
  stages: [
    {
      required: false,
      message: 'Please ass the stages of the group',
      trigger: 'blur',
    },
  ],
};

export const DefaultProps = {
  children: 'children',
  label: 'name',
};

export const TargetTypes = {
  Group: 'group',
  Resources: 'resources',
};
// catalog to group type
export const CatalogToGroupType = {
  CATALOG_MEASURE: 'measure',
  CATALOG_STREAM: 'stream',
  CATALOG_PROPERTY: 'property',
};

// group type to catalog
export const GroupTypeToCatalog = {
  measure: 'CATALOG_MEASURE',
  stream: 'CATALOG_STREAM',
  property: 'CATALOG_PROPERTY',
};

export const TypeMap = {
  topNAggregation: 'topn-agg',
  indexRule: 'index-rule',
  indexRuleBinding: 'index-rule-binding',
  children: 'children',
};
