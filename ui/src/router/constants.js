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

/**
 * Router Constants
 * Shared constants for router configuration
 */

/** Base path for all BanyanDB routes */
export const BASE_PATH = '/banyandb';

/** Route parameter patterns for CRUD operations */
export const ROUTE_PARAMS = {
  OPERATOR_READ: 'operator-read/:type/:operator/:group/:name',
  OPERATOR_CREATE: 'operator-create/:type/:operator/:group',
  OPERATOR_EDIT: 'operator-edit/:type/:operator/:group/:name',
};

/** Schema type identifiers */
export const SCHEMA_TYPES = {
  STREAM: 'stream',
  MEASURE: 'measure',
  PROPERTY: 'property',
  TRACE: 'trace',
};

/** Component paths for dynamic imports (reference only) */
export const COMPONENTS = {
  START: '@/components/Start/index.vue',
  INDEX_RULE: '@/components/IndexRule/index.vue',
  INDEX_RULE_EDITOR: '@/components/IndexRule/Editor.vue',
  INDEX_RULE_BINDING: '@/components/IndexRuleBinding/index.vue',
  INDEX_RULE_BINDING_EDITOR: '@/components/IndexRuleBinding/Editor.vue',
  TOPN_AGG: '@/components/TopNAggregation/index.vue',
  TOPN_AGG_EDITOR: '@/components/TopNAggregation/Editor.vue',
};

