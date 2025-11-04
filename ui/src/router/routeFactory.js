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

import { ROUTE_PARAMS } from './constants';

/**
 * Route Factory Functions
 *
 * Generate route configurations with consistent patterns.
 * All routes use relative paths (nested under parent routes in modules).
 * CRUD routes are defined inline in modules for Vite static import compatibility.
 */

/**
 * Creates start page route
 * @param {string} schemaType - Schema type (stream, measure, trace, property)
 */
export function createStartRoute(schemaType) {
  return {
    path: 'start',
    name: `${schemaType}Start`,
    component: () => import('@/components/Start/index.vue'),
    meta: { type: schemaType },
  };
}

/**
 * Creates index rule routes (read, create, edit)
 * @param {string} schemaType - Schema type
 */
export function createIndexRuleRoutes(schemaType) {
  return [
    {
      path: `${ROUTE_PARAMS.OPERATOR_READ}`,
      name: `${schemaType}-index-rule`,
      component: () => import('@/components/IndexRule/index.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_CREATE}`,
      name: `${schemaType}-create-index-rule`,
      component: () => import('@/components/IndexRule/Editor.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_EDIT}`,
      name: `${schemaType}-edit-index-rule`,
      component: () => import('@/components/IndexRule/Editor.vue'),
    },
  ];
}

/**
 * Creates index rule binding routes (read, create, edit)
 * @param {string} schemaType - Schema type
 */
export function createIndexRuleBindingRoutes(schemaType) {
  return [
    {
      path: `${ROUTE_PARAMS.OPERATOR_READ}`,
      name: `${schemaType}-index-rule-binding`,
      component: () => import('@/components/IndexRuleBinding/index.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_CREATE}`,
      name: `${schemaType}-create-index-rule-binding`,
      component: () => import('@/components/IndexRuleBinding/Editor.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_EDIT}`,
      name: `${schemaType}-edit-index-rule-binding`,
      component: () => import('@/components/IndexRuleBinding/Editor.vue'),
    },
  ];
}

/**
 * Creates TopN aggregation routes (measure only)
 * @param {string} schemaType - Schema type (typically 'measure')
 */
export function createTopNAggRoutes(schemaType) {
  return [
    {
      path: `${ROUTE_PARAMS.OPERATOR_READ}`,
      name: `${schemaType}-topn-agg`,
      component: () => import('@/components/TopNAggregation/index.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_CREATE}`,
      name: `${schemaType}-create-topn-agg`,
      component: () => import('@/components/TopNAggregation/Editor.vue'),
    },
    {
      path: `${ROUTE_PARAMS.OPERATOR_EDIT}`,
      name: `${schemaType}-edit-topn-agg`,
      component: () => import('@/components/TopNAggregation/Editor.vue'),
    },
  ];
}
