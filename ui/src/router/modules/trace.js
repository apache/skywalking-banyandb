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

import { BASE_PATH, SCHEMA_TYPES, ROUTE_PARAMS } from '../constants';
import {
  createStartRoute,
  createIndexRuleRoutes,
  createIndexRuleBindingRoutes,
} from '../routeFactory';

/**
 * Trace schema routes
 * Includes: CRUD, index rules, index rule bindings
 */
export default {
  path: `${BASE_PATH}/trace`,
  name: 'traceHome',
  redirect: `${BASE_PATH}/trace/start`,
  component: () => import('@/views/Trace/index.vue'),
  children: [
    createStartRoute(SCHEMA_TYPES.TRACE),
    {
      path: ROUTE_PARAMS.OPERATOR_READ,
      name: SCHEMA_TYPES.TRACE,
      component: () => import('@/components/Trace/TraceRead.vue'),
    },
    {
      path: ROUTE_PARAMS.OPERATOR_CREATE,
      name: `create-${SCHEMA_TYPES.TRACE}`,
      component: () => import('@/views/Trace/createEdit.vue'),
    },
    {
      path: ROUTE_PARAMS.OPERATOR_EDIT,
      name: `edit-${SCHEMA_TYPES.TRACE}`,
      component: () => import('@/views/Trace/createEdit.vue'),
    },
    ...createIndexRuleRoutes(SCHEMA_TYPES.TRACE),
    ...createIndexRuleBindingRoutes(SCHEMA_TYPES.TRACE),
  ],
};

