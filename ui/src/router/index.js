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

import { createRouter, createWebHistory } from 'vue-router';
import Header from '@/components/Header/index.vue';
import { MENU_DEFAULT_PATH } from '@/components/Header/components/constants';
import { BASE_PATH } from './constants';

// Import route modules
import dashboardRoute from './modules/dashboard';
import streamRoutes from './modules/stream';
import measureRoutes from './modules/measure';
import propertyRoutes from './modules/property';
import traceRoutes from './modules/trace';
import queryRoute from './modules/query';

/**
 * Router configuration for BanyanDB UI
 * 
 * Route structure:
 * - / : Redirects to /banyandb
 * - /banyandb : Main application layout with header
 *   - /dashboard : Dashboard view
 *   - /stream : Stream schema management
 *   - /measure : Measure schema management
 *   - /property : Property management
 *   - /trace : Trace schema management
 *   - /query : BydbQL query interface
 * - /* : 404 error page
 * Structure: /banyandb/{module}/{operation}
 * Modules: dashboard, stream, measure, property, trace, query
 */
const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    // Root redirect
    {
      path: '/',
      redirect: BASE_PATH,
    },
    // Main application routes
    {
      path: BASE_PATH,
      component: Header,
      name: 'banyandb',
      redirect: MENU_DEFAULT_PATH,
      meta: { keepAlive: false },
      children: [
        dashboardRoute,
        streamRoutes,
        measureRoutes,
        propertyRoutes,
        traceRoutes,
        queryRoute,
      ],
    },
    // 404 Not Found
    {
      path: '/:pathMatch(.*)',
      name: 'NotFound',
      component: Header,
      meta: { keepAlive: false },
      children: [
        {
          path: '/:pathMatch(.*)',
          name: 'error',
          component: () => import('@/views/Errors/NotFound.vue'),
        },
      ],
    },
  ],
});

export default router;
