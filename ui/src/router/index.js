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

import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'
import NotFoundView from '../views/NotFoundView.vue'
import StreamView from '../views/StreamView.vue'
import PropertyView from '../views/PropertyView.vue'
import MeasureView from '../views/MeasureView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/dashboard'
    },
    {
      path: '/dashboard',
      name: 'Dashboard',
      component: DashboardView,
      meta: {
        keepAlive: false,
      }
    },
    {
      path: '/stream',
      name: 'Stream',
      component: StreamView,
      meta: {
        keepAlive: true,
      }
    },
    {
      path: '/measure',
      name: 'Measure',
      component: MeasureView,
      meta: {
        keepAlive: false,
      }
    },
    {
      path: '/property',
      name: 'Property',
      component: PropertyView,
      meta: {
        keepAlive: false,
      }
    },
    {
      // will match everything
      path: '/:pathMatch(.*)',
      name: 'NotFound',
      component: NotFoundView,
      meta: {
        keepAlive: false,
      }
    },
  ]
})

export default router
