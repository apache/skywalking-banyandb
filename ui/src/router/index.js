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
import HomeView from '../views/HomeView.vue'
import NotFoundView from '../views/NotFoundView.vue'
import DatabaseView from '../views/DatabaseView.vue'
import AboutView from '../views/AboutView.vue'
import StructureView from '../views/StructureView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/home'
    },
    {
      path: '/home',
      name: 'Home',
      component: HomeView,
      meta: {
        keepAlive: false,
      }
    },
    {
      path: '/database',
      name: 'Database',
      component: DatabaseView,
      meta: {
        keepAlive: true,
      }
    },
    {
      path: '/structure',
      name: 'StructureView',
      component: StructureView,
      meta: {
        keepAlive: false,
      }
    },
    {
      path: '/about',
      name: 'AboutView',
      component: AboutView,
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
