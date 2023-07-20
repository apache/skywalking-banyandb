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
import Header from '@/components/Header/index.vue'
import NotFoundView from '../views/NotFoundView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/banyandb'
    },
    {
      path: '/banyandb',
      component: Header,
      name: 'banyandb',
      redirect: '/banyandb/dashboard',
      meta: {
        keepAlive: false,
      },
      children: [
        {
          path: '/banyandb/dashboard',
          name: 'dashboard',
          component: () => import('@/views/Dashboard/index.vue')
        },
        {
          path: '/banyandb/stream',
          name: 'streamHome',
          redirect: '/banyandb/stream/start',
          component: () => import('@/views/Stream/index.vue'),
          children: [
            {
              path: '/banyandb/stream/start',
              name: 'streamStart',
              component: () => import('@/components/Start/index.vue'),
              meta: {
                type: 'stream'
              }
            },
            {
              path: '/banyandb/stream/operator-read/:type/:operator/:group/:name',
              name: 'stream',
              component: () => import('@/views/Stream/stream.vue')
            },
            {
              path: '/banyandb/stream/operator-create/:type/:operator/:group',
              name: 'create-stream',
              component: () => import('@/views/Stream/createEdit.vue')
            },
            {
              path: '/banyandb/stream/operator-edit/:type/:operator/:group/:name',
              name: 'edit-stream',
              component: () => import('@/views/Stream/createEdit.vue')
            },
            {
              path: '/banyandb/stream/index-rule/operator-read/:type/:operator/:group/:name',
              name: 'index-rule',
              component: () => import('@/components/IndexRule/index.vue')
            },
            {
              path: '/banyandb/stream/index-rule/operator-create/:type/:operator/:group',
              name: 'create-index-rule',
              component: () => import('@/components/IndexRule/Editor.vue')
            },
            {
              path: '/banyandb/stream/index-rule/operator-edit/:type/:operator/:group/:name',
              name: 'edit-index-rule',
              component: () => import('@/components/IndexRule/Editor.vue')
            },
            {
              path: '/banyandb/stream/index-rule-binding/operator-read/:type/:operator/:group/:name',
              name: 'index-rule-binding',
              component: () => import('@/components/IndexRuleBinding/index.vue')
            },
            {
              path: '/banyandb/stream/index-rule-binding/operator-create/:type/:operator/:group',
              name: 'create-index-rule-binding',
              component: () => import('@/components/IndexRuleBinding/Editor.vue')
            },
            {
              path: '/banyandb/stream/index-rule-binding/operator-edit/:type/:operator/:group/:name',
              name: 'edit-index-rule-binding',
              component: () => import('@/components/IndexRuleBinding/Editor.vue')
            }
          ]
        },
        {
          path: '/banyandb/measure',
          name: 'measureHome',
          redirect: '/banyandb/measure/start',
          component: () => import('@/views/Measure/index.vue'),
          children: [
            {
              path: '/banyandb/measure/start',
              name: 'measureStart',
              component: () => import('@/components/Start/index.vue'),
              meta: {
                type: 'measure'
              }
            },
            {
              path: '/banyandb/measure/operator-read/:type/:operator/:group/:name',
              name: 'measure',
              component: () => import('@/views/Measure/measure.vue')
            },
            {
              path: '/banyandb/measure/operator-create/:type/:operator/:group',
              name: 'create-measure',
              component: () => import('@/views/Measure/createEdit.vue')
            },
            {
              path: '/banyandb/measure/operator-edit/:type/:operator/:group/:name',
              name: 'edit-measure',
              component: () => import('@/views/Stream/createEdit.vue')
            }
          ]
        },
        {
          path: '/banyandb/property',
          name: 'Property',
          component: () => import('@/views/Property/index.vue')
        }
      ]
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
