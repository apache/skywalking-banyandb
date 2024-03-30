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

// Option 2: Proxy all traffic starting with "/api" to http://localhost:8081

import { fileURLToPath, URL } from 'node:url'
import AutoImport from 'unplugin-auto-import/vite'
import Components from 'unplugin-vue-components/vite'
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers'
import vue from '@vitejs/plugin-vue'
import {loadEnv} from 'vite'

// https://vitejs.dev/config/
export default ({ mode }) => {
  const { VITE_API_PROXY_TARGET, VITE_MONITOR_PROXY_TARGET } = loadEnv(mode, process.cwd());

  return {
    plugins: [
      // ...
      AutoImport({
        resolvers: [ElementPlusResolver()],
      }),
      Components({
        resolvers: [ElementPlusResolver()],
      }),
      vue()
    ],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url))
      }
    },
    server: {
      proxy: {
        "^/api": {
          target: `${VITE_API_PROXY_TARGET || "http://127.0.0.1:17913"}`,
          changeOrigin: true,
        },
        "^/monitoring": {
          target: `${VITE_MONITOR_PROXY_TARGET || "http://127.0.0.1:2121"}`,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/monitoring/, ''),
        },
      },
    }
  };
};