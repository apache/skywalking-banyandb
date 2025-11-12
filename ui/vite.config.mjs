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

import { fileURLToPath, URL } from 'node:url';
import AutoImport from 'unplugin-auto-import/vite';
import Components from 'unplugin-vue-components/vite';
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers';
import vue from '@vitejs/plugin-vue';
import { loadEnv } from 'vite';

export default ({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const { VITE_API_PROXY, VITE_MONITOR_PROXY } = env;
  const isProduction = mode === 'production';

  return {
    cacheDir: fileURLToPath(new URL('./node_modules/.vite-ui', import.meta.url)),
    plugins: [
      AutoImport({
        resolvers: [ElementPlusResolver()],
      }),
      Components({
        resolvers: [ElementPlusResolver()],
      }),
      vue(),
    ],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url)),
        'vue-demi': 'vue-demi/lib/index.mjs',
      },
    },
    optimizeDeps: {
      include: [
        '@tanstack/vue-query',
        'codemirror',
        'echarts',
        'element-plus/es',
        'vue-demi',
        'vue-router',
      ],
    },
    css: {
      preprocessorOptions: {
        scss: {
          api: 'modern-compiler',
        },
      },
    },
    esbuild: {
      drop: isProduction ? ['console', 'debugger'] : [],
    },
    define: {
      __VUE_OPTIONS_API__: true,
      __VUE_PROD_DEVTOOLS__: false,
    },
    build: {
      target: 'es2019',
      cssCodeSplit: true,
      sourcemap: false,
      chunkSizeWarningLimit: 1500,
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (!id.includes('node_modules')) {
              return;
            }

            if (id.includes('element-plus')) {
              return 'element-plus';
            }
            if (id.includes('codemirror')) {
              return 'codemirror';
            }
            if (id.includes('echarts')) {
              return 'echarts';
            }
            if (id.includes('@tanstack')) {
              return 'vue-query';
            }
            if (id.includes('vue-router')) {
              return 'vue-router';
            }
          },
        },
      },
    },
    server: {
      proxy: {
        '^/api': {
          target: `${VITE_API_PROXY || 'http://127.0.0.1:17913'}`,
          changeOrigin: true,
        },
        '^/monitoring': {
          target: `${VITE_MONITOR_PROXY || 'http://127.0.0.1:2121'}`,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/monitoring/, ''),
        },
      },
    },
  };
};
