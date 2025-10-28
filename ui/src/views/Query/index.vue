<!--
  ~ Licensed to Apache Software Foundation (ASF) under one or more contributor
  ~ license agreements. See the NOTICE file distributed with
  ~ this work for additional information regarding copyright
  ~ ownership. Apache Software Foundation (ASF) licenses this file to you under
  ~ the Apache License, Version 2.0 (the "License"); you may
  ~ not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

<script setup>
  import { ref, computed, onMounted, nextTick } from 'vue';
  import { ElMessage } from 'element-plus';
  import { executeBydbQLQuery } from '@/api/index';
  import CodeMirror from '@/components/CodeMirror/index.vue';

  const queryText = ref('');
  const queryResult = ref(null);
  const loading = ref(false);
  const error = ref(null);
  const executionTime = ref(0);

  // Example queries for users
  const exampleQueries = [
    'FROM STREAM sw | LIMIT 10',
    'FROM MEASURE metrics | LIMIT 10',
    'FROM TRACE traces | LIMIT 10',
  ];

  const hasResult = computed(() => queryResult.value !== null);
  const resultType = computed(() => {
    if (!queryResult.value) return null;
    if (queryResult.value.stream_result) return 'stream';
    if (queryResult.value.measure_result) return 'measure';
    if (queryResult.value.property_result) return 'property';
    if (queryResult.value.trace_result) return 'trace';
    if (queryResult.value.topn_result) return 'topn';
    return 'unknown';
  });

  const formattedResult = computed(() => {
    if (!queryResult.value) return '';
    try {
      return JSON.stringify(queryResult.value, null, 2);
    } catch (e) {
      return String(queryResult.value);
    }
  });

  function setExampleQuery(query) {
    queryText.value = query;
  }

  async function executeQuery() {
    if (!queryText.value.trim()) {
      ElMessage.warning('Please enter a query');
      return;
    }

    loading.value = true;
    error.value = null;
    queryResult.value = null;
    const startTime = performance.now();

    try {
      const response = await executeBydbQLQuery({ query: queryText.value });
      const endTime = performance.now();
      executionTime.value = Math.round(endTime - startTime);

      if (response.error) {
        error.value = response.error.message || 'Failed to execute query';
        ElMessage.error(error.value);
      } else {
        queryResult.value = response;
        ElMessage.success(`Query executed successfully in ${executionTime.value}ms`);
      }
    } catch (err) {
      error.value = err.message || 'An unexpected error occurred';
      ElMessage.error(error.value);
    } finally {
      loading.value = false;
    }
  }

  function clearQuery() {
    queryText.value = '';
    queryResult.value = null;
    error.value = null;
    executionTime.value = 0;
  }

  // Setup keyboard shortcuts for CodeMirror
  onMounted(() => {
    nextTick(() => {
      // Find the CodeMirror instance and add keyboard shortcuts
      const codeMirrorElement = document.querySelector('.query-input .CodeMirror');
      if (codeMirrorElement && codeMirrorElement.CodeMirror) {
        const cm = codeMirrorElement.CodeMirror;
        cm.setOption('extraKeys', {
          'Ctrl-Enter': executeQuery,
          'Cmd-Enter': executeQuery,
        });
      }
    });
  });
</script>

<template>
  <div class="query-page">
    <el-card shadow="always" class="query-card">
      <template #header>
        <div class="card-header">
          <span class="header-title">BydbQL Query Console</span>
          <div class="header-actions">
            <el-button @click="clearQuery" size="small" :disabled="loading"> Clear </el-button>
            <el-button
              type="primary"
              @click="executeQuery"
              :loading="loading"
              size="small"
              :disabled="!queryText.trim()"
            >
              Execute Query
            </el-button>
          </div>
        </div>
      </template>

      <div class="query-section">
        <div class="query-input-header">
          <span class="input-label">Query</span>
          <span class="input-hint">Press Ctrl/Cmd + Enter to execute</span>
        </div>
        <div class="query-input-container">
          <CodeMirror
            v-model="queryText"
            :mode="'sql'"
            :lint="false"
            :readonly="false"
            theme="default"
            :style-active-line="true"
            :auto-refresh="true"
            class="query-input"
          />
        </div>

        <div class="examples-section">
          <span class="examples-label">Examples:</span>
          <el-button
            v-for="(query, index) in exampleQueries"
            :key="index"
            size="small"
            @click="setExampleQuery(query)"
            text
            class="example-button"
          >
            {{ query }}
          </el-button>
        </div>
      </div>
    </el-card>

    <el-card v-if="hasResult || error" shadow="always" class="result-card">
      <template #header>
        <div class="card-header">
          <div class="result-header-left">
            <span class="header-title">Query Results</span>
            <el-tag v-if="resultType" size="small" class="result-type-tag">
              {{ resultType.toUpperCase() }}
            </el-tag>
          </div>
          <span v-if="executionTime > 0" class="execution-time"> Execution time: {{ executionTime }}ms </span>
        </div>
      </template>

      <div class="result-section">
        <div v-if="error" class="error-message">
          <el-alert title="Error" type="error" :description="error" show-icon :closable="false" />
        </div>

        <div v-else-if="hasResult" class="result-content">
          <pre class="result-json">{{ formattedResult }}</pre>
        </div>
      </div>
    </el-card>

    <el-card shadow="always" class="info-card">
      <template #header>
        <div class="card-header">
          <span class="header-title">About BydbQL</span>
        </div>
      </template>
      <div class="info-content">
        <p>
          BydbQL (BanyanDB Query Language) provides a unified query interface for different data types in BanyanDB.
        </p>
        <ul>
          <li><strong>STREAM:</strong> Query stream data with time-series tags</li>
          <li><strong>MEASURE:</strong> Query aggregated metrics and measurements</li>
          <li><strong>PROPERTY:</strong> Query property data and metadata</li>
          <li><strong>TRACE:</strong> Query distributed tracing data</li>
          <li><strong>TopN:</strong> Query top N aggregation results</li>
        </ul>
        <p class="info-note">
          Note: Queries must include a FROM clause specifying the resource type and name (e.g., "FROM STREAM sw").
        </p>
      </div>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
  .query-page {
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 20px;
    max-width: 1400px;
    margin: 0 auto;
  }

  .query-card,
  .result-card,
  .info-card {
    width: 100%;
  }

  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 10px;
  }

  .header-title {
    font-size: 18px;
    font-weight: 600;
  }

  .header-actions {
    display: flex;
    gap: 10px;
  }

  .result-header-left {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .result-type-tag {
    font-weight: 600;
  }

  .execution-time {
    font-size: 14px;
    color: #606266;
  }

  .query-section {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .query-input-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .input-label {
    font-weight: 500;
    font-size: 14px;
    color: #303133;
  }

  .input-hint {
    font-size: 12px;
    color: #909399;
  }

  .query-input-container {
    border: 1px solid #dcdfe6;
    border-radius: 4px;
    overflow: hidden;
    
    :deep(.in-coder-panel) {
      height: 250px;
    }

    :deep(.CodeMirror) {
      border: none;
      height: 100%;
      font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
      font-size: 14px;
      line-height: 1.6;
    }

    :deep(.CodeMirror-scroll) {
      min-height: 250px;
    }
  }

  .examples-section {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    padding: 12px;
    background-color: #f5f7fa;
    border-radius: 4px;
  }

  .examples-label {
    font-size: 13px;
    font-weight: 500;
    color: #606266;
  }

  .example-button {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 12px;
  }

  .result-section {
    min-height: 100px;
  }

  .error-message {
    margin-bottom: 16px;
  }

  .result-content {
    background-color: #f5f7fa;
    border-radius: 4px;
    padding: 16px;
    overflow: auto;
    max-height: 600px;
  }

  .result-json {
    margin: 0;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 13px;
    line-height: 1.6;
    color: #303133;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .info-content {
    font-size: 14px;
    line-height: 1.8;
    color: #606266;

    p {
      margin: 0 0 12px 0;
    }

    ul {
      margin: 12px 0;
      padding-left: 24px;

      li {
        margin: 8px 0;

        strong {
          color: #303133;
        }
      }
    }

    .info-note {
      margin-top: 16px;
      padding: 12px;
      background-color: #f0f9ff;
      border-left: 4px solid #409eff;
      border-radius: 4px;
      font-size: 13px;
      color: #303133;
    }
  }

  @media (max-width: 768px) {
    .query-page {
      padding: 10px;
    }

    .card-header {
      flex-direction: column;
      align-items: flex-start;
    }

    .header-actions {
      width: 100%;
      justify-content: flex-end;
    }

    .examples-section {
      flex-direction: column;
      align-items: flex-start;
    }
  }
</style>

