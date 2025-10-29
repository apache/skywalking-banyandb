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
  import TopNTable from '@/components/common/TopNTable.vue';
  import MeasureAndStreamTable from '@/components/common/MeasureAndStreamTable.vue';
  import PropertyTable from '@/components/common/PropertyTable.vue';
  import TraceTable from '@/components/common/TraceTable.vue';
  import { CatalogToGroupType } from '@/components/common/data';

  const queryText = ref('');
  const queryResult = ref(null);
  const loading = ref(false);
  const error = ref(null);
  const executionTime = ref(0);

  // Example queries for users
  const exampleQueries = [
    `SELECT * FROM STREAM log in sw_recordsLog TIME > '-30m'`,
    `SELECT * FROM MEASURE service_cpm_minute in sw_metricsMinute TIME > '30m'`,
  ];

  const hasResult = computed(() => queryResult.value !== null);
  const resultType = computed(() => {
    if (!queryResult.value) return null;
    if (queryResult.value.streamResult) return CatalogToGroupType.CATALOG_STREAM;
    if (queryResult.value.measureResult) return CatalogToGroupType.CATALOG_MEASURE;
    if (queryResult.value.propertyResult) return CatalogToGroupType.CATALOG_PROPERTY;
    if (queryResult.value.traceResult) return CatalogToGroupType.CATALOG_TRACE;
    if (queryResult.value.topnResult) return CatalogToGroupType.CATALOG_TOPN;
    return 'unknown';
  });
  // Transform query results into table format
  const tableData = computed(() => {
    if (!queryResult.value) return [];

    try {
      // Handle TopN results differently
      if (queryResult.value.topnResult?.lists) {
        const topnLists = queryResult.value.topnResult.lists;
        const rows = topnLists
          .map((list) =>
            (list.items || []).map((item) => ({
              label: item.entity?.[0]?.value?.str?.value || 'N/A',
              value: item.value?.int?.value ?? item.value?.float?.value ?? 'N/A',
              timestamp: list.timestamp || item.timestamp,
            })),
          )
          .flat();
        return rows;
      }

      let elements = [];
      if (queryResult.value.streamResult?.elements) {
        elements = queryResult.value.streamResult.elements;
      }
      if (queryResult.value.measureResult?.dataPoints) {
        elements = queryResult.value.measureResult.dataPoints;
      }
      if (queryResult.value.traceResult?.traces) {
        elements = queryResult.value.traceResult.traces;
      }
      if (queryResult.value.propertyResult?.properties) {
        elements = queryResult.value.propertyResult.properties;
      }

      if (!elements || elements.length === 0) return [];

      // Transform elements to table rows
      const rows = elements.map((item) => {
        const row = {};

        // Process tag families
        if (item.tag_families || item.tagFamilies) {
          const tagFamilies = item.tag_families || item.tagFamilies;
          tagFamilies.forEach((family) => {
            const tags = family.tags || [];
            tags.forEach((tag) => {
              const key = tag.key;
              const value = tag.value;
              if (value) {
                const typeKeys = Object.keys(value);
                for (const typeKey of typeKeys) {
                  if (value[typeKey] !== undefined && value[typeKey] !== null) {
                    if (typeof value[typeKey] === 'object' && value[typeKey].value !== undefined) {
                      row[key] = value[typeKey].value;
                    } else {
                      row[key] = value[typeKey];
                    }
                    break;
                  }
                }
              }

              if (row[key] === undefined || row[key] === null) {
                row[key] = 'Null';
              }
            });
          });
        }
        // Process fields for measure results
        if (item.fields) {
          item.fields.forEach((field) => {
            const name = field.name;
            const value = field.value;

            if (value) {
              const typeKeys = Object.keys(value);
              for (const typeKey of typeKeys) {
                if (value[typeKey] !== undefined && value[typeKey] !== null) {
                  if (typeof value[typeKey] === 'object' && value[typeKey].value !== undefined) {
                    row[name] = value[typeKey].value;
                  } else {
                    row[name] = value[typeKey];
                  }
                  break;
                }
              }
            }
            if (row[name] === undefined || row[name] === null) {
              row[name] = 'Null';
            }
          });
        }
        row.timestamp = item.timestamp;
        return row;
      });

      return rows;
    } catch (e) {
      console.error('Error parsing table data:', e);
      return [];
    }
  });

  // Generate table columns from the first row of data
  const tableColumns = computed(() => {
    if (!tableData.value || tableData.value.length === 0) return [];

    const firstRow = tableData.value[0];
    const columns = [];

    Object.keys(firstRow).forEach((key) => {
      if (key !== 'timestamp') {
        columns.push({
          name: key,
          label: key,
          prop: key,
          type: Array.isArray(firstRow[key]) ? 'TAG_TYPE_STRING_ARRAY' : 'TAG_TYPE_STRING',
        });
      }
    });

    return columns;
  });

  // Determine if we should use pagination based on result type
  const shouldTopNResult = computed(() => resultType.value === CatalogToGroupType.CATALOG_TOPN);
  const shouldPropertyResult = computed(() => resultType.value === CatalogToGroupType.CATALOG_PROPERTY);
  const shouldTraceResult = computed(() => resultType.value === CatalogToGroupType.CATALOG_TRACE);

  // Transform property results into table format
  const propertyData = computed(() => {
    if (!queryResult.value || !queryResult.value.propertyResult) return [];

    const properties = queryResult.value.propertyResult.properties || [];
    return properties.map((item) => {
      // Clone and process tags to stringify values
      const processedItem = { ...item };
      if (processedItem.tags) {
        processedItem.tags = processedItem.tags.map((tag) => ({
          ...tag,
          value: JSON.stringify(tag.value),
        }));
      }
      return processedItem;
    });
  });

  // Transform trace results into table format for TraceTable
  const traceTableData = computed(() => {
    if (!queryResult.value || !queryResult.value.traceResult?.traces) return [];

    const traces = queryResult.value.traceResult.traces;
    return traces
      .map((trace) => {
        return (trace.spans || []).map((span) => {
          const tagsMap = {};
          for (const tag of span.tags || []) {
            tagsMap[tag.key] = tag.value;
          }
          return {
            traceId: trace.traceId,
            ...span,
            ...tagsMap,
          };
        });
      })
      .flat();
  });

  // Extract span tags for TraceTable columns
  const spanTags = computed(() => {
    if (!queryResult.value || !queryResult.value.traceResult?.traces) return [];

    const tags = new Set();
    const traces = queryResult.value.traceResult.traces;
    traces.forEach((trace) => {
      (trace.spans || []).forEach((span) => {
        (span.tags || []).forEach((tag) => {
          tags.add(tag.key);
        });
      });
    });
    return Array.from(tags);
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
    const response = await executeBydbQLQuery({ query: queryText.value });
    const endTime = performance.now();
    loading.value = false;
    executionTime.value = Math.round(endTime - startTime);

    if (response.error) {
      error.value = response.error.message || 'Failed to execute query';
      ElMessage.error(error.value);
      return;
    }
    queryResult.value = response;
    ElMessage.success(`Query executed successfully in ${executionTime.value}ms`);
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
  <div class="query-container">
    <el-card shadow="always" class="query-card">
      <template #header>
        <div class="card-header">
          <span class="header-title">BydbQL Query Console</span>
          <div class="header-actions">
            <el-button @click="clearQuery" size="small" :disabled="loading"> Clear </el-button>
            <el-button type="primary" @click="executeQuery" :loading="loading" size="small"> Execute Query </el-button>
          </div>
        </div>
      </template>
      <div class="query-input-container">
        <CodeMirror
          v-model="queryText"
          :mode="'sql'"
          :lint="false"
          :readonly="false"
          :style-active-line="true"
          :auto-refresh="true"
          class="query-input"
        />
      </div>
    </el-card>
    <el-card shadow="always" class="result-card">
      <template #header>
        <div>
          <el-tag v-if="resultType" size="small" class="result-type-tag">
            {{ resultType.toUpperCase() }}
          </el-tag>
        </div>
      </template>
      <div v-if="error" class="error-message">
        <el-alert title="Error" type="error" :description="error" show-icon :closable="false" />
      </div>
      <div v-if="!hasResult && !error">
        <el-empty description="No result" />
      </div>
      <div class="result-table" v-if="hasResult">
        <!-- Use PropertyTable for Property results -->
        <PropertyTable v-if="shouldPropertyResult" :data="propertyData" :border="true" @refresh="executeQuery" />
        <!-- Use TopNTable for TopN results -->
        <TopNTable
          v-else-if="shouldTopNResult"
          :data="tableData"
          :columns="tableColumns"
          :loading="false"
          :page-size="20"
          :show-selection="false"
          :show-index="false"
          :show-timestamp="false"
          :show-pagination="true"
          :stripe="true"
          :border="true"
          empty-text="No data yet"
          min-height="300px"
        />
        <!-- Use TraceTable for Trace results -->
        <TraceTable
          v-else-if="shouldTraceResult"
          :data="traceTableData"
          :span-tags="spanTags"
          :border="true"
          :show-selection="false"
          :enable-merge="true"
          empty-text="No trace data found"
        />
        <!-- Use MeasureAndStreamTable for other result types -->
        <MeasureAndStreamTable
          v-else
          :table-data="tableData"
          :loading="false"
          :table-header="tableColumns"
          :show-selection="false"
          :show-index="true"
          :show-timestamp="true"
          empty-text="No data yet"
        />
      </div>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
  .query-container {
    display: flex;
    flex-direction: column;
    gap: 20px;
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

  .header-right {
    display: flex;
    align-items: center;
    gap: 16px;
  }

  .result-type-tag {
    font-weight: 600;
  }

  .execution-time {
    font-size: 14px;
    color: #606266;
  }

  .result-table {
    overflow: auto;
    max-height: 46vh;
    min-height: 100px;
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
      height: 150px;
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
