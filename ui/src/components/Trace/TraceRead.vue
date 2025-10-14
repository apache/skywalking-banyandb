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
  import { queryTraces } from '@/api/index';
  import { getCurrentInstance } from '@vue/runtime-core';
  import { useRoute } from 'vue-router';
  import { ElMessage } from 'element-plus';
  import { reactive, ref, watch, onMounted } from 'vue';
  import { RefreshRight, Search, View } from '@element-plus/icons-vue';
  import { jsonToYaml, yamlToJson } from '@/utils/yaml';
  import CodeMirror from '@/components/CodeMirror/index.vue';
  import FormHeader from '../common/FormHeader.vue';
  import { Last15Minutes, Shortcuts } from '../common/data';

  const { proxy } = getCurrentInstance();
  const route = useRoute();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const yamlRef = ref(null);
  const timeRange = ref([]);
  const data = reactive({
    group: route.params.group,
    tableData: [],
    name: route.params.name,
  });
  const yamlCode = ref(``);

  const getTraces = (params) => {
    $loadingCreate();
    queryTraces({ groups: [data.group], name: data.name, ...params })
      .then((res) => {
        if (res.status === 200) {
          data.tableData = res.data.traces || [];
        }
      })
      .catch((err) => {
        ElMessage({
          message: 'An error occurred while querying traces. Please refresh and try again. Error: ' + err,
          type: 'error',
          duration: 3000,
        });
      })
      .finally(() => {
        $loadingClose();
      });
  };

  const spanDialogVisible = ref(false);
  const currentSpanData = ref(null);

  const viewSpanDetails = (span) => {
    try {
      // Decode base64 span data
      const decodedBytes = atob(span.span);
      // Try to parse as JSON first
      try {
        currentSpanData.value = JSON.parse(decodedBytes);
      } catch {
        // If not JSON, display as text
        currentSpanData.value = decodedBytes;
      }
      spanDialogVisible.value = true;
    } catch (err) {
      ElMessage({
        message: 'Failed to decode span data: ' + err,
        type: 'error',
        duration: 3000,
      });
    }
  };

  const closeSpanDialog = () => {
    spanDialogVisible.value = false;
    currentSpanData.value = null;
  };

  const formatTagValue = (tagValue) => {
    if (!tagValue) return 'N/A';
    // Handle different tag value types
    if (tagValue.str) return tagValue.str.value;
    if (tagValue.int) return tagValue.int.value;
    if (tagValue.strArray) return JSON.stringify(tagValue.strArray.value);
    if (tagValue.intArray) return JSON.stringify(tagValue.intArray.value);
    if (tagValue.binaryData) return `<binary data: ${tagValue.binaryData.length} bytes>`;
    if (tagValue.id) return JSON.stringify(tagValue.id);
    return JSON.stringify(tagValue);
  };

  function searchTraces() {
    yamlRef.value
      .checkYaml(yamlCode.value)
      .then(() => {
        const json = yamlToJson(yamlCode.value).data;
        getTraces(json);
      })
      .catch((err) => {
        ElMessage({
          dangerouslyUseHTMLString: true,
          showClose: true,
          message: `<div>${err.message}</div>`,
          type: 'error',
          duration: 5000,
        });
      });
  }

  function changeTimeRange() {
    const json = yamlToJson(yamlCode.value);
    if (!json.data.timeRange) {
      json.data.timeRange = {
        begin: '',
        end: '',
      };
    }
    json.data.timeRange.begin = timeRange.value[0] ?? null;
    json.data.timeRange.end = timeRange.value[1] ?? null;
    yamlCode.value = jsonToYaml(json.data).data;
  }

  function initTraceData() {
    if (!(data.group && data.name)) {
      return;
    }
    timeRange.value = [new Date(new Date().getTime() - Last15Minutes), new Date()];
    const range = jsonToYaml({
      timeRange: {
        begin: timeRange.value[0],
        end: timeRange.value[1],
      },
    }).data;
    yamlCode.value = `${range}groups:
  - ${data.group}
name: ${data.name}
offset: 1
limit: 10
orderBy:
  indexRuleName: ""
  sort: "SORT_DESC"`;

    getTraces(yamlToJson(yamlCode.value).data);
  }

  watch(
    () => route,
    () => {
      const { group, name } = route.params;
      data.name = name;
      data.group = group;
      initTraceData();
    },
    {
      immediate: true,
      deep: true,
    },
  );
</script>
<template>
  <div>
    <el-card shadow="always">
      <template #header>
        <FormHeader :fields="data" />
      </template>
      <el-row>
        <el-col :span="10">
          <div class="flex align-item-center" style="height: 40px; width: 100%">
            <el-date-picker
              @change="changeTimeRange"
              v-model="timeRange"
              type="datetimerange"
              :shortcuts="Shortcuts"
              range-separator="to"
              start-placeholder="begin"
              end-placeholder="end"
            />
            <el-button :icon="Search" @click="searchTraces" style="margin-left: 10px" color="#6E38F7" plain />
          </div>
        </el-col>
        <el-col :span="14">
          <div class="flex align-item-center justify-end" style="height: 30px">
            <el-button :icon="RefreshRight" @click="initTraceData" plain />
          </div>
        </el-col>
      </el-row>
      
      <CodeMirror ref="yamlRef" v-model="yamlCode" mode="yaml" style="height: 250px" :lint="true" />
      
      <!-- Traces Table -->
      <div v-if="data.tableData.length > 0" style="margin-top: 20px">
        <el-card v-for="(trace, traceIndex) in data.tableData" :key="traceIndex" style="margin-bottom: 15px">
          <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center">
              <span><strong>Trace #{{ traceIndex + 1 }}</strong></span>
              <el-tag type="info">{{ trace.spans ? trace.spans.length : 0 }} Span(s)</el-tag>
            </div>
          </template>
          
          <!-- Spans Table for this trace -->
          <el-table :data="trace.spans" border style="width: 100%">
            <el-table-column type="expand">
              <template #default="props">
                <div style="padding: 15px">
                  <h4 style="margin-top: 0">Indexed Tags:</h4>
                  <el-table :data="props.row.tags" size="small" border v-if="props.row.tags && props.row.tags.length > 0">
                    <el-table-column label="Tag Name" width="250">
                      <template #default="tagScope">
                        <el-tag size="small">{{ tagScope.row.key }}</el-tag>
                      </template>
                    </el-table-column>
                    <el-table-column label="Value">
                      <template #default="tagScope">
                        <code>{{ formatTagValue(tagScope.row.value) }}</code>
                      </template>
                    </el-table-column>
                  </el-table>
                  <el-empty v-else description="No indexed tags" :image-size="60" />
                  
                  <h4 style="margin-top: 20px; margin-bottom: 10px">Raw Span Data:</h4>
                  <el-button
                    size="small"
                    type="primary"
                    @click="viewSpanDetails(props.row)"
                    :icon="View"
                  >
                    View Raw Span Data (bytes)
                  </el-button>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="Span #" width="100">
              <template #default="scope">
                <strong>#{{ scope.$index + 1 }}</strong>
              </template>
            </el-table-column>
            <el-table-column label="Indexed Tags Count" width="180">
              <template #default="scope">
                <el-tag size="small" type="success">{{ scope.row.tags ? scope.row.tags.length : 0 }} tag(s)</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="Tag Summary" min-width="300">
              <template #default="scope">
                <div v-if="scope.row.tags && scope.row.tags.length > 0">
                  <el-tag
                    v-for="(tag, idx) in scope.row.tags.slice(0, 3)"
                    :key="idx"
                    size="small"
                    style="margin-right: 5px; margin-bottom: 5px"
                  >
                    {{ tag.key }}
                  </el-tag>
                  <span v-if="scope.row.tags.length > 3">+{{ scope.row.tags.length - 3 }} more</span>
                </div>
                <el-empty v-else description="No tags" :image-size="30" />
              </template>
            </el-table-column>
            <el-table-column label="Actions" width="120">
              <template #default="scope">
                <el-button
                  link
                  type="primary"
                  size="small"
                  @click="viewSpanDetails(scope.row)"
                >
                  View Data
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </div>
      <el-empty v-else description="No trace data found" style="margin-top: 20px" />
    </el-card>

    <!-- Span Data Dialog -->
    <el-dialog
      v-model="spanDialogVisible"
      title="Raw Span Data"
      width="70%"
      @close="closeSpanDialog"
    >
      <div v-if="currentSpanData !== null">
        <pre style="background: #f5f5f5; padding: 15px; border-radius: 4px; overflow: auto; max-height: 500px">{{ typeof currentSpanData === 'object' ? JSON.stringify(currentSpanData, null, 2) : currentSpanData }}</pre>
      </div>
      <template #footer>
        <el-button @click="closeSpanDialog">Close</el-button>
      </template>
    </el-dialog>
  </div>
</template>
<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }

  .button-group-operator {
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    margin-bottom: 10px;
  }
</style>

