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
  import { queryTraces, getindexRuleList } from '@/api/index';
  import { getCurrentInstance } from 'vue';
  import { useRoute } from 'vue-router';
  import { ElMessage } from 'element-plus';
  import { reactive, ref, watch } from 'vue';
  import { RefreshRight, Search, Download } from '@element-plus/icons-vue';
  import { jsonToYaml, yamlToJson } from '@/utils/yaml';
  import CodeMirror from '@/components/CodeMirror/index.vue';
  import FormHeader from '../common/FormHeader.vue';
  import { Last15Minutes, Shortcuts } from '../common/data';
  import JSZip from 'jszip';

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
    indexRule: null,
    spanTags: ['traceId', 'spanId'],
  });
  const yamlCode = ref(``);
  const selectedSpans = ref([]);

  const getTraces = async (params) => {
    if (!data.indexRule?.metadata?.name) {
      ElMessage({
        message: 'No index rule found',
        type: 'error',
      });
      return;
    }
    $loadingCreate();
    const response = await queryTraces({ groups: [data.group], name: data.name, ...params });
    $loadingClose();
    if (response.error) {
      data.tableData = [];
      ElMessage({
        message: response.error.message,
        type: 'error',
      });
      return;
    }
    data.spanTags = ['traceId', 'spanId'];
    data.tableData = (response.traces || [])
      .map((trace) => {
        return trace.spans.map((span) => {
          const tagsMap = {};
          for (const tag of span.tags) {
            if (!data.spanTags.includes(tag.key)) {
              data.spanTags.push(tag.key);
            }
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
  };

  const getIndexRule = async () => {
    if (!data.group) {
      return;
    }
    try {
      const response = await getindexRuleList(data.group);
      if (response.status === 200 && response.data.indexRule && response.data.indexRule.length > 0) {
        data.indexRule = response.data.indexRule[0];
      } else {
        data.indexRule = null;
      }
    } catch (err) {
      console.error('Failed to fetch indexRule:', err);
      data.indexRule = null;
      ElMessage({
        message: 'Failed to fetch index rule: ' + err,
        type: 'error',
        duration: 3000,
      });
    }
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
          showClose: true,
          message: err.message,
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

  async function initTraceData() {
    if (!(data.group && data.name)) {
      return;
    }
    await getIndexRule();
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
offset: 0
limit: 10
tagProjection: ${Array.isArray(data.indexRule?.tags) && data.indexRule.tags.length ? JSON.stringify(data.indexRule.tags) : '[]'}
orderBy:
  indexRuleName: ${data.indexRule?.metadata?.name || ''}
  sort: SORT_DESC`;
    getTraces(yamlToJson(yamlCode.value).data);
  }

  function handleSelectionChange(selection) {
    selectedSpans.value = selection;
  }

  async function downloadMultipleSpans() {
    const selection = selectedSpans.value;
    if (!selection || selection.length === 0) {
      ElMessage({
        message: 'Please select at least one span to download',
        type: 'warning',
        duration: 3000,
      });
      return;
    }

    try {
      const zip = new JSZip();
      const timestamp = Date.now();
      let successCount = 0;

      // Add each span to the ZIP file
      for (let i = 0; i < selection.length; i++) {
        const span = selection[i];
        if (span && span.span) {
          const base64Data = span.span;
          const binaryString = atob(base64Data);
          const bytes = new Uint8Array(binaryString.length);
          for (let j = 0; j < binaryString.length; j++) {
            bytes[j] = binaryString.charCodeAt(j);
          }
          // Add the binary data to the ZIP file
          zip.file(`span-${i + 1}.bin`, bytes);
          successCount++;
        }
      }

      if (successCount === 0) {
        ElMessage({
          message: 'No valid spans to download',
          type: 'warning',
          duration: 3000,
        });
        return;
      }

      // Generate the ZIP file
      const zipBlob = await zip.generateAsync({ type: 'blob' });

      // Download the ZIP file
      const url = URL.createObjectURL(zipBlob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `spans-${timestamp}.zip`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      ElMessage({
        message: `Successfully downloaded ${successCount} span(s) as compressed file`,
        type: 'success',
        duration: 2000,
      });
    } catch (err) {
      ElMessage({
        message: 'Failed to download spans: ' + err.message,
        type: 'error',
        duration: 3000,
      });
    }
  }

  const getTagValue = (data) => {
    let value = data.value;

    const isNullish = (val) => val === null || val === undefined || val === 'null';
    if (isNullish(value)) {
      return 'N/A';
    }
    for (let i = 0; i < 2; i++) {
      if (typeof value !== 'object') {
        return value;
      }
      for (const key in value) {
        if (Object.hasOwn(value, key)) {
          value = value[key];
          break;
        }
      }
      if (isNullish(value)) {
        return 'N/A';
      }
    }

    return value;
  };

  const objectSpanMethod = ({ row, column, rowIndex, columnIndex }) => {
    // Only merge the traceId column (first column after selection)
    if (columnIndex === 1) {
      const currentTraceId = row.traceId;
      // Check if this is the first row with this traceId
      if (rowIndex === 0 || data.tableData[rowIndex - 1].traceId !== currentTraceId) {
        // Count how many rows have the same traceId
        let rowspan = 1;
        for (let i = rowIndex + 1; i < data.tableData.length; i++) {
          if (data.tableData[i].traceId === currentTraceId) {
            rowspan++;
          } else {
            break;
          }
        }
        return {
          rowspan: rowspan,
          colspan: 1,
        };
      } else {
        // This row's traceId is merged with a previous row
        return {
          rowspan: 0,
          colspan: 0,
        };
      }
    }
  };

  watch(
    () => route,
    () => {
      const { group, name } = route.params;
      data.name = name;
      data.group = group;
      data.indexRule = null;
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

      <!-- Trace Data Table -->
      <div v-if="data.tableData.length > 0" style="margin-top: 20px">
        <div style="margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center">
          <span
            ><strong>Total {{ data.tableData.length }} span(s)</strong></span
          >
          <el-button :icon="Download" @click="downloadMultipleSpans"> Download Selected </el-button>
        </div>
        <el-table
          :data="data.tableData"
          :border="true"
          style="width: 100%; background-color: #f5f7fa"
          @selection-change="handleSelectionChange"
          :span-method="objectSpanMethod"
        >
          <el-table-column type="selection" width="55" />
          <el-table-column v-for="tag in data.spanTags" :key="tag" :label="tag" :prop="tag">
            <template #default="scope">
              {{ getTagValue({ value: scope.row[tag] }) }}
            </template>
          </el-table-column>
        </el-table>
      </div>
      <el-empty v-else description="No trace data found" style="margin-top: 20px" />
    </el-card>
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

  .span-data {
    font-family: 'Courier New', Courier, monospace;
    word-break: break-all;
    font-size: 12px;
  }
</style>
