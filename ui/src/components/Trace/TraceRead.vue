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
    indexRule: '',
  });
  const yamlCode = ref(``);
  const selectedSpans = ref({});

  const getTraces = async (params) => {
    $loadingCreate();
    const response = await queryTraces({ groups: [data.group], name: data.name, ...params });
    $loadingClose();
    if (response.error) {
      data.tableData = [];
      ElMessage({
        message: response.error.message,
        type: 'error',
        duration: 3000,
      });
      return;
    }
    data.tableData = response.traces || [];
  };

  const getIndexRule = async () => {
    if (!data.group) {
      return;
    }
    try {
      const response = await getindexRuleList(data.group);
      if (response.status === 200 && response.data.indexRule && response.data.indexRule.length > 0) {
        data.indexRule = response.data.indexRule[0].metadata;
      } else {
        data.indexRule = '';
      }
    } catch (err) {
      console.error('Failed to fetch indexRule:', err);
      data.indexRule = '';
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

  async function initTraceData() {
    if (!(data.group && data.name)) {
      return;
    }
    await getIndexRule();
    if (!data.indexRule) {
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
offset: 0
limit: 10
tagProjection: ["trace_id", "service_id"]
orderBy:
  indexRuleName: ${data.indexRule.name}
  sort: SORT_DESC`;

    getTraces(yamlToJson(yamlCode.value).data);
  }

  function handleSelectionChange(traceIndex, selection) {
    selectedSpans.value[traceIndex] = selection;
  }

  async function downloadMultipleSpans(traceIndex) {
    const selection = selectedSpans.value[traceIndex];
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
      const trace = data.tableData[traceIndex];
      const timestamp = Date.now();
      let successCount = 0;

      // Add each span to the ZIP file
      for (const span of selection) {
        if (span && span.span) {
          const base64Data = span.span;
          const binaryString = atob(base64Data);
          const bytes = new Uint8Array(binaryString.length);
          for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }
          // Find the original index of the span in the trace
          const spanIndex = trace.spans.indexOf(span);
          // Add the binary data to the ZIP file
          zip.file(`span-${spanIndex + 1}.bin`, bytes);
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
      link.download = `trace-${traceIndex + 1}-spans-${timestamp}.zip`;
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
    if (!data.value) {
      return '';
    }
    return JSON.stringify(data.value);
  };

  watch(
    () => route,
    () => {
      const { group, name } = route.params;
      data.name = name;
      data.group = group;
      data.indexRule = '';
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
              <span
                ><strong>Trace #{{ traceIndex + 1 }}</strong></span
              >
              <div style="display: flex; align-items: center; gap: 10px">
                <el-button
                  :icon="Download"
                  size="small"
                  @click="downloadMultipleSpans(traceIndex)"
                  plain
                  type="primary"
                >
                  Download Selected
                </el-button>
                <el-tag type="info">{{ trace.spans ? trace.spans.length : 0 }} Span(s)</el-tag>
              </div>
            </div>
          </template>
          <!-- Spans List -->
          <div v-if="trace.spans && trace.spans.length > 0">
            <el-table
              :data="trace.spans"
              stripe
              border
              style="width: 100%"
              @selection-change="(selection) => handleSelectionChange(traceIndex, selection)"
            >
              <el-table-column type="selection" width="55" />
              <el-table-column type="index" label="#" width="60" />
              <el-table-column label="Tags">
                <template #default="scope">
                  <el-table :data="scope.row.tags">
                    <el-table-column label="Key" prop="key" width="150"></el-table-column>
                    <el-table-column label="Value" prop="value">
                      <template #default="scope">
                        {{ getTagValue(scope.row) }}
                      </template>
                    </el-table-column>
                  </el-table>
                </template>
              </el-table-column>
            </el-table>
          </div>
          <el-empty v-else description="No spans in this trace" :image-size="80" />
        </el-card>
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
