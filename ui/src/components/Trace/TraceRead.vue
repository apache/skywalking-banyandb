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
  import { RefreshRight, Search } from '@element-plus/icons-vue';
  import { yamlToJson } from '@/utils/yaml';
  import CodeMirror from '@/components/CodeMirror/index.vue';
  import FormHeader from '../common/FormHeader.vue';

  const { proxy } = getCurrentInstance();
  const route = useRoute();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const yamlRef = ref(null);
  const data = reactive({
    group: route.params.group,
    tableData: [],
    name: route.params.name,
  });
  const yamlCode = ref(`groups:
  - ${data.group}
name: ${data.name}
timeRange:
  begin: "2024-01-01T00:00:00Z"
  end: "2024-12-31T23:59:59Z"
offset: 0
limit: 10
orderBy:
  indexRuleName: ""
  sort: "SORT_DESC"
trace: false`);

  const getTraces = (params) => {
    $loadingCreate();
    const defaultParams = {
      groups: [data.group],
      name: data.name,
      timeRange: {
        begin: '2024-01-01T00:00:00Z',
        end: '2024-12-31T23:59:59Z',
      },
      offset: 0,
      limit: 10,
      orderBy: {
        indexRuleName: "",
        sort: 'SORT_DESC',
      },
      trace: false,
    };
    queryTraces({ ...defaultParams, ...params })
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

  const viewSpanDetails = (span) => {
    try {
      const spanData = JSON.parse(atob(span.span));
      ElMessage({
        dangerouslyUseHTMLString: true,
        message: `<pre>${JSON.stringify(spanData, null, 2)}</pre>`,
        type: 'info',
        duration: 10000,
        showClose: true,
      });
    } catch (err) {
      ElMessage({
        message: 'Failed to parse span data: ' + err,
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

  onMounted(() => {
    getTraces();
  });

  watch(
    () => route.params,
    () => {
      const { group, name } = route.params;
      data.name = name;
      data.group = group;
      yamlCode.value = `groups:
  - ${data.group}
name: ${data.name}
timeRange:
  begin: "2024-01-01T00:00:00Z"
  end: "2024-12-31T23:59:59Z"
offset: 0
limit: 10
trace: false`;
      getTraces();
    },
  );
</script>
<template>
  <div>
    <el-card shadow="always">
      <template #header>
        <FormHeader :fields="data" />
      </template>
      <div class="button-group-operator">
        <div>
          <el-button size="small" :icon="Search" @click="searchTraces" plain />
          <el-button size="small" :icon="RefreshRight" @click="getTraces" plain />
        </div>
      </div>
      <CodeMirror ref="yamlRef" v-model="yamlCode" mode="yaml" style="height: 250px" :lint="true" />
      <el-table :data="data.tableData" style="width: 100%; margin-top: 20px" border>
        <el-table-column label="Trace ID" width="180">
          <template #default="scope">
            <span>Trace {{ scope.$index + 1 }}</span>
          </template>
        </el-table-column>
        <el-table-column label="Spans Count" width="150">
          <template #default="scope">
            <span>{{ scope.row.spans ? scope.row.spans.length : 0 }}</span>
          </template>
        </el-table-column>
        <el-table-column label="Spans">
          <template #default="scope">
            <el-collapse accordion>
              <el-collapse-item
                v-for="(span, spanIndex) in scope.row.spans"
                :key="spanIndex"
                :title="`Span ${spanIndex + 1}`"
              >
                <div class="span-details">
                  <div v-if="span.tags && span.tags.length > 0">
                    <strong>Tags:</strong>
                    <el-table :data="span.tags" size="small" style="margin-top: 10px">
                      <el-table-column label="Key" prop="key" width="200"></el-table-column>
                      <el-table-column label="Value">
                        <template #default="tagScope">
                          <span>{{ JSON.stringify(tagScope.row.value) }}</span>
                        </template>
                      </el-table-column>
                    </el-table>
                  </div>
                  <div style="margin-top: 10px">
                    <strong>Span Data:</strong>
                    <el-button
                      size="small"
                      type="primary"
                      @click="viewSpanDetails(span)"
                      style="margin-left: 10px"
                      plain
                    >
                      View Details
                    </el-button>
                  </div>
                </div>
              </el-collapse-item>
            </el-collapse>
          </template>
        </el-table-column>
      </el-table>
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

  .span-details {
    padding: 10px;
  }
</style>

