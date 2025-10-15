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
  import { getCurrentInstance } from '@vue/runtime-core';
  import { useRoute } from 'vue-router';
  import { ElMessage } from 'element-plus';
  import { reactive, ref, watch } from 'vue';
  import { RefreshRight, Search } from '@element-plus/icons-vue';
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
    indexRule: '',
  });
  const yamlCode = ref(``);

  const getTraces = async (params) => {
    $loadingCreate();
    const response = await queryTraces({ groups: [data.group], name: data.name, ...params })
    if (response.status === 200) {
      data.tableData = response.data.traces || [];
    }
    $loadingClose();
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
orderBy:
  indexRuleName: ${data.indexRule.name}
  sort: SORT_DESC`;

    getTraces(yamlToJson(yamlCode.value).data);
  }

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
              <span><strong>Trace #{{ traceIndex + 1 }}</strong></span>
              <el-tag type="info">{{ trace.spans ? trace.spans.length : 0 }} Span(s)</el-tag>
            </div>
          </template>
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
</style>

