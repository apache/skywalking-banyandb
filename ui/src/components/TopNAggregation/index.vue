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
  import { reactive, ref } from 'vue';
  import { watch } from '@vue/runtime-core';
  import { useRoute } from 'vue-router';
  import { jsonToYaml, yamlToJson } from '@/utils/yaml';
  import { Search, RefreshRight } from '@element-plus/icons-vue';
  import { getTopNAggregationData } from '@/api/index';
  import FormHeader from '../common/FormHeader.vue';
  import { Shortcuts, Last15Minutes } from '../common/data';

  const pageSize = 10;
  const route = useRoute();
  const data = reactive({
    group: '',
    name: '',
    type: '',
    operator: '',
    lists: [],
  });
  const yamlRef = ref(null);
  const timeRange = ref([]);
  const yamlCode = ref('');
  const loading = ref(false);
  const currentList = ref([]);

  function initTopNAggregationData() {
    if (!(data.type && data.group && data.name)) {
      return;
    }
    timeRange.value = [new Date(new Date().getTime() - Last15Minutes), new Date()];
    const range = jsonToYaml({
      timeRange: {
        begin: timeRange.value[0],
        end: timeRange.value[1],
      },
    }).data;
    yamlCode.value = `${range}topN: 10
agg: 2
fieldValueSort: 1`;
    fetchTopNAggregationData();
  }

  async function fetchTopNAggregationData(param) {
    loading.value = true;
    const result = await getTopNAggregationData({
      groups: [data.group],
      name: data.name,
      timeRange: { begin: timeRange.value[0], end: timeRange.value[1] },
      topN: 10,
      agg: 2,
      fieldValueSort: 1,
      ...param,
    });
    loading.value = false;
    if (!result.data) {
      ElMessage({
        message: `Please refresh and try again. Error: ${err}`,
        type: 'error',
        duration: 3000,
      });
      return;
    }
    data.lists = result.data.lists
      .map((d) => d.items.map((item) => ({ label: item.entity[0].value.str.value, value: item.value.int.value })))
      .flat();
    changePage(0);
  }

  function searchTopNAggregation() {
    yamlRef.value
      .checkYaml(yamlCode.value)
      .then(() => {
        const json = yamlToJson(yamlCode.value).data;
        fetchTopNAggregationData(json);
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

  function changePage(pageIndex) {
    currentList.value = data.lists.filter(
      (d, index) => (pageIndex - 1 || 0) * pageSize <= index && pageSize * (pageIndex || 1) > index,
    );
  }

  watch(
    () => route,
    () => {
      data.group = route.params.group;
      data.name = route.params.name;
      data.type = route.params.type;
      data.operator = route.params.operator;
      initTopNAggregationData();
    },
    {
      immediate: true,
      deep: true,
    },
  );
</script>

<template>
  <div v-loading="loading">
    <el-card>
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
            <el-button :icon="Search" @click="searchTopNAggregation" style="margin-left: 10px" color="#6E38F7" plain />
          </div>
        </el-col>
        <el-col :span="14">
          <div class="flex align-item-center justify-end" style="height: 30px">
            <el-button :icon="RefreshRight" @click="initTopNAggregationData" plain />
          </div>
        </el-col>
      </el-row>
      <CodeMirror ref="yamlRef" v-model="yamlCode" mode="yaml" style="height: 200px" :lint="true" />
    </el-card>
    <el-card>
      <el-table
        :data="currentList"
        style="width: 100%; margin: 10px 0; min-height: 440px"
        stripe
        :border="true"
        highlight-current-row
        tooltip-effect="dark"
      >
        <el-table-column prop="label" label="Label" />
        <el-table-column prop="value" label="Value" width="220" />
      </el-table>
      <el-pagination
        background
        layout="prev, pager, next"
        :page-size="pageSize"
        :total="data.lists.length"
        @current-change="changePage"
        @prev-click="changePage"
        @next-click="changePage"
      />
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }
</style>
