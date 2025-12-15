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
  import { useRoute } from 'vue-router';
  import { ElMessage } from 'element-plus';
  import { reactive, ref, watch, onMounted, getCurrentInstance } from 'vue';
  import { RefreshRight, Search, TrendCharts } from '@element-plus/icons-vue';
  import { fetchProperties } from '@/api/index';
  import { yamlToJson } from '@/utils/yaml';
  import CodeMirror from '@/components/CodeMirror/index.vue';
  import FormHeader from '../common/FormHeader.vue';
  import TraceTree from '../TraceTree/TraceContent.vue';
  import PropertyTable from '@/components/common/PropertyTable.vue';

  const { proxy } = getCurrentInstance();
  // Loading
  const route = useRoute();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const yamlRef = ref(null);
  const data = reactive({
    group: route.params.group,
    tableData: [],
    name: route.params.name,
  });
  const yamlCode = ref(`name: ${data.name}
limit: 10`);
  const showTracesDialog = ref(false);
  const traceData = ref(null);
  const getProperties = async (params) => {
    $loadingCreate();
    const res = await fetchProperties({ groups: [data.group], name: data.name, limit: 10, ...params });
    $loadingClose();
    if (res.error) {
      ElMessage({
        message: `Failed to fetch properties: ${res.error.message}`,
        type: 'error',
      });
      return;
    }
    traceData.value = res.trace;
    data.tableData = (res.properties || []).map((item) => {
      item.tags.forEach((tag) => {
        tag.value = JSON.stringify(tag.value);
      });
      return item;
    });
  };

  function searchProperties() {
    yamlRef.value
      .checkYaml(yamlCode.value)
      .then(() => {
        const json = yamlToJson(yamlCode.value).data;
        getProperties(json);
      })
      .catch((err) => {
        ElMessage({
          dangerouslyUseHTMLString: true,
          showClose: true,
          message: err.message,
          type: 'error',
          duration: 5000,
        });
      });
  }

  onMounted(() => {
    getProperties();
  });
  watch(
    () => route.params,
    () => {
      const { group, name } = route.params;
      data.name = name;
      data.group = group;
      yamlCode.value = `name: ${data.name}
limit: 10`;
      getProperties();
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
        <el-button size="small" :icon="Search" @click="searchProperties" plain />
        <el-button size="small" :icon="RefreshRight" @click="getProperties" plain />
      </div>
      <CodeMirror ref="yamlRef" v-model="yamlCode" mode="yaml" style="height: 200px" :lint="true" />
      <div style="margin-top: 20px; margin-bottom: 10px; display: flex; justify-content: flex-end">
        <el-button :icon="TrendCharts" @click="showTracesDialog = true" :disabled="!traceData" plain>
          <span>Debug Trace</span>
        </el-button>
      </div>
      <PropertyTable :data="data.tableData" :border="true" :show-operator="true" @refresh="getProperties" />
    </el-card>
  </div>
  <el-dialog
    v-model="showTracesDialog"
    width="90%"
    :destroy-on-close="true"
    @closed="showTracesDialog = false"
    class="trace-dialog"
  >
    <div style="max-height: 74vh; overflow-y: auto">
      <TraceTree :trace="traceData" />
    </div>
  </el-dialog>
</template>
<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }

  .button-group-operator {
    display: flex;
    flex-direction: row;
    justify-content: end;
    margin-bottom: 10px;
  }
</style>
