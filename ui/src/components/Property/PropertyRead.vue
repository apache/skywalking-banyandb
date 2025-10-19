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
  import { fetchProperties, deleteProperty } from '@/api/index';
  import { getCurrentInstance } from '@vue/runtime-core';
  import { useRoute } from 'vue-router';
  import { ElMessage } from 'element-plus';
  import { reactive, ref, watch, onMounted } from 'vue';
  import { RefreshRight, Search } from '@element-plus/icons-vue';
  import { yamlToJson } from '@/utils/yaml';
  import PropertyEditor from './PropertyEditor.vue';
  import PropertyValueReader from './PropertyValueReader.vue';
  import FormHeader from '../common/FormHeader.vue';

  const { proxy } = getCurrentInstance();
  // Loading
  const route = useRoute();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const propertyEditorRef = ref();
  const propertyValueViewerRef = ref();
  const yamlRef = ref(null);
  const data = reactive({
    group: route.params.group,
    tableData: [],
    name: route.params.name,
  });
  const yamlCode = ref(`name: ${data.name}
limit: 10`);
  const getProperties = (params) => {
    $loadingCreate();
    fetchProperties({ groups: [data.group], name: data.name, limit: 10, ...params })
      .then((res) => {
        if (res.status === 200) {
          data.tableData = res.data.properties.map((item) => {
            item.tags.forEach((tag) => {
              tag.value = JSON.stringify(tag.value);
            });
            return item;
          });
        }
      })
      .catch((err) => {
        ElMessage({
          message: 'An error occurred while obtaining group data. Please refresh and try again. Error: ' + err,
          type: 'error',
          duration: 3000,
        });
      })
      .finally(() => {
        $loadingClose();
      });
  };
  const openPropertyView = (data) => {
    propertyValueViewerRef?.value.openDialog(data);
  };

  const ellipsizeValueData = (data) => {
    return data.value.slice(0, 20) + '...';
  };
  const openEditField = (index) => {
    const item = data.tableData[index];
    const param = {
      group: item.metadata.group,
      name: item.metadata.name,
      modRevision: item.metadata.modRevision,
      createRevision: item.metadata.createRevision,
      id: item.id,
      tags: JSON.parse(JSON.stringify(item.tags)),
    };
    propertyEditorRef?.value.openDialog(true, param).then(() => {
      getProperties();
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
  const deleteTableData = (index) => {
    const item = data.tableData[index];
    $loadingCreate();
    deleteProperty(item.metadata.group, item.metadata.name, item.id)
      .then((res) => {
        if (res.status === 200) {
          ElMessage({
            message: 'successed',
            type: 'success',
            duration: 5000,
          });
          getProperties();
        }
      })
      .catch((err) => {
        ElMessage({
          message: 'Please refresh and try again. Error: ' + err,
          type: 'error',
          duration: 3000,
        });
      })
      .finally(() => {
        $loadingClose();
      });
  };
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
        <div>
          <el-button size="small" :icon="Search" @click="searchProperties" plain />
          <el-button size="small" :icon="RefreshRight" @click="getProperties" plain />
        </div>
      </div>
      <CodeMirror ref="yamlRef" v-model="yamlCode" mode="yaml" style="height: 200px" :lint="true" />
      <el-table :data="data.tableData" style="width: 100%; margin-top: 20px" border>
        <el-table-column label="Group" prop="metadata.group" width="100"></el-table-column>
        <el-table-column label="Name" prop="metadata.name" width="120"></el-table-column>
        <el-table-column label="ModRevision" prop="metadata.modRevision" width="120"></el-table-column>
        <el-table-column label="CreateRevision" prop="metadata.createRevision" width="140"></el-table-column>
        <el-table-column label="ID" prop="id" width="150"></el-table-column>
        <el-table-column label="Tags">
          <template #default="scope">
            <el-table :data="scope.row.tags">
              <el-table-column label="Key" prop="key" width="150"></el-table-column>
              <el-table-column label="Value" prop="value">
                <template #default="scope">
                  {{ ellipsizeValueData(scope.row) }}
                  <el-button
                    link
                    type="primary"
                    @click.prevent="openPropertyView(scope.row)"
                    style="color: var(--color-main); font-weight: bold"
                    >view</el-button
                  >
                </template>
              </el-table-column>
            </el-table>
          </template>
        </el-table-column>
        <el-table-column label="Operator" width="150">
          <template #default="scope">
            <el-button
              link
              type="primary"
              @click.prevent="openEditField(scope.$index)"
              style="color: var(--color-main); font-weight: bold"
              >Edit</el-button
            >
            <el-popconfirm @confirm="deleteTableData(scope.$index)" title="Are you sure to delete this?">
              <template #reference>
                <el-button link type="danger" style="color: red; font-weight: bold">Delete</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
    <PropertyEditor ref="propertyEditorRef"></PropertyEditor>
    <PropertyValueReader ref="propertyValueViewerRef"></PropertyValueReader>
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
