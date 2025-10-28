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
  import { ref } from 'vue';
  import { Plus } from '@element-plus/icons-vue';
  import { ElMessage } from 'element-plus';
  import BydbQLQuery from '@/components/BydbQL/Index.vue';

  let tabIndex = 2;
  const activeTab = ref('tab-1');
  const tabs = ref([
    {
      name: 'tab-1',
      title: 'Query 1',
      closable: false,
    },
  ]);

  const addTab = () => {
    const newTabName = `tab-${tabIndex++}`;
    tabs.value.push({
      name: newTabName,
      title: `Query ${tabIndex - 1}`,
      closable: true,
    });
    activeTab.value = newTabName;
  };

  const removeTab = (targetName) => {
    if (tabs.value.length === 1) {
      ElMessage.warning('At least one tab must remain');
      return;
    }

    const targetIndex = tabs.value.findIndex((tab) => tab.name === targetName);
    const targetTab = tabs.value[targetIndex];

    if (!targetTab.closable) {
      ElMessage.warning('This tab cannot be closed');
      return;
    }

    tabs.value.splice(targetIndex, 1);

    if (activeTab.value === targetName) {
      const newActiveTab = tabs.value[targetIndex] || tabs.value[targetIndex - 1];
      activeTab.value = newActiveTab.name;
    }
  };
</script>

<template>
  <div class="query-page">
    <div class="tabs-header">
      <el-tabs v-model="activeTab" class="query-tabs" closable @tab-remove="removeTab">
        <el-tab-pane v-for="tab in tabs" :key="tab.name" :label="tab.title" :name="tab.name" :closable="tab.closable">
          <BydbQLQuery />
        </el-tab-pane>
      </el-tabs>
      <el-button
        :icon="Plus"
        size="small"
        type="primary"
        circle
        @click="addTab"
        class="add-tab-button"
        title="Add new query tab"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  .query-page {
    height: 100%;
    display: flex;
    flex-direction: column;
    padding: 10px 20px;
  }

  .tabs-header {
    flex: 1;
    display: flex;
    flex-direction: column;
    position: relative;

    :deep(.el-tabs__header) {
      margin-bottom: 0;
    }

    .add-tab-button {
      position: absolute;
      top: 5px;
      right: 10px;
      z-index: 10;
      width: 28px;
      height: 28px;
      padding: 0;
      display: flex;
      align-items: center;
      justify-content: center;
    }
  }

  .query-tabs {
    flex: 1;
    display: flex;
    flex-direction: column;

    :deep(.el-tabs__content) {
      flex: 1;
      overflow: auto;
    }

    :deep(.el-tab-pane) {
      height: 100%;
      padding-top: 10px;
    }

    :deep(.el-tabs__header) {
      padding-right: 50px;
    }
  }
</style>
