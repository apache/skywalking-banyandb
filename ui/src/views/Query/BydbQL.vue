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
  import BydbQLQuery from '@/components/BydbQL/Index.vue';

  const activeTab = ref('bydbql');
</script>

<template>
  <div class="query-page">
    <el-tabs v-model="activeTab" class="query-tabs">
      <el-tab-pane label="BydbQL" name="bydbql">
        <BydbQLQuery />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style lang="scss" scoped>
  .query-page {
    height: 100%;
    display: flex;
    flex-direction: column;
    padding: 20px;
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
    }
  }
</style>

