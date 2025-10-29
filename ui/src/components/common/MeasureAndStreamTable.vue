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
  import { defineProps, ref, computed } from 'vue';

  const props = defineProps({
    tableData: {
      type: Array,
      default: () => [],
    },
    loading: {
      type: Boolean,
      default: false,
    },
    tableHeader: {
      type: Array,
      default: () => [],
    },
    showSelection: {
      type: Boolean,
      default: true,
    },
    showIndex: {
      type: Boolean,
      default: true,
    },
    showTimestamp: {
      type: Boolean,
      default: true,
    },
    emptyText: {
      type: String,
      default: 'No data yet',
    },
  });

  const currentPage = ref(1);
  const pageSize = ref(10);

  const paginatedData = computed(() => {
    const start = (currentPage.value - 1) * pageSize.value;
    const end = start + pageSize.value;
    return props.tableData.slice(start, end);
  });

  const handleCurrentChange = (val) => {
    currentPage.value = val;
  };
</script>

<template>
  <div class="measure-and-stream-table">
    <el-table
      v-loading="loading"
      element-loading-text="loading"
      element-loading-spinner="el-icon-loading"
      element-loading-background="rgba(0, 0, 0, 0.8)"
      ref="multipleTable"
      stripe
      :border="true"
      highlight-current-row
      tooltip-effect="dark"
      :empty-text="emptyText"
      :data="paginatedData"
      style="width: 100%"
    >
      <el-table-column v-if="showSelection" type="selection" width="55"> </el-table-column>
      <el-table-column v-if="showIndex" type="index" label="number" width="90"> </el-table-column>
      <el-table-column
        v-if="showTimestamp"
        label="timestamp"
        width="260"
        key="timestamp"
        prop="timestamp"
      ></el-table-column>
      <el-table-column
        v-for="item in tableHeader"
        sortable
        :key="item.name"
        :label="item.label"
        :prop="item.name"
        show-overflow-tooltip
      >
        <template #default="scope">
          <el-popover
            v-if="(item.type || item.fieldType)?.includes(`ARRAY`) && scope.row[item.name] !== `Null`"
            effect="dark"
            trigger="hover"
            placement="top"
            width="auto"
          >
            <template #default>
              <div>{{ scope.row[item.name].join('; ') }}</div>
            </template>
            <template #reference>
              <el-tag>View</el-tag>
            </template>
          </el-popover>
          <div v-else>{{ scope.row[item.name] }}</div>
        </template>
      </el-table-column>
    </el-table>
    <div style="margin-top: 20px; display: flex; justify-content: flex-end">
      <el-pagination
        v-model:current-page="currentPage"
        :page-size="pageSize"
        :total="tableData.length"
        layout="total, prev, pager, next, jumper"
        @current-change="handleCurrentChange"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  .measure-and-stream-table {
    width: 100%;
    overflow-x: auto;
  }
</style>
