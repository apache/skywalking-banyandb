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
  import { ref, computed, watch } from 'vue';

  const props = defineProps({
    // Full data array
    data: {
      type: Array,
      default: () => [],
    },
    // Table columns configuration
    columns: {
      type: Array,
      default: () => [],
    },
    // Loading state
    loading: {
      type: Boolean,
      default: false,
    },
    // Page size
    pageSize: {
      type: Number,
      default: 10,
    },
    // Table empty text
    emptyText: {
      type: String,
      default: 'No data yet',
    },
    // Show selection column
    showSelection: {
      type: Boolean,
      default: false,
    },
    // Show index column
    showIndex: {
      type: Boolean,
      default: false,
    },
    // Show timestamp column
    showTimestamp: {
      type: Boolean,
      default: false,
    },
    // Table header (for MeasureAndStreamTable compatibility)
    tableHeader: {
      type: Array,
      default: () => [],
    },
    // Show pagination
    showPagination: {
      type: Boolean,
      default: true,
    },
    // Stripe style
    stripe: {
      type: Boolean,
      default: true,
    },
    // Border style
    border: {
      type: Boolean,
      default: true,
    },
    // Minimum height for table
    minHeight: {
      type: String,
      default: '440px',
    },
  });

  const currentPage = ref(1);

  // Compute paginated data
  const paginatedData = computed(() => {
    if (!props.showPagination) {
      return props.data;
    }
    const start = (currentPage.value - 1) * props.pageSize;
    const end = start + props.pageSize;
    return props.data.slice(start, end);
  });

  // Compute effective columns (use columns or tableHeader)
  const effectiveColumns = computed(() => {
    return props.columns.length > 0 ? props.columns : props.tableHeader;
  });

  // Handle page change
  function handlePageChange(page) {
    currentPage.value = page;
  }

  // Reset to first page when data changes
  watch(() => props.data, () => {
    currentPage.value = 1;
  });
</script>

<template>
  <div class="topn-table">
    <el-table
      v-loading="loading"
      element-loading-text="loading"
      element-loading-spinner="el-icon-loading"
      element-loading-background="rgba(0, 0, 0, 0.8)"
      :data="paginatedData"
      :style="{ width: '100%', margin: '10px 0', minHeight: minHeight }"
      :stripe="stripe"
      :border="border"
      highlight-current-row
      tooltip-effect="dark"
      :empty-text="emptyText"
    >
      <el-table-column v-if="showSelection" type="selection" width="55" />
      <el-table-column v-if="showIndex" type="index" label="number" width="90" />
      <el-table-column
        v-if="showTimestamp"
        label="timestamp"
        width="260"
        key="timestamp"
        prop="timestamp"
      />
      
      <!-- Dynamic columns -->
      <el-table-column
        v-for="column in effectiveColumns"
        :key="column.name || column.prop"
        :label="column.label"
        :prop="column.name || column.prop"
        :width="column.width"
        :sortable="column.sortable !== false"
        show-overflow-tooltip
      >
        <template #default="scope">
          <!-- Handle array types with popover -->
          <el-popover
            v-if="(column.type || column.fieldType)?.includes('ARRAY') && scope.row[column.name || column.prop] !== 'Null'"
            effect="dark"
            trigger="hover"
            placement="top"
            width="auto"
          >
            <template #default>
              <div>{{ scope.row[column.name || column.prop].join('; ') }}</div>
            </template>
            <template #reference>
              <el-tag>View</el-tag>
            </template>
          </el-popover>
          <!-- Regular display -->
          <div v-else>{{ scope.row[column.name || column.prop] }}</div>
        </template>
      </el-table-column>

      <!-- Slot for custom columns -->
      <slot name="columns" />
    </el-table>

    <!-- Pagination -->
    <el-pagination
      v-if="showPagination && data.length > 0"
      background
      layout="prev, pager, next"
      :page-size="pageSize"
      :total="data.length"
      :current-page="currentPage"
      @current-change="handlePageChange"
    />
  </div>
</template>

<style lang="scss" scoped>
  .topn-table {
    width: 100%;
  }

  .el-pagination {
    margin-top: 10px;
  }
</style>

