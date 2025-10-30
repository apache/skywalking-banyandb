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
  const props = defineProps({
    // Full data array
    data: {
      type: Array,
      default: () => [],
    },
    // Span tags to display as columns
    spanTags: {
      type: Array,
      default: () => [],
    },
    // Loading state
    loading: {
      type: Boolean,
      default: false,
    },
    // Show selection column
    showSelection: {
      type: Boolean,
      default: true,
    },
    // Border style
    border: {
      type: Boolean,
      default: true,
    },
    // Enable traceId cell merging
    enableMerge: {
      type: Boolean,
      default: true,
    },
    // Empty text
    emptyText: {
      type: String,
      default: 'No trace data found',
    },
  });

  const emit = defineEmits(['selection-change']);

  // Extract tag value with proper formatting
  const getTagValue = (data) => {
    let value = data.value;

    const isNullish = (val) => val === null || val === undefined || val === 'null';
    if (isNullish(value)) {
      return 'N/A';
    }
    for (let i = 0; i < 2; i++) {
      if (typeof value !== 'object') {
        const strValue = value.toString();
        return strValue.length > 100 ? strValue.substring(0, 100) + '...' : strValue;
      }
      for (const key in value) {
        if (Object.hasOwn(value, key)) {
          value = value[key];
          break;
        }
      }
      if (isNullish(value)) {
        return 'N/A';
      }
    }

    const strValue = value.toString();
    return strValue.length > 100 ? strValue.substring(0, 100) + '...' : strValue;
  };

  // Cell merging strategy for traceId
  const objectSpanMethod = ({ row, column, rowIndex, columnIndex }) => {
    if (!props.enableMerge) {
      return;
    }

    // Only merge the traceId column (first column after selection)
    const traceIdColumnIndex = props.showSelection ? 1 : 0;
    if (columnIndex === traceIdColumnIndex) {
      const currentTraceId = row.traceId;
      // Check if this is the first row with this traceId
      if (rowIndex === 0 || props.data[rowIndex - 1].traceId !== currentTraceId) {
        // Count how many rows have the same traceId
        let rowspan = 1;
        for (let i = rowIndex + 1; i < props.data.length; i++) {
          if (props.data[i].traceId === currentTraceId) {
            rowspan++;
          } else {
            break;
          }
        }
        return {
          rowspan: rowspan,
          colspan: 1,
        };
      } else {
        // This row's traceId is merged with a previous row
        return {
          rowspan: 0,
          colspan: 0,
        };
      }
    }
  };
  // Handle selection change
  function handleSelectionChange(selection) {
    emit('selection-change', selection);
  }
</script>

<template>
  <div class="trace-table">
    <el-table
      v-loading="loading"
      element-loading-text="loading"
      element-loading-spinner="el-icon-loading"
      element-loading-background="rgba(0, 0, 0, 0.8)"
      :data="data"
      :border="border"
      style="width: 100%"
      @selection-change="handleSelectionChange"
      :span-method="objectSpanMethod"
      :empty-text="emptyText"
    >
      <el-table-column v-if="showSelection" type="selection" width="55" fixed />
      <el-table-column label="traceId" prop="traceId" width="200" fixed>
        <template #default="scope">
          {{ getTagValue({ value: scope.row.traceId }) }}
        </template>
      </el-table-column>
      <el-table-column label="spanId" prop="spanId" width="300" fixed>
        <template #default="scope">
          {{ getTagValue({ value: scope.row.spanId }) }}
        </template>
      </el-table-column>
      <el-table-column v-for="tag in spanTags" :key="tag" :label="tag" :prop="tag" min-width="200">
        <template #default="scope">
          {{ getTagValue({ value: scope.row[tag] }) }}
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style lang="scss" scoped>
  .trace-table {
    width: 100%;
    overflow-x: auto;
  }
</style>
