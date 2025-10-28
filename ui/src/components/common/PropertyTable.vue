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
  import { defineProps, defineEmits } from 'vue';

  const props = defineProps({
    // Table data
    data: {
      type: Array,
      default: () => [],
    },
    // Loading state
    loading: {
      type: Boolean,
      default: false,
    },
    // Table empty text
    emptyText: {
      type: String,
      default: 'No data yet',
    },
    // Show operator column (Edit/Delete)
    showOperator: {
      type: Boolean,
      default: true,
    },
    // Border style
    border: {
      type: Boolean,
      default: true,
    },
    // Stripe style
    stripe: {
      type: Boolean,
      default: false,
    },
    // Max characters to show before ellipsis for tag values
    maxValueLength: {
      type: Number,
      default: 20,
    },
  });

  const emit = defineEmits(['edit', 'delete', 'view-value']);

  const ellipsizeValueData = (data) => {
    if (!data.value || data.value.length <= props.maxValueLength) {
      return data.value;
    }
    return data.value.slice(0, props.maxValueLength) + '...';
  };

  const handleEdit = (index) => {
    emit('edit', index);
  };

  const handleDelete = (index) => {
    emit('delete', index);
  };

  const handleViewValue = (tagData) => {
    emit('view-value', tagData);
  };
</script>

<template>
  <el-table 
    v-loading="loading"
    element-loading-text="loading"
    element-loading-spinner="el-icon-loading"
    element-loading-background="rgba(0, 0, 0, 0.8)"
    :data="data" 
    style="width: 100%" 
    :border="border"
    :stripe="stripe"
    :empty-text="emptyText"
  >
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
                @click.prevent="handleViewValue(scope.row)"
                style="color: var(--color-main); font-weight: bold"
                >view</el-button
              >
            </template>
          </el-table-column>
        </el-table>
      </template>
    </el-table-column>
    <el-table-column v-if="showOperator" label="Operator" width="150">
      <template #default="scope">
        <el-button
          link
          type="primary"
          @click.prevent="handleEdit(scope.$index)"
          style="color: var(--color-main); font-weight: bold"
          >Edit</el-button
        >
        <el-popconfirm @confirm="handleDelete(scope.$index)" title="Are you sure to delete this?">
          <template #reference>
            <el-button link type="danger" style="color: red; font-weight: bold">Delete</el-button>
          </template>
        </el-popconfirm>
      </template>
    </el-table-column>
  </el-table>
</template>

<style lang="scss" scoped>
</style>

