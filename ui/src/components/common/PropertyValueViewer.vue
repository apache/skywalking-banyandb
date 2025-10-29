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

  const emit = defineEmits(['close']);

  // Viewer state
  const showDialog = ref(false);
  const dialogTitle = ref('');
  const valueData = reactive({
    data: '',
    formattedData: '',
  });
  const numSpaces = 2;

  // Open viewer with tag data
  const openViewer = (tagData) => {
    dialogTitle.value = 'Value of key ' + tagData.key;
    showDialog.value = true;
    valueData.data = tagData.value;
    try {
      valueData.formattedData = JSON.stringify(JSON.parse(valueData.data), null, numSpaces);
    } catch (error) {
      // If value is not valid JSON, display as-is
      valueData.formattedData = valueData.data;
    }
  };

  // Close viewer dialog
  const closeViewer = () => {
    showDialog.value = false;
    valueData.data = '';
    valueData.formattedData = '';
    emit('close');
  };

  // Download value as text file
  const downloadValue = () => {
    const dataBlob = new Blob([valueData.formattedData], { type: 'text/plain' });
    const a = document.createElement('a');
    a.download = 'property-value.txt';
    a.href = URL.createObjectURL(dataBlob);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  };

  // Expose methods to parent component
  defineExpose({
    openViewer,
    closeViewer,
  });
</script>

<template>
  <!-- Property Value Viewer Dialog -->
  <el-dialog v-model="showDialog" :title="dialogTitle" width="50%">
    <div class="value-content">{{ valueData.formattedData }}</div>
    <template #footer>
      <span class="dialog-footer footer">
        <el-button @click="closeViewer">Cancel</el-button>
        <el-button type="primary" @click.prevent="downloadValue"> Download </el-button>
      </span>
    </template>
  </el-dialog>
</template>

<style lang="scss" scoped>
  .footer {
    width: 100%;
    display: flex;
    justify-content: center;
  }
  .value-content {
    width: 100%;
    overflow: auto;
    max-height: 700px;
    white-space: pre;
    font-family: monospace;
    background-color: #f5f5f5;
    padding: 15px;
    border-radius: 4px;
  }
</style>
