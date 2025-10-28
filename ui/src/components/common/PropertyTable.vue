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
  import { defineProps, defineEmits, reactive, ref, getCurrentInstance } from 'vue';
  import { ElMessage } from 'element-plus';
  import TagEditor from '@/components/Property/TagEditor.vue';
  import { applyProperty, deleteProperty } from '@/api/index';
  import { rules, strategyGroup, formConfig } from '@/components/Property/data';

  const { proxy } = getCurrentInstance();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;

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

  const emit = defineEmits(['refresh']);

  // Property Value Viewer state
  const showValueDialog = ref(false);
  const valueDialogTitle = ref('');
  const valueData = reactive({
    data: '',
    formattedData: '',
  });
  const numSpaces = 2;

  // Property Editor state
  const showEditorDialog = ref(false);
  const editorTitle = ref('');
  const tagEditorRef = ref();
  const ruleForm = ref();
  const formData = reactive({
    strategy: strategyGroup[0].value,
    group: '',
    name: '',
    modRevision: 0,
    createRevision: 0,
    id: '',
    tags: [],
  });

  const ellipsizeValueData = (data) => {
    if (!data.value || data.value.length <= props.maxValueLength) {
      return data.value;
    }
    return data.value.slice(0, props.maxValueLength) + '...';
  };

  // Property Value Viewer functions
  const handleViewValue = (tagData) => {
    valueDialogTitle.value = 'Value of key ' + tagData.key;
    showValueDialog.value = true;
    valueData.data = tagData.value;
    valueData.formattedData = JSON.stringify(JSON.parse(valueData.data), null, numSpaces);
  };

  const closeValueDialog = () => {
    showValueDialog.value = false;
  };

  const downloadValue = () => {
    const dataBlob = new Blob([valueData.formattedData], { type: 'text/JSON' });
    var a = document.createElement('a');
    a.download = 'value.txt';
    a.href = URL.createObjectURL(dataBlob);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  };

  // Property Editor functions
  const initEditorData = () => {
    formData.strategy = strategyGroup[0].value;
    formData.group = '';
    formData.name = '';
    formData.modRevision = 0;
    formData.createRevision = 0;
    formData.id = '';
    formData.tags = [];
  };

  const closeEditorDialog = () => {
    showEditorDialog.value = false;
    initEditorData();
  };

  const openEditTag = (index) => {
    tagEditorRef.value.openDialog(formData.tags[index]).then((res) => {
      formData.tags[index].key = res.key;
      formData.tags[index].value = res.value;
    });
  };

  const deleteTag = (index) => {
    formData.tags.splice(index, 1);
  };

  const openAddTag = () => {
    tagEditorRef.value.openDialog().then((res) => {
      formData.tags.push(res);
    });
  };

  const confirmApply = async () => {
    if (!ruleForm.value) return;
    await ruleForm.value.validate(async (valid) => {
      if (valid) {
        $loadingCreate();
        const param = {
          strategy: formData.strategy,
          property: {
            id: formData.id,
            metadata: {
              createRevision: formData.createRevision,
              group: formData.group,
              modRevision: formData.modRevision,
              name: formData.name,
            },
            tags: formData.tags.map((item) => {
              return {
                key: item.key,
                value: JSON.parse(item.value),
              };
            }),
          },
        };
        const response = await applyProperty(formData.group, formData.name, formData.id, param);
        $loadingClose();
        if (response.error) {
          ElMessage({
            message: `Failed to apply property: ${response.error.message}`,
            type: 'error',
          });
          return;
        }
        ElMessage({
          message: 'successed',
          type: 'success',
        });
        showEditorDialog.value = false;
        emit('refresh');
      }
    });
  };

  const handleEdit = (index) => {
    const item = props.data[index];
    showEditorDialog.value = true;
    editorTitle.value = 'Edit Property';
    formData.group = item.metadata.group;
    formData.name = item.metadata.name;
    formData.modRevision = item.metadata.modRevision;
    formData.createRevision = item.metadata.createRevision;
    formData.id = item.id;
    formData.tags = JSON.parse(JSON.stringify(item.tags));
  };

  const handleDelete = async (index) => {
    const item = props.data[index];
    $loadingCreate();
    const response = await deleteProperty(item.metadata.group, item.metadata.name, item.id);
    $loadingClose();
    if (response.error) {
      ElMessage({
        message: `Failed to delete property: ${response.error.message}`,
        type: 'error',
      });
      return;
    }
    ElMessage({
      message: 'successed',
      type: 'success',
    });
    emit('refresh');
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

  <!-- Property Value Viewer Dialog -->
  <el-dialog v-model="showValueDialog" :title="valueDialogTitle">
    <div class="configuration">{{ valueData.formattedData }}</div>
    <template #footer>
      <span class="dialog-footer footer">
        <el-button @click="closeValueDialog">Cancel</el-button>
        <el-button type="primary" @click.prevent="downloadValue()"> Download </el-button>
      </span>
    </template>
  </el-dialog>

  <!-- Property Editor Dialog -->
  <el-dialog v-model="showEditorDialog" :title="editorTitle" width="50%">
    <el-form ref="ruleForm" :rules="rules" :model="formData" label-position="left">
      <el-form-item v-for="item in formConfig" :key="item.prop" :label="item.label" :prop="item.prop" label-width="200">
        <el-select
          v-if="item.type === 'select'"
          v-model="formData[item.prop]"
          placeholder="please select"
          style="width: 100%"
        >
          <el-option
            v-for="option in item.selectGroup"
            :key="option.value"
            :label="option.label"
            :value="option.value"
          ></el-option>
        </el-select>
        <el-input
          v-if="item.type === 'input'"
          v-model="formData[item.prop]"
          :disabled="item.disabled"
          autocomplete="off"
        ></el-input>
        <el-input-number v-if="item.type === 'number'" v-model="formData[item.prop]" :min="0"></el-input-number>
      </el-form-item>
      <el-form-item label="Tags" prop="tags" label-width="200">
        <el-button size="small" type="primary" color="#6E38F7" @click="openAddTag">Add Tag</el-button>
        <el-table style="margin-top: 10px" :data="formData.tags" border>
          <el-table-column label="Key" prop="key"></el-table-column>
          <el-table-column label="Value" prop="value"></el-table-column>
          <el-table-column label="Operator" width="150">
            <template #default="scope">
              <el-button
                link
                type="primary"
                @click.prevent="openEditTag(scope.$index)"
                style="color: var(--color-main); font-weight: bold"
                >Edit</el-button
              >
              <el-popconfirm @confirm="deleteTag(scope.$index)" title="Are you sure to delete this?">
                <template #reference>
                  <el-button link type="danger" style="color: red; font-weight: bold">Delete</el-button>
                </template>
              </el-popconfirm>
            </template>
          </el-table-column>
        </el-table>
      </el-form-item>
    </el-form>
    <template #footer>
      <span class="dialog-footer footer">
        <el-button @click="closeEditorDialog">Cancel</el-button>
        <el-button type="primary" @click="confirmApply"> Confirm </el-button>
      </span>
    </template>
  </el-dialog>

  <!-- Tag Editor Dialog -->
  <TagEditor ref="tagEditorRef"></TagEditor>
</template>

<style lang="scss" scoped>
  .footer {
    width: 100%;
    display: flex;
    justify-content: center;
  }
  .configuration {
    width: 100%;
    overflow: auto;
    max-height: 700px;
    white-space: pre;
  }
</style>
