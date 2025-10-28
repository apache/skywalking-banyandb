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
  import { reactive, ref, computed, getCurrentInstance } from 'vue';
  import { ElMessage } from 'element-plus';
  import TagEditor from '@/components/common/TagEditor.vue';
  import { applyProperty } from '@/api/index';
  import { rules, strategyGroup, formConfig } from '@/components/Property/data';

  const EDITOR_MODE = {
    CREATE: 'create',
    EDIT: 'edit',
  };
  const DIALOG_TITLES = {
    [EDITOR_MODE.CREATE]: 'Create Property',
    [EDITOR_MODE.EDIT]: 'Edit Property',
  };
  const { proxy } = getCurrentInstance();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const emit = defineEmits(['refresh', 'close']);
  const createInitialFormData = () => ({
    strategy: strategyGroup[0].value,
    group: '',
    name: '',
    modRevision: 0,
    createRevision: 0,
    id: '',
    tags: [],
  });

  const showDialog = ref(false);
  const editorMode = ref(EDITOR_MODE.CREATE);
  const tagEditorRef = ref();
  const ruleForm = ref();
  const formData = reactive(createInitialFormData());

  const editorTitle = computed(() => DIALOG_TITLES[editorMode.value]);
  const resetFormData = () => {
    Object.assign(formData, createInitialFormData());
  };
  const deepClone = (obj) => {
    try {
      return structuredClone(obj);
    } catch (e) {
      // Fallback for older browsers
      return JSON.parse(JSON.stringify(obj));
    }
  };

  const openEditor = (propertyData = null) => {
    if (propertyData) {
      editorMode.value = EDITOR_MODE.EDIT;
      formData.group = propertyData.metadata.group;
      formData.name = propertyData.metadata.name;
      formData.modRevision = propertyData.metadata.modRevision;
      formData.createRevision = propertyData.metadata.createRevision;
      formData.id = propertyData.id;
      formData.tags = deepClone(propertyData.tags);
    } else {
      editorMode.value = EDITOR_MODE.CREATE;
      resetFormData();
    }
    showDialog.value = true;
  };

  const closeEditor = () => {
    showDialog.value = false;
    ruleForm.value?.resetFields();
    resetFormData();
    emit('close');
  };

  // Tag management functions with error handling.
  const openEditTag = async (index) => {
    try {
      const result = await tagEditorRef.value.openDialog(formData.tags[index]);
      formData.tags[index] = { ...formData.tags[index], ...result };
    } catch (error) {
      // User cancelled the dialog, do nothing
    }
  };

  const deleteTag = (index) => {
    formData.tags.splice(index, 1);
  };

  const openAddTag = async () => {
    try {
      const result = await tagEditorRef.value.openDialog();
      formData.tags.push(result);
    } catch (error) {
      // User cancelled the dialog, do nothing
    }
  };

  const buildPropertyPayload = () => ({
    strategy: formData.strategy,
    property: {
      id: formData.id,
      metadata: {
        createRevision: formData.createRevision,
        group: formData.group,
        modRevision: formData.modRevision,
        name: formData.name,
      },
      tags: formData.tags.map(({ key, value }) => ({
        key,
        value: JSON.parse(value),
      })),
    },
  });

  // Confirm and apply property with improved error handling.
  const confirmApply = async () => {
    if (!ruleForm.value) return;

    try {
      const isValid = await ruleForm.value.validate();
      if (!isValid) return;

      $loadingCreate();

      const payload = buildPropertyPayload();
      const response = await applyProperty(
        formData.group,
        formData.name,
        formData.id,
        payload
      );

      if (response.error) {
        throw new Error(response.error.message);
      }

      ElMessage({
        message: 'Property applied successfully',
        type: 'success',
      });

      showDialog.value = false;
      emit('refresh');
      resetFormData();
    } catch (error) {
      ElMessage({
        message: `Failed to apply property: ${error.message || 'Unknown error'}`,
        type: 'error',
      });
    } finally {
      $loadingClose();
    }
  };

  defineExpose({
    openEditor,
    closeEditor,
  });
</script>

<template>
  <!-- Property Editor Dialog -->
  <el-dialog v-model="showDialog" :title="editorTitle" width="80%" style="height: 75vh;">
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
        <el-table style="margin-top: 10px; height: 40vh; overflow: auto;" :data="formData.tags" border >
          <el-table-column label="Key" prop="key" width="200"></el-table-column>
          <el-table-column label="Value" prop="value"></el-table-column>
          <el-table-column label="Operator" width="150">
            <template #default="scope">
              <el-button
                link
                type="primary"
                @click.prevent="openEditTag(scope.$index)"
                style="color: var(--color-main); font-weight: bold"
                >Edit</el-button>
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
        <el-button @click="closeEditor">Cancel</el-button>
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
</style>

