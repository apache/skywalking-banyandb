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
  import { reactive, ref, computed } from 'vue';

  // Constants
  const DIALOG_MODE = {
    ADD: 'add',
    EDIT: 'edit',
  };

  const DIALOG_TITLES = {
    [DIALOG_MODE.ADD]: 'Add Tag',
    [DIALOG_MODE.EDIT]: 'Edit Tag',
  };

  // Validation rules
  const rules = {
    key: [
      { required: true, message: 'Please enter tag key', trigger: 'blur' },
      { min: 1, max: 100, message: 'Key length should be 1-100 characters', trigger: 'blur' },
    ],
    value: [{ required: true, message: 'Please enter tag value', trigger: 'blur' }],
  };

  // Factory function for initial form data
  const createInitialFormData = () => ({
    key: '',
    value: '',
  });

  // Refs
  const showDialog = ref(false);
  const dialogMode = ref(DIALOG_MODE.ADD);
  const ruleForm = ref();
  const formData = reactive(createInitialFormData());

  // Promise handlers
  let promiseResolve = null;
  let promiseReject = null;

  // Computed properties
  const dialogTitle = computed(() => DIALOG_TITLES[dialogMode.value]);

  // Reset form data to initial state
  const resetFormData = () => {
    Object.assign(formData, createInitialFormData());
  };

  // Close dialog and cleanup
  const closeDialog = () => {
    showDialog.value = false;
    ruleForm.value?.resetFields();
    resetFormData();

    // Reject promise if user cancels
    if (promiseReject) {
      promiseReject(new Error('User cancelled'));
      promiseReject = null;
    }
    promiseResolve = null;
  };

  // Confirm and return data
  const confirmApply = async () => {
    if (!ruleForm.value) return;

    try {
      const isValid = await ruleForm.value.validate();
      if (!isValid) return;

      // Return a copy of the data
      const result = { ...formData };

      if (promiseResolve) {
        promiseResolve(result);
        promiseResolve = null;
        promiseReject = null;
      }

      showDialog.value = false;
      resetFormData();
    } catch (error) {
      // Validation failed, do nothing
    }
  };

  // Open dialog for add/edit
  const openDialog = (data = null) => {
    if (data) {
      dialogMode.value = DIALOG_MODE.EDIT;
      formData.key = data.key;
      formData.value = data.value;
    } else {
      dialogMode.value = DIALOG_MODE.ADD;
      resetFormData();
    }

    showDialog.value = true;

    return new Promise((resolve, reject) => {
      promiseResolve = resolve;
      promiseReject = reject;
    });
  };

  // Expose methods
  defineExpose({
    openDialog,
  });
</script>

<template>
  <el-dialog v-model="showDialog" :title="dialogTitle" width="30%">
    <el-form ref="ruleForm" :rules="rules" :model="formData" label-position="left">
      <el-form-item label="Key" prop="key" label-width="150">
        <el-input v-model="formData.key" autocomplete="off" placeholder="Enter tag key"></el-input>
      </el-form-item>
      <el-form-item label="Value" prop="value" label-width="150">
        <el-input
          v-model="formData.value"
          type="textarea"
          :rows="4"
          autocomplete="off"
          placeholder="Enter tag value (JSON format)"
        ></el-input>
      </el-form-item>
    </el-form>
    <template #footer>
      <span class="dialog-footer footer">
        <el-button @click="closeDialog">Cancel</el-button>
        <el-button type="primary" @click="confirmApply"> Confirm </el-button>
      </span>
    </template>
  </el-dialog>
</template>

<style scoped lang="scss">
  .footer {
    width: 100%;
    display: flex;
    justify-content: center;
  }
</style>
