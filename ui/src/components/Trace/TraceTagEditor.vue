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
  import { ref, reactive } from 'vue';
  import { tagTypeOptions } from './data';

  const visible = ref(false);
  const ruleForm = ref(null);
  const resolve = ref(null);

  const formData = reactive({
    key: '',
    value: 'TAG_TYPE_STRING',
  });

  const rules = {
    key: [{ required: true, message: 'Please input tag name', trigger: 'blur' }],
    value: [{ required: true, message: 'Please select tag type', trigger: 'change' }],
  };

  function openDialog(data) {
    visible.value = true;
    if (data) {
      formData.key = data.key;
      formData.value = data.value;
    } else {
      formData.key = '';
      formData.value = 'TAG_TYPE_STRING';
    }

    return new Promise((res) => {
      resolve.value = res;
    });
  }

  function handleClose() {
    visible.value = false;
    ruleForm.value?.resetFields();
  }

  function confirm() {
    ruleForm.value?.validate((valid) => {
      if (valid) {
        resolve.value({
          key: formData.key,
          value: formData.value,
        });
        handleClose();
      }
    });
  }

  defineExpose({
    openDialog,
  });
</script>

<template>
  <el-dialog v-model="visible" title="Tag Editor" width="600px" @close="handleClose">
    <el-form ref="ruleForm" :model="formData" :rules="rules" label-width="150px">
      <el-form-item label="Tag Name" prop="key">
        <el-input v-model="formData.key" placeholder="Please input tag name"></el-input>
      </el-form-item>
      <el-form-item label="Tag Type" prop="value">
        <el-select v-model="formData.value" placeholder="Please select tag type" style="width: 100%">
          <el-option v-for="item in tagTypeOptions" :key="item.value" :label="item.label" :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
    </el-form>
    <template #footer>
      <span class="dialog-footer">
        <el-button @click="handleClose">Cancel</el-button>
        <el-button type="primary" @click="confirm" color="#6E38F7">Confirm</el-button>
      </span>
    </template>
  </el-dialog>
</template>

<style lang="scss" scoped></style>

