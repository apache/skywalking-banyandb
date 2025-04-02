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
  import { StageConfig } from './data';

  const showDialog = ref(false);
  const ruleForm = ref();
  const title = ref('');
  const stage = ref(StageConfig);
  let promiseResolve;
  const closeDialog = () => {
    showDialog.value = false;
    stage.value = StageConfig;
  };
  const confirmApply = async () => {
    if (!ruleForm.value) return;
    await ruleForm.value.validate((valid) => {
      if (valid) {
        promiseResolve(JSON.parse(JSON.stringify(stage)));
        showDialog.value = false;
      }
    });
  };
  const openDialog = (data) => {
    if (data) {
      stage.value = data;
      title.value = 'Edit Stage';
    } else {
      title.value = 'Add Stage';
    }
    showDialog.value = true;
    return new Promise((resolve) => {
      promiseResolve = resolve;
    });
  };
  defineExpose({
    openDialog,
  });
</script>

<template>
  <el-dialog v-model="showDialog" :title="title" width="30%">
    <el-form ref="ruleForm" :model="stage" label-position="left">
      <el-form-item label="Name" prop="name" required label-width="150">
        <el-input v-model="stage.name" autocomplete="off" />
      </el-form-item>
      <el-form-item label="Shard Number" prop="shardNum" required label-width="150">
        <el-input-number v-model="stage.shardNum" autocomplete="off" />
      </el-form-item>
      <el-form-item label="TTL Unit" prop="ttlUnit" required label-width="150">
        <el-input v-model="stage.ttlUnit" autocomplete="off" />
      </el-form-item>
      <el-form-item label="TTL Number" prop="ttlNum" required label-width="150">
        <el-input-number v-model="stage.ttlNum" autocomplete="off" />
      </el-form-item>
      <el-form-item label="Segment Interval Unit" prop="segmentIntervalUnit" required label-width="150">
        <el-input v-model="stage.segmentIntervalUnit" autocomplete="off" />
      </el-form-item>
      <el-form-item label="Segment Interval Number" prop="segmentIntervalNum" required label-width="150">
        <el-input-number v-model="stage.segmentIntervalNum" autocomplete="off" />
      </el-form-item>
      <el-form-item label="Default Node Selector" prop="defaultNodeSelector" required label-width="150">
        <el-input v-model="stage.defaultNodeSelector" autocomplete="off" />
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
