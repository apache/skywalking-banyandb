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
        promiseResolve(JSON.parse(JSON.stringify(stage.value)));
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
      stage.value = StageConfig;
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
  <el-dialog v-model="showDialog" :title="title" width="600px">
    <el-form ref="ruleForm" :model="stage" label-position="left">
      <el-form-item label="Name" prop="name" required label-width="200">
        <el-input v-model="stage.name" autocomplete="off" style="width: 100%" />
      </el-form-item>
      <el-form-item label="Shard Number" prop="shardNum" required label-width="200">
        <el-input-number v-model="stage.shardNum" autocomplete="off" style="width: 100%" />
      </el-form-item>
      <el-form-item label="TTL Unit" prop="ttlUnit" required label-width="200">
        <el-select v-model="stage.ttlUnit" placeholder="please select" style="width: 100%">
          <el-option label="Hour" value="UNIT_HOUR"></el-option>
          <el-option label="Day" value="UNIT_DAY"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="TTL Number" prop="ttlNum" required label-width="200">
        <el-input-number v-model="stage.ttlNum" autocomplete="off" style="width: 100%" />
      </el-form-item>
      <el-form-item label="Segment Interval Unit" prop="segmentIntervalUnit" required label-width="200">
        <el-select v-model="stage.segmentIntervalUnit" placeholder="please select" style="width: 100%">
          <el-option label="Hour" value="UNIT_HOUR"></el-option>
          <el-option label="Day" value="UNIT_DAY"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Segment Interval Number" prop="segmentIntervalNum" required label-width="200">
        <el-input-number v-model="stage.segmentIntervalNum" autocomplete="off" style="width: 100%" />
      </el-form-item>
      <el-form-item label="Node Selector" prop="nodeSelector" required label-width="200">
        <el-input v-model="stage.nodeSelector" autocomplete="off" />
      </el-form-item>
      <el-form-item label="Close" prop="close" label-width="200">
        <el-switch v-model="stage.close" />
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
