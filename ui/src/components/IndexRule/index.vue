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
import { reactive } from 'vue';
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { useRoute } from 'vue-router'
import { getIndexRuleOrIndexRuleBinding } from '@/api/index'

const { proxy } = getCurrentInstance()
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose

const data = reactive({
  group: '',
  name: '',
  type: '',
  operator: '',
  indexRule: {}
})

const route = useRoute()

watch(() => route, () => {
  data.group = route.params.group
  data.name = route.params.name
  data.type = route.params.type
  data.operator = route.params.operator
  initData()
}, {
  immediate: true,
  deep: true
})

function initData() {
  if (data.type && data.group && data.name) {
    $loadingCreate()
    getIndexRuleOrIndexRuleBinding(data.type, data.group, data.name)
      .then(result => {
        data.indexRule = result.data.indexRule
      })
      .catch(err => {
        ElMessage({
          message: 'Please refresh and try again. Error: ' + err,
          type: "error",
          duration: 3000
        })
      })
      .finally(() => {
        $loadingClose()
      })
  }
}
</script>

<template>
  <div>
    <el-card>
      <template #header>
        <div class="flex">
          <span class="text-bold">Group：</span>
          <span style="margin-right: 20px;">{{ data.group }}</span>
          <span class="text-bold">Name：</span>
          <span style="margin-right: 20px;">{{ data.name }}</span>
          <span class="text-bold">Type：</span>
          <span style="margin-right: 20px;">{{ data.type }}</span>
          <span class="text-bold">Operation：</span>
          <span>Read</span>
        </div>
      </template>
      <el-form label-position="left" label-width="100px" :model="data.indexRule" style="width: 50%;">
        <el-form-item label="Analyzer">
          <el-input v-model="data.indexRule.analyzer" :disabled="true"></el-input>
        </el-form-item>
        <el-form-item label="Location">
          <el-input v-model="data.indexRule.location" :disabled="true"></el-input>
        </el-form-item>
        <el-form-item label="Tags">
          <el-select v-model="data.indexRule.tags" style="width: 100%;" :disabled="true" multiple></el-select>
        </el-form-item>
        <el-form-item label="Type">
          <el-input v-model="data.indexRule.type" :disabled="true"></el-input>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
::v-deep {
  .el-card {
    margin: 15px;
  }
}
</style>