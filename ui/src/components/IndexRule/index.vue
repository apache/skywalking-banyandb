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
import { getSecondaryDataModel } from '@/api/index'
import FormHeader from '../common/FormHeader.vue'

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

async function initData() {
  if (!(data.type && data.group && data.name)) {
    return;
  }
  $loadingCreate()
  const result = await getSecondaryDataModel(data.type, data.group, data.name)
  $loadingClose()
  if (!(result.data && result.data.indexRule)) {
    ElMessage({
      message: `Please refresh and try again.`,
      type: "error",
      duration: 3000
    })
    return;
  }
  data.indexRule = {...result.data.indexRule, noSort: String(result.data.indexRule.noSort)}
}
</script>

<template>
  <div>
    <el-card>
      <template #header>
        <FormHeader :fields="data" />
      </template>
      <el-form label-position="left" label-width="100px" :model="data.indexRule" style="width: 90%;">
        <el-form-item label="Analyzer">
          <el-input v-model="data.indexRule.analyzer" :disabled="true"></el-input>
        </el-form-item>
        <el-form-item label="Tags">
          <el-select class="tags-and-rules" v-model="data.indexRule.tags" style="width: 100%;" :disabled="true" multiple></el-select>
        </el-form-item>
        <el-form-item label="Type">
          <el-input v-model="data.indexRule.type" disabled></el-input>
        </el-form-item>
        <el-form-item label="No Sort">
          <el-input v-model="data.indexRule.noSort" disabled></el-input>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
:deep(.el-card) {
    margin: 15px;
}
</style>