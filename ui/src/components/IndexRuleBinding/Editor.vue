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

<script lang="ts" setup>
import { reactive, ref } from 'vue'
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { useRoute, useRouter } from 'vue-router'
import type { FormInstance } from 'element-plus'
import { createIndexRuleOrIndexRuleBinding, getIndexRuleOrIndexRuleBinding, updatendexRuleOrIndexRuleBinding } from '@/api/index'
import { ElMessage } from 'element-plus'
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
const ruleFormRef = ref<FormInstance>()

const route = useRoute()
const router = useRouter()
const rules = {
  beginAt: [
    {
      required: true, message: 'Please enter the beginAt', trigger: 'blur'
    }
  ],
  expireAt: [
    {
      required: true, message: 'Please enter the expireAt', trigger: 'blur'
    }
  ],
  rules: [
    {
      required: true, message: 'Please select the rules', trigger: 'blur'
    }
  ]
}
const data = reactive({
  group: route.params.group,
  name: route.params.name,
  type: route.params.type,
  operator: route.params.operator,
  form: {
    group: route.params.group,
    name: route.params.name || '',
    beginAt: '',
    expireAt: '',
    rules: []
  }
})


watch(() => route, () => {
  data.form = {
    group: route.params.group,
    name: route.params.name || '',
    beginAt: '',
    expireAt: '',
    rules: []
  }
  data.group = route.params.group
  data.name = route.params.name
  data.type = route.params.type
  data.operator = route.params.operator
  initData()
}, {
  immediate: true,
  deep: true
})
const submit = async (formEl: FormInstance | undefined) => {
  if (!formEl) return
  await formEl.validate((valid) => {
    if (valid) {
      const param = {
        indexRuleBinding: {
          metadata: {
            group: data.form.group,
            name: data.form.name
          },
          subject: {
            catalog: "CATALOG_STREAM",
            name: data.form.name
          },
          beginAt: data.form.beginAt,
          expireAt: data.form.expireAt,
          rules: data.form.rules
        }
      }
      $loadingCreate()
      if (data.operator == 'create') {
        return createIndexRuleOrIndexRuleBinding("index-rule-binding", param)
          .then(res => {
            if (res.status == 200) {
              ElMessage({
                message: 'Create successed',
                type: "success",
                duration: 5000
              })
              $bus.emit('refreshAside')
              $bus.emit('deleteGroup', data.form.group)
              openIndexRuleBinding()
            }
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
      } else {
        return updatendexRuleOrIndexRuleBinding("index-rule-binding", data.form.group, data.form.name, param)
          .then(res => {
            if (res.status == 200) {
              ElMessage({
                message: 'Edit successed',
                type: "success",
                duration: 5000
              })
              $bus.emit('refreshAside')
              $bus.emit('deleteResource', data.form.group)
              openIndexRuleBinding()
            }
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
  })
}

function openIndexRuleBinding() {
  const route = {
    name: data.type + '',
    params: {
      group: data.form.group,
      name: data.form.name,
      operator: 'read',
      type: data.type + ''
    }
  }
  router.push(route)
  const add = {
    label: data.form.name,
    type: `Read-${data.type}`,
    route
  }
  $bus.emit('changeAside', data.form)
  $bus.emit('AddTabs', add)
}

function initData() {
  if (data.operator == 'edit' && data.form.group && data.form.name) {
    $loadingCreate()
    getIndexRuleOrIndexRuleBinding("index-rule-binding", data.form.group, data.form.name)
      .then(res => {
        if (res.status == 200) {
          const indexRuleBinding = res.data.indexRuleBinding
          data.form = {
            group: indexRuleBinding.metadata.group,
            name: indexRuleBinding.metadata.name,
            beginAt: indexRuleBinding.beginAt,
            expireAt: indexRuleBinding.expireAt,
            rules: indexRuleBinding.rules
          }
        }
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
        <el-row>
          <el-col :span="12">
            <div class="flex align-item-center" style="height: 30px; width: 100%;">
              <div class="flex" style="height: 30px;">
                <span class="text-bold">Group：</span>
                <span style="margin-right: 20px;">{{ data.group }}</span>
                <span class="text-bold" v-if="data.operator == 'edit'">Name：</span>
                <span style="margin-right: 20px;">{{ data.name }}</span>
                <span class="text-bold">Type：</span>
                <span style="margin-right: 20px;">{{ data.type }}</span>
                <span class="text-bold">Operation：</span>
                <span>{{ data.operator }}</span>
              </div>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="flex align-item-center justify-end" style="height: 30px;">
              <el-button size="small" type="primary" @click="submit(ruleFormRef)" color="#6E38F7">submit</el-button>
            </div>
          </el-col>
        </el-row>
      </template>
      <el-form ref="ruleFormRef" :rules="rules" label-position="left" label-width="100px" :model="data.form"
        style="width: 50%;">
        <el-form-item label="Group" prop="group">
          <el-input v-model="data.form.group" clearable :disabled="true"></el-input>
        </el-form-item>
        <el-form-item label="Name" prop="name">
          <el-input v-model="data.form.name" :disabled="data.operator == 'edit'" clearable
            placeholder="Input Name"></el-input>
        </el-form-item>
        <el-form-item label="Begin At" prop="beginAt">
          <el-date-picker v-model="data.form.beginAt" placeholder="Choose Begin At" style="width: 100%;" type="datetime"
            clearable></el-date-picker>
        </el-form-item>
        <el-form-item label="Expire At" prop="expireAt">
          <el-date-picker v-model="data.form.expireAt" placeholder="Choose Expire" style="width: 100%;" type="datetime"
            clearable></el-date-picker>
        </el-form-item>
        <el-form-item label="Rules" prop="rules">
          <el-select v-model="data.form.rules" allow-create filterable default-first-option placeholder="Input Rules"
            style="width: 100%;" clearable multiple></el-select>
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