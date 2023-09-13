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
  group: [
    {
      required: true, message: 'Please enter the group', trigger: 'blur'
    }
  ],
  name: [
    {
      required: true, message: 'Please enter the name', trigger: 'blur'
    }
  ],
  analyzer: [
    {
      required: true, message: 'Please select the analyzer', trigger: 'blur'
    }
  ],
  location: [
    {
      required: true, message: 'Please select the location', trigger: 'blur'
    }
  ],
  tags: [
    {
      required: true, message: 'Please select the tags', trigger: 'blur'
    }
  ],
  type: [
    {
      required: true, message: 'Please select the type', trigger: 'blur'
    }
  ]
}
const typeList = [
  {
    label: "TYPE_UNSPECIFIED",
    value: "TYPE_UNSPECIFIED"
  },
  {
    label: "TYPE_TREE",
    value: "TYPE_TREE"
  },
  {
    label: "TYPE_INVERTED",
    value: "TYPE_INVERTED"
  }
]
const locationList = [
  {
    label: "LOCATION_UNSPECIFIED",
    value: "LOCATION_UNSPECIFIED"
  },
  {
    label: "LOCATION_SERIES",
    value: "LOCATION_SERIES"
  },
  {
    label: "LOCATION_GLOBAL",
    value: "LOCATION_GLOBAL"
  }
]
const analyzerList = [
  {
    label: "ANALYZER_UNSPECIFIED",
    value: "ANALYZER_UNSPECIFIED"
  },
  {
    label: "ANALYZER_KEYWORD",
    value: "ANALYZER_KEYWORD"
  },
  {
    label: "ANALYZER_STANDARD",
    value: "ANALYZER_STANDARD"
  },
  {
    label: "ANALYZER_SIMPLE",
    value: "ANALYZER_SIMPLE"
  }
]
const data = reactive({
  group: route.params.group,
  name: route.params.name,
  type: route.params.type,
  operator: route.params.operator,
  form: {
    group: route.params.group,
    name: route.params.name || '',
    analyzer: '',
    location: '',
    tags: [],
    type: ''
  }
})


watch(() => route, () => {
  data.form = {
    group: route.params.group,
    name: route.params.name || '',
    analyzer: '',
    location: '',
    tags: [],
    type: ''
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
        indexRule: {
          metadata: {
            group: data.form.group,
            name: data.form.name
          },
          tags: data.form.tags,
          type: data.form.type,
          location: data.form.location,
          analyzer: data.form.analyzer
        }
      }
      $loadingCreate()
      if (data.operator == 'create') {
        return createIndexRuleOrIndexRuleBinding("index-rule", param)
          .then(res => {
            if (res.status == 200) {
              ElMessage({
                message: 'Create successed',
                type: "success",
                duration: 5000
              })
              $bus.emit('refreshAside')
              $bus.emit('deleteGroup', data.form.group)
              openIndexRule()
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
        return updatendexRuleOrIndexRuleBinding("index-rule", data.form.group, data.form.name, param)
          .then(res => {
            if (res.status == 200) {
              ElMessage({
                message: 'Edit successed',
                type: "success",
                duration: 5000
              })
              $bus.emit('refreshAside')
              $bus.emit('deleteResource', data.form.group)
              openIndexRule()
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

function openIndexRule() {
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
    getIndexRuleOrIndexRuleBinding("index-rule", data.form.group, data.form.name)
      .then(res => {
        if (res.status == 200) {
          const indexRule = res.data.indexRule
          data.form = {
            group: indexRule.metadata.group,
            name: indexRule.metadata.name,
            analyzer: indexRule.analyzer,
            location: indexRule.location,
            tags: indexRule.tags,
            type: indexRule.type
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
        <el-form-item label="Analyzer" prop="analyzer">
          <el-select v-model="data.form.analyzer" placeholder="Choose Analyzer" style="width: 100%;" clearable>
            <el-option v-for="item in analyzerList" :key="item.value" :label="item.label" :value="item.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="Location" prop="location">
          <el-select v-model="data.form.location" placeholder="Choose Location" style="width: 100%;" clearable>
            <el-option v-for="item in locationList" :key="item.value" :label="item.label" :value="item.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="Tags" prop="tags">
          <el-select v-model="data.form.tags" allow-create filterable default-first-option placeholder="Input Tags"
            style="width: 100%;" clearable multiple></el-select>
        </el-form-item>
        <el-form-item label="Type" prop="type">
          <el-select v-model="data.form.type" placeholder="Choose Type" style="width: 100%;" clearable>
            <el-option v-for="item in typeList" :key="item.value" :label="item.label" :value="item.value" />
          </el-select>
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