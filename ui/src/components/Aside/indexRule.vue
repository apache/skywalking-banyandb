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
import { reactive } from 'vue'
import { watch } from "@vue/runtime-core"

const data = reactive({
  indexRule: [],
  index: ''
})
const props = defineProps({
  indexRule: {
    type: Array,
    required: true,
    default: () => {
      return []
    }
  },
  index: {
    type: String,
    required: true
  }
})
watch(() => props.indexRule, () => {
  console.log('watch', props.indexRule)
  data.indexRule = props.indexRule
})
watch(() => props.index, () => {
  data.index = props.index
})

console.log('indexRule', props.indexRule)
</script>

<template>
  <el-sub-menu :index="data.index">
    <template #title>
      <el-icon>
        <Folder />
      </el-icon>
      <span slot="title" title="Index-Rule" style="width: 70%" class="text-overflow-hidden">
        Index-Rule
      </span>
    </template>
    <div v-if="(item, index) in data.indexRule" :key="item.metadata.name">
      <el-menu-item :index="`${item.metadata.group}-${item.metadata.name}`">
        <template #title>
          <el-icon>
            <Document />
          </el-icon>
          <span slot="title" :title="item.metadata.name" style="width: 90%" class="text-overflow-hidden">
            {{ item.metadata.name }}
          </span>
        </template>
      </el-menu-item>
    </div>
  </el-sub-menu>
</template>

<style lang="scss" scoped></style>