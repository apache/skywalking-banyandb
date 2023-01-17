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
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { reactive } from 'vue';
import { useRoute } from 'vue-router'
const route = useRoute()

const data = reactive({
    group: route.params.group,
    name: route.params.group,
    type: route.params.type,
    operator: route.params.operator
})

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

}
</script>

<template>
    <div>
        <el-card shadow="always">
            <template #header>
                <el-row>
                    <el-col :span="12">
                        <div class="flex align-item-center" style="height: 30px; width: 100%;">
                            <div class="flex" style="height: 30px;">
                                <span class="text-bold">Type：</span>
                                <span style="margin-right: 20px;">{{ data.type }}</span>
                                <span class="text-bold">Group：</span>
                                <span style="margin-right: 20px;">{{ data.group }}</span>
                                <span class="text-bold" v-if="data.name">Name：</span>
                                <span style="margin-right: 20px;" v-if="data.name">{{ data.name }}</span>
                                <span class="text-bold">Operator：</span>
                                <span>{{ data.operator }}</span>
                            </div>
                        </div>
                    </el-col>
                    <el-col :span="12">
                        <div class="flex align-item-center justify-end" style="height: 30px;">
                            <el-button size="small" type="primary" color="#6E38F7" >submit</el-button>
                        </div>
                    </el-col>
                </el-row>

            </template>

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