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
defineProps({
    tableData: {
        type: Array,
        required: true
    }
})
let options = ['tags', 'type']
function handleSelectionChange() { }
</script>

<template>
    <div>
        <el-table ref="multipleTable" height=395 stripe :data="tableData" highlight-current-row tooltip-effect="dark"
            @selection-change="handleSelectionChange" border empty-text="No data yet">
            <el-table-column type="index" label="number" width="90">
            </el-table-column>
            <el-table-column v-for="item in options" :key="item" :prop="item" :label="item" show-overflow-tooltip>
            </el-table-column>
        </el-table>
    </div>
</template>