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
import { getCurrentInstance, reactive } from "@vue/runtime-core"
import { Search } from '@element-plus/icons-vue'
const { proxy } = getCurrentInstance()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
defineProps({
    showDrawer: {
        type: Boolean,
        default: false
    }
})
const emit = defineEmits(['openDetail'])

// data
let data = reactive({
    options: [],
    tagFamily: 0
})
let options = []
let refreshStyle = {
    fontColor: "var(--color-main-font)",
    color: "var(--color-main-font)",
    backgroundColor: "var(--color-white)"
}
let query = ''
let value = ''
let pickerOptions = {
    shortcuts: [{
        text: 'Last week',
        onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
            picker.$emit('pick', [start, end]);
        }
    }, {
        text: 'Last month',
        onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
            picker.$emit('pick', [start, end]);
        }
    }, {
        text: 'Last three months',
        onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 90);
            picker.$emit('pick', [start, end]);
        }
    }]
}

$bus.on('setOptions', (opt) => {
    data.options = opt
})

// methods
function andleOver() {
    refreshStyle.fontColor = "var(--color-main)"
    refreshStyle.color = "var(--color-main)"
    refreshStyle.backgroundColor = "var(--color-select)"
}
function handleLeave() {
    refreshStyle.fontColor = "var(--color-main-font)"
    refreshStyle.color = "var(--color-main-font)"
    refreshStyle.backgroundColor = "var(--color-white)"
}
function changeTagFamilies() {
    $bus.emit('changeTagFamilies', data.tagFamily)
}
function refresh() {
    $bus.emit('refresh')
}
function openDetail() {
    emit('openDetail')
}
function openDesign() { }
</script>

<template>
    <div class="flex second-nav-contain align-item-center justify-center">
        <div style="width: 130px;">
            <el-select v-model="data.tagFamily" @change="changeTagFamilies" filterable placeholder="Please select">
                <el-option v-for="item in data.options" :key="item.value" :label="item.label" :value="item.value">
                </el-option>
            </el-select>
        </div>

        <div class="flex justify-between align-item-center" style="width: calc(100% - 130px);">
            <div class="flex align-item-center" style="width: 50%;">
                <div class="flex justify-start align-item-center set-margin-left" style="width: 270px;">
                    <el-date-picker v-model="value" type="datetimerange" :shortcuts="pickerOptions.shortcuts"
                        range-separator="to" start-placeholder="begin" end-placeholder="end" align="right" disabled>
                    </el-date-picker>
                </div>
                <div class="flex justify-start align-item-center set-margin-left" style="width: 270px;">
                    <el-input class="search-input" placeholder="Search by Tags" clearable v-model="query" disabled>
                        <template #append>
                            <el-button :icon="Search" />
                        </template>
                    </el-input>
                </div>
            </div>
            <div class="flex align-item-center justify-end" style="width:50%;">
                <div>
                    <el-button class="nav-button" @click="refresh" disabled>Refresh</el-button>
                </div>
                <div class="set-margin-left">
                    <el-button class="nav-button" @click="openDesign" disabled>Open Design</el-button>
                </div>
                <div class="set-margin-left">
                    <el-button class="nav-button" type="primary" @click="openDetail">
                        {{ showDrawer ? "Close Detail" : "Open Detail" }}
                    </el-button>
                </div>
            </div>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.set-margin-left {
    margin-left: 10px;
}
.nav-button {
    width: 100%;
    margin: 0;
}

.second-nav-contain {
    width: calc(100% - 20px) !important;
    height: 30px;
    padding: 0 10px 0 10px;

    .search-input {
        width: 100%;
        margin: 0;
    }
}
</style>