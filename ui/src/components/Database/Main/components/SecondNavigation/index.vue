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
import { getCurrentInstance } from "@vue/runtime-core"

const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
defineProps({
    showDrawer: {
        type: Boolean,
        default: false
    }
})
const emit = defineEmits(['openDetail'])

// data
let options = []
let tagFamily = 0
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
    options = opt
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
    $bus.emit('changeTagFamilies', tagFamily)
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
    <div class="flex second-nav-contain align-item-center justify-around">
        <el-select v-model="tagFamily" @change="changeTagFamilies" filterable placeholder="Please select">
            <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value">
            </el-option>
        </el-select>
        <el-date-picker class="date-picker" v-model="value" type="datetimerange" :picker-options="pickerOptions"
            range-separator="to" start-placeholder="begin" end-placeholder="end" align="right">
        </el-date-picker>
        <el-input class="search-input" placeholder="Search by Tags" clearable v-model="query">
            <el-button slot="append" icon="el-icon-search"></el-button>
        </el-input>
        <el-button class="nav-button" @click="refresh" icon="el-icon-refresh-right">Refresh</el-button>
        <el-button class="nav-button" @click="openDetail">{{ showDrawer ? "Close Detail" : "Open Detail" }}</el-button>
        <el-button class="nav-button" @click="openDesign">Open Design</el-button>
    </div>
</template>

<style lang="scss" scoped>
.nav-button {
    width: 8%;
    margin: 0;
}

.second-nav-contain {
    width: 100%;
    height: 50px;

    .date-picker {
        width: 28%;
        margin: 0;
    }

    .search-input {
        width: 28%;
        margin: 0;
    }

    .refresh {
        width: 5.5%;
        height: 80%;
    }
}

.picker {
    width: 95%;
    margin-left: 10px;
}
</style>