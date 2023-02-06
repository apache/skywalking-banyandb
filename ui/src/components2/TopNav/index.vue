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
import { useRouter, useRoute } from 'vue-router';
import { getCurrentInstance } from '@vue/runtime-core'
import { ElStep } from 'element-plus';

const router = useRouter()
const route = useRoute()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

const data = reactive({
    activeTabIndex: 0,
    tabsList: []
})
function tabClick() {
    router.push(data.tabsList[data.activeTabIndex].route)
    const group = data.tabsList[data.activeTabIndex].route.params.group
    const name = data.tabsList[data.activeTabIndex].route.params.name

    if (group && name) {
        $bus.emit('changeAside', { group, name })
    }
}
function removeTab(index) {
    const len = data.tabsList.length
    const activeTabIndex = data.activeTabIndex
    if (len == 1) {
        data.tabsList = []
        $bus.emit('resetAside')
        return
    }
    data.tabsList.splice(index, 1)
    if (activeTabIndex != 0) {
        data.activeTabIndex--
    }
    router.push(data.tabsList[data.activeTabIndex].route)
}
function initData() {
    const group = route.params.group
    const name = route.params.name
    const operator = route.params.operator
    const type = route.params.type
    if (!operator || !group || !type) {
        return
    }
    const routeData = {
        params: route.params
    }
    const add = { route: routeData }
    if (operator == 'read') {
        routeData.name = type
        add.label = name
        add.type = 'Read'
    } else if (operator == 'edit') {
        routeData.name = `edit-${type}`
        add.label = name
        add.type = 'Edit'
    } else {
        routeData.name = `create-${type}`
        add.label = group
        add.type = 'Create'
    }
    data.tabsList.push(add)
}
$bus.on('AddTabs', (tab) => {
    const index = data.tabsList.findIndex(item => {
        return item.label == tab.label && item.type == tab.type
    })
    if (index >= 0) {
        return data.activeTabIndex = index
    }
    data.tabsList.push(tab)
    data.activeTabIndex = data.tabsList.length - 1
})
$bus.on('deleteGroup', (group) => {
    for (let i = 0; i < data.tabsList.length; i++) {
        if (data.tabsList[i].route.params.group && data.tabsList[i].route.params.group == group) {
            removeTab(i)
        }
    }
})
$bus.on('deleteResource', (name) => {
    for (let i = 0; i < data.tabsList.length; i++) {
        if (data.tabsList[i].route.params.name && data.tabsList[i].route.params.name == name) {
            removeTab(i)
        }
    }
})
initData()
</script>

<template>
    <div class="topNav-box bd-top" v-if="data.tabsList.length > 0">
        <el-tabs type="card" v-model="data.activeTabIndex" closable class="demo-tabs" @tab-change="tabClick"
            @tab-remove="removeTab">
            <el-tab-pane v-for="item, index in data.tabsList" :key="`${item.type}-${item.label}`"
                :label="`${item.type}-${item.label}`" :name="index"></el-tab-pane>
        </el-tabs>
    </div>
</template>

<style>
.demo-tabs>.el-tabs__content {
    padding: 32px;
    color: #6b778c;
    font-size: 32px;
    font-weight: 600;
}
</style>
<style lang="scss" scoped>
.topNav-box {
    width: 100%;
    height: 40px;
    background-color: var(--color-white);
    box-shadow: 0 0 10px 0 var(--color-placeholder-font);
}

::v-deep {
    .el-tabs--card>.el-tabs__header .el-tabs__nav {
        border-top: none !important;
        padding: 0 !important;
    }

    .el-tabs__header.is-top {
        padding: 0 !important;
        margin: 0 !important;
    }

    .el-tabs__item.is-active {
        color: var(--color-main) !important;
        background-color: var(--color-main-background) !important;
        border: 1px solid var(--color-main) !important;
        border-bottom: none !important;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }

    .el-tabs__content {
        padding: 0 !important;
        margin: 0 !important;
        height: 0 !important;
    }
}
</style>