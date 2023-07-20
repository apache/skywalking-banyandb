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
import { reactive } from "vue";
import { ElImage, ElTooltip, ElMenu, ElMenuItem } from 'element-plus'
import { useRoute } from 'vue-router'
import { watch, getCurrentInstance } from '@vue/runtime-core'
import userImg from '@/assets/banyandb_small.jpg'

// Eventbus
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

// router
const route = useRoute()

// data
const data = reactive({
    activeMenu: '/banyandb/dashboard',
    isCollapse: false,
    showButton: false
})

// watch
watch(() => route, () => {
    let arr = route.path.split('/')
    data.activeMenu = `/${arr[1]}/${arr[2]}`
}, {
    immediate: true,
    deep: true
})
watch(() => data.activeMenu, () => {
    data.showButton = data.activeMenu == '/banyandb/stream' || data.activeMenu == '/banyandb/measure' ? true : false
}, {
    immediate: true,
    deep: true
})

// function
function initData() {
    let arr = route.path.split('/')
    data.activeMenu = `/${arr[1]}/${arr[2]}`
}
function changeAsideWidth() {
    if (data.isCollapse) {
        $bus.emit('changeIsCollapse', {
            isCollapse: false,
            width: '200px'
        })
    } else {
        $bus.emit('changeIsCollapse', {
            isCollapse: true,
            width: '65px'
        })
    }
}
$bus.on('changeIsCollapse', (obj) => {
    data.isCollapse = obj.isCollapse
})
initData()
</script>

<template>
    <div class="size flex align-item-center justify-between bd-bottom">
        <div class="image flex align-item-center justify-between">
            <el-image :src="userImg" class="flex center" fit="fill">
                <div slot="error" class="image-slot">
                    <i class="el-icon-picture-outline"></i>
                </div>
            </el-image>
            <div class="title text-main-color text-title text-family text-weight-lt">BanyanDB Manager</div>
            <!-- open aside menu -->
            <div class="flex center pointer icon-size" v-if="data.showButton" @click="changeAsideWidth">
                <el-tooltip class="item" effect="dark" :content="!data.isCollapse ? 'Collapse menu' : 'Expand menu'"
                    placement="bottom">
                    <el-icon v-if="!data.isCollapse" class="icon">
                        <Fold />
                    </el-icon>
                    <el-icon class="icon" v-else>
                        <Expand />
                    </el-icon>
                </el-tooltip>
            </div>
            <div v-else class="icon-size"></div>
            <!-- stream/measure sources url -->
            <div style="width:380px;" class="margin-left-small"></div>
        </div>
        <div class="navigation" style="margin-right: 20%">
            <el-menu active-text-color="#6E38F7" router :ellipsis="false" class="el-menu-demo" mode="horizontal"
                :default-active="data.activeMenu" @select="handleSelect">
                <el-menu-item index="/banyandb/dashboard">Dashboard</el-menu-item>
                <el-menu-item index="/banyandb/stream">Stream</el-menu-item>
                <el-menu-item index="/banyandb/measure">Measure</el-menu-item>
                <el-menu-item index="/banyandb/property">Property</el-menu-item>
            </el-menu>
        </div>
        <div class="person flex justify-around align-item-center">
            <el-icon class="icon pointer">
                <Message />
            </el-icon>
            <el-icon class="icon pointer">
                <Avatar />
            </el-icon>
            <div class="text-normal text-main-color text-family text-weight-lt text-title">Admin</div>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.image {
    width: 665px;
    height: 100%;

    .el-image {
        width: 59px;
        height: 59px;
    }

    .title {
        height: 100%;
        line-height: 59px;
    }
}

.el-menu-item {
    font-weight: var(--weight-lt);
    font-size: var(--size-lt);
    font-family: var(--font-family-main);
}

.el-menu-item:hover {
    color: var(--el-menu-active-color) !important;
}

.navigation {
    display: flex;
    align-items: center;
    justify-content: center;
}

.person {
    width: 140px;
    height: 100%;
    margin-right: 30px;
}

.icon-size {
    width: 25px;
    height: 25px;
}
</style>