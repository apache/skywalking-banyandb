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
import { ref, reactive } from "vue";
import { computed } from '@vue/runtime-core'
import { ElImage, ElTooltip, ElMenu, ElMenuItem } from 'element-plus'
import stores from '../../stores/index'
const { aside, header, tags } = stores()

const currentMenu = computed(() => {
    return tags.currentMenu
})
const isCollapse = computed(() => {
    return aside.isCollapse
})
const showButton = computed(() => {
    return header.showButton
})
const data = reactive({
    activeMenu: '/dashboard'
})

const userImg = ref('src/assets/banyandb_small.jpg')
const handleSelect = (e) => {
    sessionStorage.setItem('active', e)
    data.activeMenu = e
    e === '/stream' || e === '/measure'
        ? header.changeShowButton(true)
        : header.changeShowButton(false)
}
const changeAsideWidth = () => {
    if (isCollapse.value) {
        aside.changeCollapse(false)
        aside.changeFatherWidth('200px')
    } else {
        aside.changeCollapse(true)
        aside.changeFatherWidth('65px')
    }
}
defineProps({
    active: {
        type: String,
        default: '/dashboard'
    }
})
function initData() {
    data.activeMenu = sessionStorage.getItem('active')
    data.activeMenu === '/stream' || data.activeMenu === '/measure'
        ? header.changeShowButton(true)
        : header.changeShowButton(false)
}
initData()
</script>

<template>
    <div class="flex align-item-center justify-between bd-bottom header">
        <div class="image flex align-item-center justify-between">
            <el-image :src="userImg" class="flex center" fit="fill">
                <div slot="error" class="image-slot">
                    <i class="el-icon-picture-outline"></i>
                </div>
            </el-image>
            <div class="title text-main-color text-title text-family text-weight-lt">BanyanDB Manager</div>
            <div class="flex center pointer icon-size" @click="changeAsideWidth" v-if="header.showButton">
                <el-tooltip class="item" effect="dark" :content="!aside.isCollapse ? 'Collapse menu' : 'Expand menu'"
                    placement="bottom">
                    <el-icon v-if="!aside.isCollapse" class="icon">
                        <Fold />
                    </el-icon>
                    <el-icon class="icon" v-else>
                        <Expand />
                    </el-icon>
                </el-tooltip>
            </div>
            <div v-else class="icon-size"></div>
            <span v-if="header.showButton && tags.currentMenu && data.activeMenu == '/stream'"
                :title="tags.currentMenu.metadata.group + ' / ' + tags.currentMenu.metadata.type + ' / ' + tags.currentMenu.metadata.name"
                class="text-overflow-hidden text-general-color pointer margin-left-small" style="width:380px;">{{
                        tags.currentMenu.metadata.group + ' / ' + tags.currentMenu.metadata.name
                }}</span>
            <div v-else style="width:380px;" class="margin-left-small"></div>
        </div>
        <div class="navigation" style="margin-right: 20%">
            <el-menu active-text-color="#6E38F7" router :ellipsis="false" class="el-menu-demo" mode="horizontal"
                :default-active="data.activeMenu" @select="handleSelect">
                <el-menu-item index="/dashboard">Dashboard</el-menu-item>
                <el-menu-item index="/stream">Stream</el-menu-item>
                <el-menu-item index="/measure">Measure</el-menu-item>
                <el-menu-item index="/property">Property</el-menu-item>
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
.header {
    width: 100%;
    height: 100%
}

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