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
import SecondNavigationComponent from './components/SecondNavigation/index.vue'
import TagNavigationComponent from './components/TagNavigation/index.vue'
import DrawerRightComponent from './components/DrawerRight/index.vue'
import DataTableComponent from './components/DataTable/index.vue'
import { reactive } from '@vue/reactivity'

// data
let data = reactive({
    showDrawer: false,
    fileData: null
})
let navWidth = '0px'

// methods
function openDetail() {
    data.showDrawer = !data.showDrawer
}
function drawerRight(dataList) {
    data.fileData = dataList
}
function closeDetail() {
    data.showDrawer = false
}

</script>

<template>
    <div class="flex" style="height:100%; width:100%;">
        <div :style="data.showDrawer ? 'width: calc(80% - 2px)' : 'width: calc(100% - 2px)'" style="height: 100%;">
            <el-card style="max-height: 97.5%;">
                <div style="width: 100%; height: 40px;">
                    <tag-navigation-component></tag-navigation-component>
                </div>
                <!--top-navigation-component @handleNavigation="handleNavigation"></top-navigation-component-->
                <second-navigation-component @openDetail="openDetail" :showDrawer="data.showDrawer" style="width: 100%">
                </second-navigation-component>
                <data-table-component @drawerRight="drawerRight" class="margin-all-little"></data-table-component>
            </el-card>
        </div>
        <div class="bd-top bd-left drawer-right" v-if="data.showDrawer">
            <drawer-right-component :fileData="data.fileData" @closeDetail="closeDetail"></drawer-right-component>
        </div>
    </div>
</template>

<style lang="scss">
.el-card {
    margin: 10px;
    padding: 0;
    height: 100%;
}

.el-card__body {
    padding: 0;
}

.drawer-right {
    -webkit-animation: rtl-drawer-in .3s 1ms;
    animation: rtl-drawer-in .3s 1ms;
    width: 20%;
    height: calc(100% - 5px);
    background-color: var(--color-background);
}
</style>