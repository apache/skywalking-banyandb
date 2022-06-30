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

<template>
    <div class="flex" style="height:100%; width:100%;">
        <div>
            <tag-navigation-component></tag-navigation-component>
            <el-card :style="showDrawer ? 'width: 1330px' : 'width:1680px'">
                <top-navigation-component @handleNavigation="handleNavigation"></top-navigation-component>
                <second-navigation-component style="margin: 10px 0 10px 0;"></second-navigation-component>
                <data-table-component style="margin: 10px; width:100%;"></data-table-component>
            </el-card>
        </div>
        <div class="bd-top bd-left drawer-right" v-if="showDrawer">
            <drawer-right-component @closeDetail="closeDetail"></drawer-right-component>
        </div>
    </div>
</template>

<script>
import TopNavigationComponent from './mainComponents/TopNavigationComponent.vue'
import SecondNavigationComponent from './mainComponents/SecondNavigationComponent.vue'
import TagNavigationComponent from './TagNavigationComponent.vue'
import DrawerRightComponent from './mainComponents/DrawerRightComponent.vue'
import DataTableComponent from './mainComponents/DataTableComponent.vue'
export default {
    name: 'MainComponent',
    data() {
        return {
            showDrawer: false,
            navWidth: "0px"
        }
    },
    components: {
        TopNavigationComponent,
        SecondNavigationComponent,
        TagNavigationComponent,
        DrawerRightComponent,
        DataTableComponent
    },
    methods: {
        handleNavigation(item, index) {
            if (item.name == "Detail") {
                this.showDrawer = !this.showDrawer
            }
        },
        closeDetail() {
            this.showDrawer = false
        }
    },
}
</script>

<style lang="scss">
.el-card {
    margin: 20px;
    padding: 0;
}

.el-card__body {
    padding: 0;
}

.drawer-right {
    -webkit-animation: rtl-drawer-in .3s 1ms;
    animation: rtl-drawer-in .3s 1ms;
    width: 350px;
    height: 100%;
    background-color: var(--color-background);
}
</style>