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
        <div class="flex-grow">
            <tag-navigation-component></tag-navigation-component>
            <el-card>
                <top-navigation-component @handleNavigation="handleNavigation"></top-navigation-component>
                <second-navigation-component></second-navigation-component>
            </el-card>
        </div>
        <div class="bd-top drawer-right" :style="showDrawer" style="width: 400px; height: 100%; background-color: white;">

        </div>
    </div>
</template>

<script>
import TopNavigationComponent from './mainComponents/TopNavigationComponent.vue'
import SecondNavigationComponent from './mainComponents/SecondNavigationComponent.vue'
import TagNavigationComponent from './TagNavigationComponent.vue'
export default {
    name: 'MainComponent',
    data() {
        return {
            showDrawer: "display: none"
        }
    },
    components: {
        TopNavigationComponent,
        SecondNavigationComponent,
        TagNavigationComponent
    },
    methods: {
        handleNavigation(item, index) {
            console.log(item)
            if (item.name == "Detail") {
                this.showDrawer = "displat: block"
            }
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
//     -webkit-animation: rtl-drawer-in .3s 1ms;
    //     animation: rtl-drawer-in .3s 1ms; el-drawer__open .el-drawer.rtl
    //     -webkit-animation: rtl-drawer-out .3s;
    //     animation: rtl-drawer-out .3s; el-drawer.rtl
.drawer-right {
-webkit-animation: rtl-drawer-in .3s 1ms;
animation: rtl-drawer-in .3s 1ms;
}
</style>