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
    <div style="width:100%; height:100%">
        <el-menu :default-active="currentMenu ? currentMenu.metadata.group + currentMenu.metadata.name : ''"
            active-text-color="#6E38F7" style="height: 100%;" :collapse="isCollapse" :collapse-transition="false">
            <div v-for="(item, index) in groupLists" :key="item.metadata.name"
                @contextmenu.prevent="rightClickGroup($event, index)">
                <el-submenu :index="item.metadata.name + '-' + index" :disabled="item.catalog == 'CATALOG_MEASURE'">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title" :title="item.metadata.name" style="width: 70%"
                            class="text-overflow-hidden">{{
                            item.metadata.name
                            }}</span>
                    </template>
                    <div v-for="(itemChildren, indexChildren) in item.children" :key="itemChildren.metadata.name">
                        <div @contextmenu.prevent="rightClick($event, index, indexChildren)">
                            <el-menu-item :index="itemChildren.metadata.group + itemChildren.metadata.name"
                                @click="openFile(index, indexChildren)">
                                <template slot="title">
                                    <i class="el-icon-document"></i>
                                    <span slot="title" :title="itemChildren.metadata.name" style="width: 90%"
                                        class="text-overflow-hidden">{{ itemChildren.metadata.name }}</span>
                                </template>
                            </el-menu-item>
                        </div>
                    </div>
                </el-submenu>
            </div>
        </el-menu>
        <div v-if="showRightMenu" class="right-menu border-radius-little box-shadow"
            :style="{ top: topNumber + 'px', left: leftNumber + 'px' }">
            <right-menu-component :rightMenuList="rightMenuList"></right-menu-component>
        </div>
    </div>
</template>
 
<script>
import { mapState } from 'vuex'
import { getGroupList, getStreamOrMeasureList } from '@/api/index'
import RightMenuComponent from './RightMenuComponent.vue'
export default {
    name: 'AsideComponent',
    data() {
        return {
            groupLists: [],
            topNumber: 0,
            leftNumber: 0,
            rightMenuListOne: [{
                icon: "el-icon-document",
                name: "new File",
                id: "create File"
            }, {
                icon: "el-icon-refresh-right",
                name: "refresh",
                id: "refresh Folder"
            }, {
                icon: "el-icon-delete",
                name: "delete",
                id: "delete Folder"
            }],
            rightMenuList: this.rightMenuListOne,
            rightMenuListTwo: [{
                icon: "el-icon-folder",
                name: "new Group",
                id: "create Group"
            }, {
                icon: "el-icon-document",
                name: "new File",
                id: "create File"
            }, {
                icon: "el-icon-refresh-right",
                name: "refresh",
                id: "refresh Group"
            }, {
                icon: "el-icon-delete",
                name: "delete",
                id: "delete Group"
            }],
            rightMenuListThree: [{
                icon: "el-icon-refresh-right",
                name: "refresh",
                id: "refresh File"
            }, {
                icon: "el-icon-delete",
                name: "delete",
                id: "delete File"
            }]
        }
    },

    components: {
        RightMenuComponent
    },

    created() {
        this.getGroupLists()
    },

    computed: {
        ...mapState({
            isCollapse: (state) => state.aside.isCollapse,
            showRightMenu: (state) => state.menuState.showRightMenu,
            currentMenu: (state) => state.tags.currentMenu
        })
    },

    methods: {
        getGroupLists() {
            this.$loading.create()
            getGroupList()
                .then(res => {
                    if (res.status == 200) {
                        let group = res.data.group
                        let length = group.length
                        this.groupLists = group
                        group.forEach((item, index) => {
                            let catalog = item.catalog
                            let type = catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
                            let name = item.metadata.name
                            getStreamOrMeasureList(type, name)
                                .then(res => {
                                    if (res.status == 200) {
                                        item.children = res.data[type]
                                    }
                                })
                                .finally(() => {
                                    if(length - 1 == index) {
                                        this.$loading.close()
                                    }
                                    this.$forceUpdate()
                                })
                        })
                    }
                })
        },
        stopPropagation(e) {
            e = e || window.event;
            if (e.stopPropagation) { //W3C阻止冒泡方法  
                e.stopPropagation();
            } else {
                e.cancelBubble = true; //IE阻止冒泡方法  
            }
        },
        openRightMenu(e) {
            this.$store.commit("changeShowRightMenu", true)
            this.topNumber = e.pageY
            this.leftNumber = e.pageX
            this.stopPropagation()
        },
        // open file right menu
        rightClick(e, index, indexMeasure) {
            this.rightMenuList = this.rightMenuListThree
            this.openRightMenu(e)
            console.log('rightClick')
        },
        // open folder right menu
        /*rightClickFolder(e, index, type) {
            this.rightMenuList = this.rightMenuListOne
            this.openRightMenu(e)
            console.log('rightClickFolder')
        },*/
        // open group right menu
        rightClickGroup(e, index) {
            this.rightMenuList = this.rightMenuListTwo
            this.openRightMenu(e)
            console.log('rightClickGroup')
        },
        openFile(index, indexChildren) {
            let item = this.groupLists[index].children[indexChildren]
            /**
             * Todo
             * Measure or Stream?
             */
            if (this.groupLists[index].catalog == "CATALOG_MEASURE") {
                item.metadata.type = "measure"
            } else {
                item.metadata.type = "stream"
            }
            this.$store.commit('selectMenu', item)
        }
    },
}
</script>

<style lang="scss" scoped>
.el-menu {
    width: 100%;
    border-right: none;
    text-align: start;
    text-justify: middle;
}

.right-menu {
    width: 130px;
    position: fixed;
    z-index: 9999999999999999999999999999 !important;
    background-color: white;
}

i {
    font-size: 25px;
    color: var(--color-main);
}
</style>