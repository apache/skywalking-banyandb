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
        <el-dialog title="Tips" :visible.sync="dialogVisible" width="25%" center>
            <span>Are you sure to delete this {{rightClickType}}?</span>
            <span slot="footer" class="dialog-footer">
                <el-button @click="dialogVisible = false">cancel</el-button>
                <el-button type="primary" @click="deleteGroupOrFile">delete</el-button>
            </span>
        </el-dialog>
        <el-dialog width="25%" center :title="`${setGroup} group`" :visible.sync="dialogFormVisible">
            <el-form ref="ruleForm" :rules="rules" :model="groupForm" label-position="left">
                <el-form-item label="group name" label-width="100px" prop="name">
                    <el-input :disabled="setGroup == 'edit'" v-model="groupForm.name" autocomplete="off" style="width: 300px;"></el-input>
                </el-form-item>
                <el-form-item label="group type" label-width="100px" prop="catalog">
                    <el-select v-model="groupForm.catalog" style="width: 300px;" placeholder="please select">
                        <el-option label="CATALOG_STREAM" value="CATALOG_STREAM"></el-option>
                        <el-option label="CATALOG_MEASURE" value="CATALOG_MEASURE"></el-option>
                    </el-select>
                </el-form-item>
            </el-form>
            <div slot="footer" class="dialog-footer">
                <el-button @click="dialogFormVisible = false">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{setGroup}}
                </el-button>
            </div>
        </el-dialog>
    </div>
</template>
 
<script>
import { mapState } from 'vuex'
import { getGroupList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup } from '@/api/index'
import { Message } from "element-ui"
import RightMenuComponent from './RightMenuComponent.vue'
export default {
    name: 'AsideComponent',
    computed: {
        ...mapState({
            isCollapse: (state) => state.aside.isCollapse,
            // showRightMenu: (state) => state.menuState.showRightMenu,
            // closeMenu: (state) => state.menuState.closeMenu,
            tags: (state) => state.tags.tagsList,
            currentMenu: (state) => state.tags.currentMenu
        })
    },
    components: {
        RightMenuComponent
    },
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
            rightMenuListTwo: [{
                icon: "el-icon-folder",
                name: "new group",
                id: "create Group"
            }, {
                icon: "el-icon-folder",
                name: "edit group",
                id: "edit Group"
            }, {
                icon: "el-icon-document",
                name: "new file",
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
                icon: "el-icon-document",
                name: "edit file",
                id: "edit File"
            }, {
                icon: "el-icon-delete",
                name: "delete",
                id: "delete File"
            }],
            rightGroupIndex: 0,
            rightChildIndex: 0,
            rightClickType: 'group',
            dialogVisible: false,
            dialogFormVisible: false,
            setGroup: 'create',
            groupForm: {
                name: null,
                catalog: 'CATALOG_STREAM'
            },
            rules: {
                name: [
                    {
                        required: true, message: 'Please enter the name of the group', trigger: 'blur'
                    }
                ],
                catalog: [
                    {
                        required: true, message: 'Please select the type of the group', trigger: 'blur'
                    }
                ]
            }
        }
    },
    created() {
        this.getGroupLists()
        this.$bus.$on('handleRightItem', (index) => {
            this.handleRightItem(index)
        })
    },

    methods: {

        /**
         * get group data
         * @author wuchusheng
         */
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
                                    if (length - 1 == index) {
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
            this.$store.commit('changeLeft', e.pageX)
            this.$store.commit('changeTop', e.pageY)
            this.stopPropagation()
        },
        // open file right menu
        rightClick(e, index, indexChild) {
            this.$store.commit('changeRightMenuList', this.rightMenuListThree)
            this.rightClickType = 'file'
            this.rightGroupIndex = index
            this.rightChildIndex = indexChild
            this.openRightMenu(e)
        },
        rightClickGroup(e, index) {
            this.$store.commit('changeRightMenuList', this.rightMenuListTwo)
            this.rightClickType = 'group'
            this.rightGroupIndex = index
            this.openRightMenu(e)
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
        },

        /**
         * click right menu item
         * @author wuchusheng
         */
        handleRightItem(index) {
            console.log('groupLists', this.groupLists)
            if (this.rightClickType == 'group') {
                // right click group
                let rightName = this.rightMenuListTwo[index].name
                switch (rightName) {
                    case 'new group':
                        this.setGroup = 'create'
                        this.openCreateGroup()
                        break
                    case 'edit group':
                        this.setGroup = 'edit'
                        this.openEditGroup()
                        break
                    case 'refresh':
                        this.getGroupLists()
                        break
                    case 'delete':
                        this.openDeleteDialog()
                        break
                }
            } else {
                // right click measure or stream
                let rightName = this.rightMenuListThree[index].name
                switch (rightName) {
                    case 'delete':
                        this.openDeleteDialog()
                }
            }
            // close right menu
            this.$store.commit("changeShowRightMenu", false)
        },

        /**
         * click right menu delete file
         * @author wuchusheng
         */
        openDeleteDialog() {
            this.dialogVisible = true
        },
        deleteGroupOrFile() {
            let group = this.groupLists[this.rightGroupIndex].metadata.name
            let type = this.groupLists[this.rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
            if (this.rightClickType == 'group') {
                // delete group
                let children = this.groupLists[this.rightGroupIndex].children
                for (let i = 0; i < children.length; i++) {
                    let file = children[i]
                    let index = this.tags.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === file.metadata.name)
                    if (index != -1) {
                        Message({
                            message: 'There are files open in this group. Please close these files before proceeding',
                            type: "warning",
                            duration: 5000
                        })
                        this.dialogVisible = false
                        return
                    }
                }
                this.$loading.create()
                deleteGroup(group)
                    .then((res) => {
                        if (res.status == 200) {
                            if (res.data.deleted) {
                                Message({
                                    message: 'Delete succeeded',
                                    type: "success",
                                    duration: 5000
                                })
                                this.getGroupLists()
                            }
                        }
                    })
                    .finally(() => {
                        this.$loading.close()
                        this.dialogVisible = false
                    })
            } else {
                // delete measure or stream file
                let name = this.groupLists[this.rightGroupIndex].children[this.rightChildIndex].metadata.name
                let index = this.tags.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === name)
                if (index != -1) {
                    Message({
                        message: 'This file has been opened. Please close the file before proceeding!',
                        type: "warning",
                        duration: 5000
                    })
                    this.dialogVisible = false
                    return
                }
                this.$loading.create()
                deleteStreamOrMeasure(type, group, name)
                    .then((res) => {
                        if (res.status == 200) {
                            if (res.data.deleted) {
                                Message({
                                    message: 'Delete succeeded',
                                    type: "success",
                                    duration: 5000
                                })
                                this.getGroupLists()
                            }
                        }
                    })
                    .finally(() => {
                        this.$loading.close()
                        this.dialogVisible = false
                    })
            }
        },
        openCreateGroup() {
            this.dialogFormVisible = true
        },
        openEditGroup() {
            let name = this.groupLists[this.rightGroupIndex].metadata.name
            let catalog = this.groupLists[this.rightGroupIndex].catalog
            this.groupForm.name = name
            this.groupForm.catalog = catalog
            this.dialogFormVisible = true
        },
        confirmForm() {
            this.setGroup == 'create' ? this.createGroup() : this.editGroup()
        },
        createGroup() {
            this.$refs.ruleForm.validate((valid) => {
                if (valid) {
                    let data = {
                        group: {
                            metadata: {
                                group: "",
                                name: this.groupForm.name
                            },
                            catalog: this.groupForm.catalog
                        }
                    }
                    this.$loading.create()
                    createGroup(data)
                        .then((res) => {
                            if (res.status == 200) {
                                this.getGroupLists()
                                Message({
                                    message: 'Created successfully',
                                    type: "success",
                                    duration: 3000
                                })
                            }
                        })
                        .finally(() => {
                            this.dialogFormVisible = false
                            this.$loading.close()
                        })
                }
            })
        },
        editGroup() {
            let name = this.groupLists[this.rightGroupIndex].metadata.name
            this.$refs.ruleForm.validate((valid) => {
                if (valid) {
                    let data = {
                        group: {
                            metadata: {
                                group: "",
                                name: this.groupForm.name
                            },
                            catalog: this.groupForm.catalog
                        }
                    }
                    this.$loading.create()
                    editGroup(name, data)
                        .then((res) => {
                            if (res.status == 200) {
                                this.getGroupLists()
                                Message({
                                    message: 'Update succeeded',
                                    type: "success",
                                    duration: 3000
                                })
                            }
                        })
                        .finally(() => {
                            this.dialogFormVisible = false
                            this.$loading.close()
                        })
                }
            })
        },
        clearGroupForm() {
            this.groupForm = {
                name: null,
                catalog: 'CATALOG_STREAM'
            }
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