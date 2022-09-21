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
import stores from './stores/index'
import { getGroupList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup, createResources } from '@/api/index'
import { Message, ElMenu, ElSubMenu, ElMenuItem, ElButton } from "element-plus"
import RightMenuComponent from './components/RightMenu'
import DialogResourcesComponent from './components/DialogResources'
import { getCurrentInstance } from "@vue/runtime-core"

// eventBus
const $bus = getCurrentInstance().appContext.config.globalProperties.$bus
const { aside, tags } = stores()
// init data
const list1 = [{
    icon: "el-icon-folder",
    name: "new group",
    id: "create Group"
}, {
    icon: "el-icon-folder",
    name: "edit group",
    id: "edit Group"
}, {
    icon: "el-icon-document",
    name: "new resources",
    id: "create resources"
}, {
    icon: "el-icon-refresh-right",
    name: "refresh",
    id: "refresh Group"
}, {
    icon: "el-icon-delete",
    name: "delete",
    id: "delete Group"
}]
const list2 = [{
    icon: "el-icon-delete",
    name: "delete",
    id: "delete resources"
}]
const rule = {
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
// pinia data
const isCollapse = computed(() => {
    return aside.isCollapse
})
const tagsList = computed(() => {
    return tags.tagsList
})
const currentMenu = computed(() => {
    return tags.currentMenu
})

// data
let groupLists = []
let rightMenuListTwo = list1 // right click group menu
let rightMenuListThree = list2 // right click Resources menu
rightGroupIndex: 0 // right click group list index
let rightChildIndex = 0 // right click Resources list index
let rightClickType = 'group' // right click group or Resources
let dialogVisible = false // delete dialog
let dialogGroupVisible = false // group dialog
let dialogResourcesVisible = false // Resources dialog
let setGroup = 'create' // group dialog is create or edit
let operation = 'create' // Resources dialog is create or edit
let type = 'stream' // Resources dialog is stream or measure
let group = ''
let groupForm = { // group dialog form
    name: null,
    catalog: 'CATALOG_STREAM'
}
let rules = rule // group dialog form rules

// methods
const getGroupLists = () => {
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
}


$bus.$on('handleRightItem', (index) => {

})
</script>

<script>
import { mapState } from 'vuex'
import { getGroupList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup, createResources } from '@/api/index'
import { Message, ElMenu, ElSubMenu, ElMenuItem, ElButton } from "element-plus"
import RightMenuComponent from './components/RightMenu'
import DialogResourcesComponent from './components/DialogResources'
const list1 = [{
    icon: "el-icon-folder",
    name: "new group",
    id: "create Group"
}, {
    icon: "el-icon-folder",
    name: "edit group",
    id: "edit Group"
}, {
    icon: "el-icon-document",
    name: "new resources",
    id: "create resources"
}, {
    icon: "el-icon-refresh-right",
    name: "refresh",
    id: "refresh Group"
}, {
    icon: "el-icon-delete",
    name: "delete",
    id: "delete Group"
}]
const list2 = [{
    icon: "el-icon-delete",
    name: "delete",
    id: "delete resources"
}]
const rule = {
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
        RightMenuComponent,
        DialogResourcesComponent,
        ElMenu,
        ElSubMenu,
        ElMenuItem,
        ElButton
    },
    data() {
        return {
            groupLists: [],
            rightMenuListTwo: list1, // right click group menu
            rightMenuListThree: list2, // right click Resources menu
            rightGroupIndex: 0, // right click group list index
            rightChildIndex: 0, // right click Resources list index
            rightClickType: 'group', // right click group or Resources
            dialogVisible: false, // delete dialog
            dialogGroupVisible: false, // group dialog
            dialogResourcesVisible: false, // Resources dialog
            setGroup: 'create', // group dialog is create or edit
            operation: 'create', // Resources dialog is create or edit
            type: 'stream', // Resources dialog is stream or measure
            group: '',
            groupForm: { // group dialog form
                name: null,
                catalog: 'CATALOG_STREAM'
            },
            rules: rule, // group dialog form rules
        }
    },
    created() {
        // get group list
        this.getGroupLists()
        // monitor click right menu item
        this.$bus.$on('handleRightItem', (index) => {
            this.handleRightItem(index)
        })
    },

    methods: {

        /**
         * get group data
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
            if (e.stopPropagation) {
                e.stopPropagation();
            } else {
                e.cancelBubble = true;
            }
        },

        /**
         * open group or resources right menu
         */
        rightClick(e, index, indexChild) {
            this.$store.commit('changeRightMenuList', this.rightMenuListThree)
            this.rightClickType = 'resources'
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
        openRightMenu(e) {
            this.$store.commit("changeShowRightMenu", true)
            this.$store.commit('changeLeft', e.pageX)
            this.$store.commit('changeTop', e.pageY)
            this.stopPropagation()
        },

        /**
         * open stream or measure
         */
        openResources(index, indexChildren) {
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
                    case 'new resources':
                        this.operation = 'create'
                        this.openResourcesDialog()
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
         * click right menu delete Resources
         */
        openDeleteDialog() {
            this.dialogVisible = true
        },
        deleteGroupOrResources() {
            let group = this.groupLists[this.rightGroupIndex].metadata.name
            let type = this.groupLists[this.rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
            if (this.rightClickType == 'group') {
                // delete group
                this.deleteGroup(group, type)
            } else {
                // delete measure or stream Resources
                this.deleteResources(group, type)
            }
        },
        deleteGroup(group, type) {
            let children = this.groupLists[this.rightGroupIndex].children
            // Check whether the Resources is open
            for (let i = 0; i < children.length; i++) {
                let Resources = children[i]
                let index = this.tags.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === Resources.metadata.name)
                if (index != -1) {
                    Message({
                        message: 'There are Resources open in this group. Please close these Resources before proceeding',
                        type: "warning",
                        duration: 5000
                    })
                    this.dialogVisible = false
                    return
                }
            }
            // delete group
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
        },
        deleteResources(group, type) {
            let name = this.groupLists[this.rightGroupIndex].children[this.rightChildIndex].metadata.name
            // Check whether the Resources is open
            let index = this.tags.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === name)
            if (index != -1) {
                Message({
                    message: 'This resources has been opened. Please close the resources before proceeding!',
                    type: "warning",
                    duration: 5000
                })
                this.dialogVisible = false
                return
            }
            // delete Resources
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
        },

        /**
         * click right menu 'new group' or 'edit group'
         */
        openCreateGroup() {
            this.dialogGroupVisible = true
        },
        openEditGroup() {
            let name = this.groupLists[this.rightGroupIndex].metadata.name
            let catalog = this.groupLists[this.rightGroupIndex].catalog
            this.groupForm.name = name
            this.groupForm.catalog = catalog
            this.dialogGroupVisible = true
        },
        // create group or edit group
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
                            this.dialogGroupVisible = false
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
                            this.dialogGroupVisible = false
                            this.$loading.close()
                        })
                }
            })
        },
        // init form data
        clearGroupForm() {
            this.groupForm = {
                name: null,
                catalog: 'CATALOG_STREAM'
            }
        },

        /**
         * click right menu 'new resources' or 'edit resources'
         */
        openResourcesDialog() {
            // the group is stream or measure
            let type = this.groupLists[this.rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
            let group = this.groupLists[this.rightGroupIndex].metadata.name
            this.group = group
            this.type = type
            this.dialogResourcesVisible = true
        },
        cancelResourcesDialog() {
            this.dialogResourcesVisible = false
        },
        confirmResourcesDialog(form) {
            let type = this.groupLists[this.rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
            let data = {}
            data[type] = form
            this.$loading.create()
            createResources(type, data)
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
                    this.dialogResourcesVisible = false
                    this.$loading.close()
                })
        }
    },
}
</script>

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
                                @click="openResources(index, indexChildren)">
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
                <el-button type="primary" @click="deleteGroupOrResources">delete</el-button>
            </span>
        </el-dialog>
        <el-dialog width="25%" center :title="`${setGroup} group`" :visible.sync="dialogGroupVisible">
            <el-form ref="ruleForm" :rules="rules" :model="groupForm" label-position="left">
                <el-form-item label="group name" label-width="100px" prop="name">
                    <el-input :disabled="setGroup == 'edit'" v-model="groupForm.name" autocomplete="off"
                        style="width: 300px;"></el-input>
                </el-form-item>
                <el-form-item label="group type" label-width="100px" prop="catalog">
                    <el-select v-model="groupForm.catalog" style="width: 300px;" placeholder="please select">
                        <el-option label="CATALOG_STREAM" value="CATALOG_STREAM"></el-option>
                        <el-option label="CATALOG_MEASURE" value="CATALOG_MEASURE"></el-option>
                    </el-select>
                </el-form-item>
            </el-form>
            <div slot="footer" class="dialog-footer">
                <el-button @click="dialogGroupVisible = false">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{setGroup}}
                </el-button>
            </div>
        </el-dialog>
        <dialog-resources-component :visible.sync="dialogResourcesVisible" :group="group" :operation="operation"
            :type="type" @cancel="cancelResourcesDialog" @confirm="confirmResourcesDialog"></dialog-resources-component>
    </div>
</template>

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