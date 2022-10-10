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
import stores from '../../../stores/index'
import { getGroupList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup, createResources } from '@/api/index'
import { ElMessage } from 'element-plus'
import RightMenuComponent from './components/RightMenu/index.vue'
import DialogResourcesComponent from './components/DialogResources/index.vue'
import { getCurrentInstance } from "@vue/runtime-core"
import { ref } from 'vue'
import { computed } from '@vue/runtime-core'

const { proxy } = getCurrentInstance()
const $loadingCreate = proxy.$loadingCreate
const $loadingClose = proxy.$loadingClose
// eventBus
const $bus = proxy.$bus
const { ctx: that } = getCurrentInstance()
const { aside, tags, menuState } = stores()
const ruleForm = ref()
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
function getGroupLists() {
    $loadingCreate()
    getGroupList()
        .then(res => {
            if (res.status == 200) {
                let group = res.data.group
                let length = group.length
                groupLists = group
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
                                $loadingClose()
                            }
                            that.$forceUpdate()
                        })
                })
            }
        })
}

function stopPropagation(e) {
    e = e || window.event;
    if (e.stopPropagation) {
        e.stopPropagation();
    } else {
        e.cancelBubble = true;
    }
}

/**
 * open group or resources right menu
 */
function rightClick(e, index, indexChild) {
    menuState.changeRightMenuList(rightMenuListThree)
    rightClickType = 'resources'
    rightGroupIndex = index
    rightChildIndex = indexChild
    openRightMenu(e)
}
function rightClickGroup(e, index) {
    menuState.changeRightMenuList(rightMenuListTwo)
    rightClickType = 'group'
    rightGroupIndex = index
    openRightMenu(e)
}
function openRightMenu(e) {
    menuState.changeShowRightMenu(true)
    menuState.changeLeft(e.pageX)
    menuState.changeTop(e.pageY)
    stopPropagation()
}

/**
 * open stream or measure 
 */
function openResources(index, indexChildren) {
    let item = groupLists[index].children[indexChildren]
    /**
     * Todo
     * Measure or Stream?
     */
    if (groupLists[index].catalog == "CATALOG_MEASURE") {
        item.metadata.type = "measure"
    } else {
        item.metadata.type = "stream"
    }
    tags.selectMenu(item)
}

/**
 * click right menu item
 */
function handleRightItem(index) {
    if (rightClickType == 'group') {
        // right click group
        let rightName = rightMenuListTwo[index].name
        switch (rightName) {
            case 'new group':
                setGroup = 'create'
                openCreateGroup()
                break
            case 'edit group':
                setGroup = 'edit'
                openEditGroup()
                break
            case 'new resources':
                operation = 'create'
                openResourcesDialog()
                break
            case 'refresh':
                getGroupLists()
                break
            case 'delete':
                openDeleteDialog()
                break
        }
    } else {
        // right click measure or stream
        let rightName = rightMenuListThree[index].name
        switch (rightName) {
            case 'delete':
                openDeleteDialog()
        }
    }
    // close right menu
    menuState.changeShowRightMenu(false)
}

/**
 * click right menu delete Resources
 */
function openDeleteDialog() {
    dialogVisible = true
}
function deleteGroupOrResources() {
    let group = groupLists[rightGroupIndex].metadata.name
    let type = groupLists[rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
    if (rightClickType == 'group') {
        // delete group
        deleteGroupFunc(group, type)
    } else {
        // delete measure or stream Resources
        deleteResources(group, type)
    }
}
function deleteGroupFunc(group, type) {
    let children = groupLists[rightGroupIndex].children
    // Check whether the Resources is open
    for (let i = 0; i < children.length; i++) {
        let Resources = children[i]
        let index = tagsList.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === Resources.metadata.name)
        if (index != -1) {
            ElMessage({
                message: 'There are Resources open in this group. Please close these Resources before proceeding',
                type: "warning",
                duration: 5000
            })
            dialogVisible = false
            return
        }
    }
    // delete group
    $loadingCreate()
    deleteGroup(group)
        .then((res) => {
            if (res.status == 200) {
                if (res.data.deleted) {
                    ElMessage({
                        message: 'Delete succeeded',
                        type: "success",
                        duration: 5000
                    })
                    getGroupLists()
                }
            }
        })
        .finally(() => {
            $loadingClose()
            dialogVisible = false
        })
}
function deleteResources(group, type) {
    let name = groupLists[rightGroupIndex].children[rightChildIndex].metadata.name
    // Check whether the Resources is open
    let index = tagsList.findIndex((item) => item.metadata.group === group && item.metadata.type === type && item.metadata.name === name)
    if (index != -1) {
        ElMessage({
            message: 'This resources has been opened. Please close the resources before proceeding!',
            type: "warning",
            duration: 5000
        })
        dialogVisible = false
        return
    }
    // delete Resources
    $loadingCreate()
    deleteStreamOrMeasure(type, group, name)
        .then((res) => {
            if (res.status == 200) {
                if (res.data.deleted) {
                    ElMessage({
                        message: 'Delete succeeded',
                        type: "success",
                        duration: 5000
                    })
                    getGroupLists()
                }
            }
        })
        .finally(() => {
            $loadingClose()
            dialogVisible = false
        })
}

/**
 * click right menu 'new group' or 'edit group'
 */
function openCreateGroup() {
    dialogGroupVisible = true
}
function openEditGroup() {
    let name = groupLists[rightGroupIndex].metadata.name
    let catalog = groupLists[rightGroupIndex].catalog
    groupForm.name = name
    groupForm.catalog = catalog
    dialogGroupVisible = true
}
// create group or edit group
function confirmForm() {
    setGroup == 'create' ? createGroupFunc() : editGroupFunc()
}
function createGroupFunc() {
    ruleForm.validate((valid) => {
        if (valid) {
            let data = {
                group: {
                    metadata: {
                        group: "",
                        name: groupForm.name
                    },
                    catalog: groupForm.catalog
                }
            }
            $loadingCreate()
            createGroup(data)
                .then((res) => {
                    if (res.status == 200) {
                        getGroupLists()
                        ElMessage({
                            message: 'Created successfully',
                            type: "success",
                            duration: 3000
                        })
                    }
                })
                .finally(() => {
                    dialogGroupVisible = false
                    $loadingClose()
                })
        }
    })
}
function editGroupFunc() {
    let name = groupLists[rightGroupIndex].metadata.name
    ruleForm.validate((valid) => {
        if (valid) {
            let data = {
                group: {
                    metadata: {
                        group: "",
                        name: groupForm.name
                    },
                    catalog: groupForm.catalog
                }
            }
            $loadingCreate()
            editGroup(name, data)
                .then((res) => {
                    if (res.status == 200) {
                        getGroupLists()
                        ElMessage({
                            message: 'Update succeeded',
                            type: "success",
                            duration: 3000
                        })
                    }
                })
                .finally(() => {
                    dialogGroupVisible = false
                    $loadingClose()
                })
        }
    })
}
// init form data
function clearGroupForm() {
    groupForm = {
        name: null,
        catalog: 'CATALOG_STREAM'
    }
}

/**
 * click right menu 'new resources' or 'edit resources'
 */
function openResourcesDialog() {
    // the group is stream or measure
    let type = groupLists[rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
    let group = groupLists[rightGroupIndex].metadata.name
    group = group
    type = type
    dialogResourcesVisible = true
}
function cancelResourcesDialog() {
    dialogResourcesVisible = false
}
function confirmResourcesDialog(form) {
    let type = groupLists[rightGroupIndex].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
    let data = {}
    data[type] = form
    $loadingCreate()
    createResources(type, data)
        .then((res) => {
            if (res.status == 200) {
                getGroupLists()
                ElMessage({
                    message: 'Created successfully',
                    type: "success",
                    duration: 3000
                })
            }
        })
        .finally(() => {
            dialogResourcesVisible = false
            $loadingClose()
        })
}
// get group list
getGroupLists()
// monitor click right menu item
$bus.on('handleRightItem', (index) => {
    handleRightItem(index)
})
</script>

<template>
    <div style="width:100%; height:100%">
        <el-menu :default-active="currentMenu ? currentMenu.metadata.group + currentMenu.metadata.name : ''"
            active-text-color="#6E38F7" style="height: 100%;" :collapse="isCollapse" :collapse-transition="false">
            <div v-for="(item, index) in groupLists" :key="item.metadata.name"
                @contextmenu.prevent="rightClickGroup($event, index)">
                <el-sub-menu :index="item.metadata.name + '-' + index" :disabled="item.catalog == 'CATALOG_MEASURE'">
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
                </el-sub-menu>
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