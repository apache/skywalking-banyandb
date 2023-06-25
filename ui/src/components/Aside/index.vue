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
import RigheMenu from '@/components/RightMenu/index.vue'
import { getGroupList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup, createResources } from '@/api/index'
import { ElMessage, ElMessageBox } from 'element-plus'
import { watch, getCurrentInstance } from "@vue/runtime-core"
import { useRouter, useRoute } from 'vue-router'
import { ref, reactive } from 'vue'
import { Search } from '@element-plus/icons-vue'
const router = useRouter()
const route = useRoute()
const { proxy } = getCurrentInstance()
const { ctx: that } = getCurrentInstance()

// ref
const ruleForm = ref()

// Loading
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose

// Data
const data = reactive({
    groupLists: [],
    groupListsCopy: [],
    showSearch: false,
    isShrink: false,
    isCollapse: false,
    // right menu
    showRightMenu: false,
    rightMenuList: [],
    top: 0,
    left: 0,
    clickIndex: 0,
    clickChildIndex: 0,
    rightClickType: 'group',
    // create/edit group
    dialogGroupVisible: false,
    setGroup: 'create',
    groupForm: {
        name: null,
        catalog: 'CATALOG_STREAM'
    },
    activeMenu: '',
    search: ''
})

watch(() => data.search, () => {
    debounce(searchGroup, 300)()
})

// menu config
const groupMenu = [
    {
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
    }
]
const resourceMenu = [
    {
        icon: "el-icon-document",
        name: "edit resources",
        id: "edit resources"
    },
    {
        icon: "el-icon-delete",
        name: "delete",
        id: "delete resources"
    }
]
const menuItemFunction = {
    "new group": openCreateGroup,
    "edit group": openEditGroup,
    "new resources": openCreateResource,
    "refresh": getGroupList,
    "delete": openDeletaDialog,
    "edit resources": openEditResource
}

// rules
const rules = {
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

// Eventbus
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

// props data
const props = defineProps({
    type: {
        type: String,
        required: true,
        default: ''
    }
})

// emit event
const emit = defineEmits(['setWidth'])

// function
// search group
function debounce(event, delay) {
    let timer = null
    return function (...args) {
        clearTimeout(timer)
        timer = setTimeout(() => {
            event(args)
        }, delay)
    }
}

function searchGroup() {
    if (!data.search) {
        return data.groupLists = JSON.parse(JSON.stringify(data.groupListsCopy))
    }
    let groupLists = []
    data.groupListsCopy.forEach(item => {
        let itemCache = JSON.parse(JSON.stringify(item))
        if (Array.isArray(itemCache.children)) {
            itemCache.children = itemCache.children.filter(child => {
                return child.metadata.name.indexOf(data.search) > -1
            })
            if (itemCache.children.length > 0) {
                groupLists.push(itemCache)
            }
        }
    })
    data.groupLists = JSON.parse(JSON.stringify(groupLists))
    that.$forceUpdate()
}
// init data
function getGroupLists() {
    $loadingCreate()
    data.showSearch = false
    data.search = ''
    getGroupList()
        .then(res => {
            if (res.status == 200) {
                let group = res.data.group
                data.groupLists = group
                deleteOtherGroup()
                let promise = data.groupLists.map((item) => {
                    let type = props.type
                    let name = item.metadata.name
                    return new Promise((resolve, reject) => {
                        getStreamOrMeasureList(type, name)
                            .then(res => {
                                if (res.status == 200) {
                                    item.children = res.data[type]
                                    resolve()
                                }
                            })
                            .catch((err) => {
                                reject(err)
                            })
                    })
                })
                Promise.all(promise).then(() => {
                    data.showSearch = true
                    data.groupListsCopy = JSON.parse(JSON.stringify(data.groupLists))
                    that.$forceUpdate()
                }).catch((err) => {
                    ElMessage({
                        message: 'An error occurred while obtaining group data. Please refresh and try again. Error: ' + err,
                        type: "error",
                        duration: 3000
                    })
                }).finally(() => {
                    $loadingClose()
                })
            }
        })
}
function deleteOtherGroup() {
    for (let i = 0; i < data.groupLists.length; i++) {
        let type = data.groupLists[i].catalog == 'CATALOG_MEASURE' ? 'measure' : 'stream'
        if (type !== props.type) {
            data.groupLists.splice(i, 1)
            i--
        }
    }
}
// to resources
function openResources(index, childIndex) {
    const group = data.groupLists[index].children[childIndex].metadata.group
    const name = data.groupLists[index].children[childIndex].metadata.name
    const route = {
        name: props.type,
        params: {
            group: group,
            name: name,
            operator: 'read',
            type: props.type
        }
    }
    router.push(route)
    const add = {
        label: name,
        type: 'Read',
        route
    }
    data.activeMenu = `${group}-${name}`
    $bus.emit('AddTabs', add)
}
// open or close Aide
function shrinkMove(e) {
    e.preventDefault()
    if (data.isShrink) {
        let wid = e.screenX + 5
        if (wid <= 65) {
            $bus.emit('changeIsCollapse', {
                isCollapse: true,
                width: '65px'
            })
        } else {
            $bus.emit('changeIsCollapse', {
                isCollapse: false,
                width: `${wid > 450 ? 450 : wid}px`
            })
        }
    }
}
function shrinkUp(e) {
    e.stopPropagation()
    data.isShrink = false
    document.getElementById('app').onmousemove = null
    document.getElementById('app').onmouseup = null
    document.getElementById('app').onmouseleave = null
    document.getElementById('app').ondragover = null
}
function shrinkDown(e) {
    data.isShrink = true
    document.getElementById('app').onmousemove = shrinkMove
    document.getElementById('app').onmouseup = shrinkUp
    document.getElementById('app').onmouseleave = shrinkUp
    document.getElementById('app').ondragover = shrinkUp
    e.preventDefault()
}
// right click menu
function rightClickGroup(e, index) {
    data.rightMenuList = groupMenu
    data.clickIndex = index
    data.rightClickType = 'group'
    openRightMenu(e)
}
function rightClickResources(e, index, childIndex) {
    data.rightMenuList = resourceMenu
    data.clickIndex = index
    data.clickChildIndex = childIndex
    data.rightClickType = 'resources'
    openRightMenu(e)
}
function openRightMenu(e) {
    data.showRightMenu = true
    data.top = e.pageY
    data.left = e.pageX
    document.getElementById('app').onclick = closeRightMenu
    stopPropagation()
}
function closeRightMenu() {
    data.showRightMenu = false
    document.getElementById('app').onclick = null
    stopPropagation()
}
function handleRightItem(index) {
    const name = data.rightMenuList[index].name
    return menuItemFunction[name]()
}
function stopPropagation(e) {
    e = e || window.event;
    if (e.stopPropagation) {
        e.stopPropagation();
    } else {
        e.cancelBubble = true;
    }
}

// CRUD operator
function openCreateGroup() {
    data.setGroup = 'create'
    data.dialogGroupVisible = true
}
function openEditGroup() {
    data.groupForm.name = data.groupLists[data.clickIndex].metadata.name
    data.groupForm.catalog = data.groupLists[data.clickIndex].catalog
    data.dialogGroupVisible = true
    data.setGroup = 'edit'
}
function openCreateResource() {
    const route = {
        name: `create-${props.type}`,
        params: {
            operator: 'create',
            group: data.groupLists[data.clickIndex].metadata.name,
            name: '',
            type: props.type
        }
    }
    router.push(route)
    const add = {
        label: data.groupLists[data.clickIndex].metadata.name,
        type: 'Create',
        route
    }
    data.activeMenu = ''
    $bus.emit('AddTabs', add)
}
function openEditResource() {
    const route = {
        name: `edit-${props.type}`,
        params: {
            operator: 'edit',
            group: data.groupLists[data.clickIndex].metadata.name,
            name: data.groupLists[data.clickIndex].children[data.clickChildIndex].metadata.name,
            type: props.type
        }
    }
    router.push(route)
    const add = {
        label: data.groupLists[data.clickIndex].children[data.clickChildIndex].metadata.name,
        type: 'Edit',
        route
    }
    $bus.emit('AddTabs', add)
}
function openDeletaDialog() {
    ElMessageBox.confirm('Are you sure to delete this resource?')
        .then(() => {
            let group = data.groupLists[data.clickIndex].metadata.name
            if (data.rightClickType == 'group') {
                return deleteGroupFunction(group)
            }
            return deleteResource(group)
        })
        .catch(() => {
            // catch error
        })
}
function deleteGroupFunction(group) {
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
                $bus.emit('deleteGroup', data.groupLists[data.clickIndex].metadata.name)
            }
        })
        .finally(() => {
            $loadingClose()
        })
}
function deleteResource(group) {
    let name = data.groupLists[data.clickIndex].children[data.clickChildIndex].metadata.name
    // delete Resources
    $loadingCreate()
    deleteStreamOrMeasure(props.type, group, name)
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
                $bus.emit('deleteResource', name)
            }
        })
        .finally(() => {
            $loadingClose()
        })
}

// create/edit group
function confirmForm() {
    data.setGroup == 'create' ? createGroupFunction() : editGroupFunction()
}
function createGroupFunction() {
    ruleForm.value.validate((valid) => {
        if (valid) {
            let dataList = {
                group: {
                    metadata: {
                        group: "",
                        name: data.groupForm.name
                    },
                    catalog: data.groupForm.catalog
                }
            }
            $loadingCreate()
            createGroup(dataList)
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
                    data.dialogGroupVisible = false
                    $loadingClose()
                })
        }
    })
}
function editGroupFunction() {
    let name = data.groupLists[data.clickIndex].metadata.name
    ruleForm.value.validate((valid) => {
        if (valid) {
            let dataList = {
                group: {
                    metadata: {
                        group: "",
                        name: data.groupForm.name
                    },
                    catalog: data.groupForm.catalog
                }
            }
            $loadingCreate()
            editGroup(name, dataList)
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
                    data.dialogGroupVisible = false
                    $loadingClose()
                })
        }
    })
}
function cancelCreateEditDialog() {
    clearGroupForm()
    data.dialogGroupVisible = false
}
// init form data
function clearGroupForm() {
    data.groupForm = {
        name: null,
        catalog: 'CATALOG_STREAM'
    }
}
function initActiveMenu() {
    const group = route.params.group
    const name = route.params.name
    if (group && name) {

        data.activeMenu = `${group}-${name}`
    }
}
// Eventbus, change isCollapse
$bus.on('changeIsCollapse', (obj) => {
    data.isCollapse = obj.isCollapse
    emit('setWidth', obj.width)
})
$bus.on('changeAside', (obj) => {
    data.activeMenu = `${obj.group}-${obj.name}`
})
$bus.on('resetAside', () => {
    data.activeMenu = ''
    router.push({
        name: `${props.type}Start`
    })
})
$bus.on('refreshAside', () => {
    getGroupLists()
})

getGroupLists()
initActiveMenu()


</script>

<template>
    <div style="display: flex; flex-direction: column; width: 100%;">
        <div class="size flex" style="display: flex; flex-direction: column; width: 100%;">
            <el-input v-if="data.showSearch" class="aside-search" v-model="data.search" placeholder="Search"
                :prefix-icon="Search" clearable />
            <el-menu v-if="data.groupLists.length > 0" :collapse="data.isCollapse" :default-active="data.activeMenu">
                <div v-for="(item, index) in data.groupLists" :key="item.metadata.name"
                    @contextmenu.prevent="rightClickGroup($event, index)">
                    <el-sub-menu :index="`${item.metadata.name}-${index}`">
                        <template #title>
                            <el-icon>
                                <Folder />
                            </el-icon>
                            <span slot="title" :title="item.metadata.name" style="width: 70%" class="text-overflow-hidden">
                                {{ item.metadata.name }}
                            </span>
                        </template>
                        <div v-for="(child, childIndex) in item.children" :key="child.metadata.name">
                            <div @contextmenu.prevent="rightClickResources($event, index, childIndex)">
                                <el-menu-item :index="`${child.metadata.group}-${child.metadata.name}`"
                                    @click="openResources(index, childIndex)">
                                    <template #title>
                                        <el-icon>
                                            <Document />
                                        </el-icon>
                                        <span slot="title" :title="child.metadata.name" style="width: 90%"
                                            class="text-overflow-hidden">
                                            {{ child.metadata.name }}
                                        </span>
                                    </template>
                                </el-menu-item>
                            </div>
                        </div>
                    </el-sub-menu>
                </div>
            </el-menu>
            <div class="resize" @mousedown="shrinkDown" title="Shrink sidebar"></div>
        </div>
        <div class="flex center add" @click="openCreateGroup" style="height: 50px; width: 100%;"
            v-if="data.groupLists.length == 0">
            <el-icon>
                <Plus />
            </el-icon>
        </div>
        <el-dialog width="25%" center :title="`${data.setGroup} group`" v-model="data.dialogGroupVisible"
            :show-close="false">
            <el-form ref="ruleForm" :rules="rules" :model="data.groupForm" label-position="left">
                <el-form-item label="group name" label-width="120px" prop="name">
                    <el-input :disabled="data.setGroup == 'edit'" v-model="data.groupForm.name" autocomplete="off">
                    </el-input>
                </el-form-item>
                <el-form-item label="group type" label-width="120px" prop="catalog">
                    <el-select v-model="data.groupForm.catalog" placeholder="please select" style="width: 100%">
                        <el-option label="CATALOG_STREAM" value="CATALOG_STREAM"></el-option>
                        <el-option label="CATALOG_MEASURE" value="CATALOG_MEASURE"></el-option>
                    </el-select>
                </el-form-item>
            </el-form>
            <div slot="footer" class="dialog-footer footer">
                <el-button @click="cancelCreateEditDialog">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{ data.setGroup }}
                </el-button>
            </div>
        </el-dialog>
        <div v-if="data.showRightMenu" class="right-menu box-shadow"
            :style="{ top: `${data.top}px`, left: `${data.left}px` }">
            <RigheMenu @handleRightItem="handleRightItem" :rightMenuList="data.rightMenuList">
            </RigheMenu>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.aside-search {
    margin: 10px;
    width: calc(100% - 20px);
}

.el-menu {
    width: 100%;
    border-right: none;
    text-align: start;
    text-justify: middle;
}

.resize {
    cursor: col-resize;
    position: absolute;
    right: 0;
    height: 100%;
    width: 5px;
}

.right-menu {
    width: 170px;
    position: fixed;
    z-index: 9999999999999999999999999999 !important;
    background-color: white;
}

.footer {
    width: 100%;
    display: flex;
    justify-content: center;
}

.add {
    cursor: pointer;
}
</style>