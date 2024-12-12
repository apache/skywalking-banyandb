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
import { deleteSecondaryDataModel, getindexRuleList, getindexRuleBindingList, getGroupList, getTopNAggregationList, getStreamOrMeasureList, deleteStreamOrMeasure, deleteGroup, createGroup, editGroup } from '@/api/index'
import { ElMessage, ElMessageBox } from 'element-plus'
import { watch, getCurrentInstance } from "@vue/runtime-core"
import { useRouter, useRoute } from 'vue-router'
import { ref, reactive } from 'vue'
import { Search } from '@element-plus/icons-vue'

const router = useRouter()
const route = useRoute()
const { proxy } = getCurrentInstance()

// ref
const ruleForm = ref()
const loading= ref(false)
const treeRef = ref()
const filterText = ref('')
const currentNode = ref({})
// Data
const data = reactive({
    groupLists: [],
    showSearch: false,
    isCollapse: false,
    // right menu
    showOperationMenus: false,
    operationMenus: [],
    top: 0,
    left: 0,
    // create/edit group
    dialogGroupVisible: false,
    setGroup: 'create',
    groupForm: {
        name: null,
        catalog: 'CATALOG_STREAM',
        shardNum: 1,
        segmentIntervalUnit: "UNIT_DAY",
        segmentIntervalNum: 1,
        ttlUnit: "UNIT_DAY",
        ttlNum: 3
    },
    activeMenu: '',
    formLabelWidth: "170px"
})

const defaultProps = {
  children: 'children',
  label: 'name',
}

// Loading
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose

// catalog to group type
const catalogToGroupType = {
    'CATALOG_MEASURE': 'measure',
    'CATALOG_STREAM': 'stream',
    'CATALOG_UNSPECIFIED': 'property'
}

// group type to catalog
const groupTypeToCatalog = {
    'measure': 'CATALOG_MEASURE',
    'stream': 'CATALOG_STREAM',
    'property': 'CATALOG_UNSPECIFIED'
}

const TypeMap = {
    'topNAggregation': 'topn-agg',
    'indexRule': 'index-rule',
    'indexRuleBinding': 'index-rule-binding',
    'children': 'children'
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
    ],
    shardNum: [
        {
            required: true, message: 'Please select the shard num of the group', trigger: 'blur'
        }
    ],
    segmentIntervalUnit: [
        {
            required: true, message: 'Please select the segment interval unit of the group', trigger: 'blur'
        }
    ],
    segmentIntervalNum: [
        {
            required: true, message: 'Please select the segment Interval num of the group', trigger: 'blur'
        }
    ],
    ttlUnit: [
        {
            required: true, message: 'Please select the ttl unit of the group', trigger: 'blur'
        }
    ],
    ttlNum: [
        {
            required: true, message: 'Please select the ttl num of the group', trigger: 'blur'
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

// init data
function getGroupLists() {
    $loadingCreate()
    data.showSearch = false
    filterText.value = ''
    loading.value = true
    getGroupList()
        .then(res => {
            if (res.status == 200) {
                let group = res.data.group
                data.groupLists = group
                for (let i = 0; i < data.groupLists.length; i++) {
                    let type = catalogToGroupType[data.groupLists[i].catalog]
                    if (type !== props.type) {
                        data.groupLists.splice(i, 1)
                        i--
                    }
                }
                if (props.type == 'property') {
                    data.showSearch = true
                    $loadingClose()
                    return
                }
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
                if (props.type == 'stream' || props.type == 'measure') {
                    let promiseIndexRule = data.groupLists.map((item) => {
                        let name = item.metadata.name
                        return new Promise((resolve, reject) => {
                            getindexRuleList(name)
                                .then(res => {
                                    if (res.status == 200) {
                                        item.indexRule = res.data.indexRule
                                        resolve()
                                    }
                                })
                                .catch((err) => {
                                    reject(err)
                                })
                        })
                    })
                    let promiseIndexRuleBinding = data.groupLists.map((item) => {
                        let name = item.metadata.name
                        return new Promise((resolve, reject) => {
                            getindexRuleBindingList(name)
                                .then(res => {
                                    if (res.status == 200) {
                                        item.indexRuleBinding = res.data.indexRuleBinding
                                        resolve()
                                    }
                                })
                                .catch((err) => {
                                    reject(err)
                                })
                        })
                    })
                    promise = promise.concat(promiseIndexRule)
                    promise = promise.concat(promiseIndexRuleBinding)
                }
                if (props.type == 'measure') {
                    let TopNAggregationRule = data.groupLists.map((item) => {
                        let name = item.metadata.name
                        return new Promise((resolve, reject) => {
                            getTopNAggregationList(name)
                                .then(res => {
                                    if (res.status == 200) {
                                        item.topNAggregation = res.data.topNAggregation
                                        resolve()
                                    }
                                })
                                .catch((err) => {
                                    reject(err)
                                })
                        })
                    })
                    promise = promise.concat(TopNAggregationRule)
                }
                Promise.all(promise).then(() => {
                    data.showSearch = true
                    data.groupLists = processGroupTree()
                }).catch((err) => {
                    ElMessage({
                        message: 'An error occurred while obtaining group data. Please refresh and try again. Error: ' + err,
                        type: "error",
                        duration: 3000
                    })
                }).finally(() => {
                    $loadingClose()
                    loading.value = false
                })
            }else{
                $loadingClose()
                loading.value = false
            }
        })
    $loadingClose()
}

function processGroupTree() {
    const trees = [];
    for (const group of data.groupLists) {
        const g = {
            ...group.metadata,
            children: [],
            catalog: group.catalog,
            type: 'group',
            key: group.metadata.name,
        }
        const keys = Object.keys(TypeMap);
        for (const key of keys) {
            if (group[key]) {
                const n = key === 'children' ? props.type : key
                const list = {
                    name: n.charAt(0).toUpperCase() + n.slice(1),
                    children: [],
                    type: n,
                    key: `${group.metadata.name}_${n}`,
                    group: group.metadata.name,
                }
                for (const item of group[key]) {
                    list.children.push({
                        ...item.metadata,
                        type: 'resources',
                        key: `${group.metadata.name}_${n}_${item.metadata.name}`,
                        typeFlag: TypeMap[n] || n,
                        catalog: group.catalog,
                        resourceOpts: group.resourceOpts
                    })
                }
                g.children.push(list);
            }
        }
        trees.push(g);
    }
    return trees;
}

// to resources
function openResources(node) {
    currentNode.value = node;
    if (node.type !== 'resources') {
        return
    }
    const {name, group, typeFlag} = node
    const values = Object.values(TypeMap)

    if (values.includes(node.typeFlag)) {
        const route = {
            name: `${props.type}-${typeFlag}`,
            params: {
                group: group,
                name: name,
                operator: 'read',
                type: typeFlag
            }
        }
        router.push(route)
        const add = {
            label: name,
            type: `Read-${typeFlag}`,
            route
        }
        data.activeMenu = `${group}-${name}` 
        return $bus.emit('AddTabs', add)
    }
    if (props.type == 'property') {
        const route = {
            name: 'property',
            params: {
                group: name,
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
        data.active = `${name}`
        return $bus.emit('AddTabs', add)
    }
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

function openOperationMenus(e, node) {
    currentNode.value = node
    data.showOperationMenus = true
    data.top = e.pageY
    data.left = e.pageX
    document.getElementById('app').onclick = closeRightMenu
    e.stopPropagation()
    const AllMenus = [
        {label: 'Create', fn: openCreateGroup}, 
        {label: 'Edit', fn: openEditGroup},
        {label: `Refresh`, fn: getGroupLists},
        {label: 'Delete', fn: openDeletaDialog}
    ]
    if (currentNode.value.type === 'group') {
        data.operationMenus = AllMenus
        return;
    }
    if (currentNode.value.type === 'resources') {
        data.operationMenus = [AllMenus[1], AllMenus[3]]
        return;
    }
    data.operationMenus = [AllMenus[0]]
}
function closeRightMenu() {
    data.showOperationMenus = false
    document.getElementById('app').onclick = null
    e.stopPropagation()
}

// CRUD operator
function openCreateSecondaryDataModel() {
    const route = {
        name: `${data.schema}-create-${data.rightClickType}`,
        params: {
            operator: 'create',
            group: data.groupLists[data.clickIndex].metadata.name,
            type: data.rightClickType,
            schema: data.schema
        }
    }
    router.push(route)
    const add = {
        label: data.groupLists[data.clickIndex].metadata.name,
        type: `Create-${data.rightClickType}`,
        route
    }
    data.activeMenu = ''
    $bus.emit('AddTabs', add)
}

function openEditSecondaryDataModel() {
    const typeFlag = {
        'topn-agg': 'topNAggregation',
        'index-rule': 'indexRule',
        'index-rule-binding': 'indexRuleBinding'
    }
    const route = {
        name: `${data.schema}-edit-${data.rightClickType}`,
        params: {
            operator: 'edit',
            group: data.groupLists[data.clickIndex].metadata.name,
            name: data.groupLists[data.clickIndex][typeFlag[data.rightClickType]][data.clickChildIndex].metadata.name,
            type: data.rightClickType,
            schema: data.schema
        }
    }
    router.push(route)
    const add = {
        label: data.groupLists[data.clickIndex][typeFlag[data.rightClickType]][data.clickChildIndex].metadata.name,
        type: `Edit-${data.rightClickType}`,
        route
    }
    $bus.emit('AddTabs', add)
}

function openCreateGroup() {
    data.setGroup = 'create'
    data.groupForm.catalog = groupTypeToCatalog[props.type]
    data.dialogGroupVisible = true
}
function openEditGroup() {
    data.groupForm.name = currentNode.value.name
    data.groupForm.catalog = currentNode.value.catalog
    data.groupForm.shardNum = currentNode.value.resourceOpts?.shardNum
    data.groupForm.segmentIntervalUnit = currentNode.value.resourceOpts?.segmentInterval?.unit
    data.groupForm.segmentIntervalNum = currentNode.value.resourceOpts?.segmentInterval?.num
    data.groupForm.ttlUnit = currentNode.value.resourceOpts?.ttl?.unit
    data.groupForm.ttlNum = currentNode.value.resourceOpts?.ttl?.num
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
    ElMessageBox.confirm('Are you sure to delete?')
        .then(() => {
            if (Object.keys(TypeMap).includes(currentNode.value.type)) {
                return deleteSecondaryDataModelFunction(TypeMap[currentNode.value.type])
            }
            if (type === 'group') {
                return deleteGroupFunction()
            }
            return deleteResource()
        })
}
function deleteSecondaryDataModelFunction(param) {
    $loadingCreate()
    deleteSecondaryDataModel(param, currentNode.value.group, currentNode.value.type)
        .then((res) => {
            if (res.status == 200) {
                if (res.data.deleted) {
                    ElMessage({
                        message: 'Delete succeeded',
                        type: "success",
                        duration: 5000
                    })
                    getGroupLists()
                    $bus.emit('deleteResource', currentNode.value.type)
                }
            }
        })
        .finally(() => {
            $loadingClose()
        })
}
function deleteGroupFunction() {
    // delete group
    $loadingCreate()
    deleteGroup(currentNode.value.name)
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
                $bus.emit('deleteGroup', currentNode.value.name)
            }
        })
        .finally(() => {
            $loadingClose()
        })
}
function deleteResource() {
    // delete Resources
    deleteStreamOrMeasure(props.type, currentNode.value.group, currentNode.value.name)
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
                $bus.emit('deleteResource', currentNode.value.name)
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
                    catalog: data.groupForm.catalog,
                    resourceOpts: {
                        shardNum: data.groupForm.shardNum,
                        segmentInterval: {
                            unit: data.groupForm.segmentIntervalUnit,
                            num: data.groupForm.segmentIntervalNum
                        },
                        ttl: {
                            unit: data.groupForm.ttlUnit,
                            num: data.groupForm.ttlNum
                        }
                    }
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
                    catalog: data.groupForm.catalog,
                    resourceOpts: {
                        shardNum: data.groupForm.shardNum,
                        segmentInterval: {
                            unit: data.groupForm.segmentIntervalUnit,
                            num: data.groupForm.segmentIntervalNum
                        },
                        ttl: {
                            unit: data.groupForm.ttlUnit,
                            num: data.groupForm.ttlNum
                        }
                    }
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
        catalog: 'CATALOG_STREAM',
        shardNum: 1,
        segmentIntervalUnit: "UNIT_DAY",
        segmentIntervalNum: 1,
        ttlUnit: "UNIT_DAY",
        ttlNum: 3
    }
}
function initActiveMenu() {
    const group = route.params.group
    const name = route.params.name
    if (group && name) {

        data.activeMenu = `${group}-${name}`
    }
}
const filterNode = (value, data) => {
  if (!value) return true
  return data.name.includes(value)
}

// Eventbus, change isCollapse
$bus.on('changeIsCollapse', (obj) => {
    data.isCollapse = obj.isCollapse
    emit('setWidth', obj.width)
})
$bus.on('changeAside', (obj) => {
    if (obj.group && obj.name)
        data.activeMenu = `${obj.group}-${obj.name}`
    else
        data.activeMenu = `${obj.group}`
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

watch(filterText, (val) => {
  treeRef.value?.filter(val)
})
</script>

<template>
    <div style="display: flex; flex-direction: column; width: 100%;">
        <div class="size flex" style="display: flex; flex-direction: column; width: 100%;">
            <el-input v-if="data.showSearch && props.type !== 'stream'" class="aside-search" v-model="filterText"
                placeholder="Search" :prefix-icon="Search" clearable />
            <el-tree
                ref="treeRef"
                v-loading="loading"
                :data="data.groupLists"
                :props="defaultProps"
                :filter-node-method="filterNode"
                @node-click="openResources"
                @node-contextmenu="openOperationMenus"
            />
            <div class="resize" @mousedown="shrinkDown" title="Shrink sidebar"></div>
        </div>
        <div class="flex center add" @click="openCreateGroup" style="height: 50px; width: 100%;" v-if="!data.groupLists.length">
            <el-icon>
                <Plus />
            </el-icon>
        </div>
        <el-dialog width="25%" center :title="`${data.setGroup} group`" v-model="data.dialogGroupVisible"
            :show-close="false">
            <el-form ref="ruleForm" :rules="rules" :model="data.groupForm" label-position="left">
                <el-form-item label="group name" :label-width="data.formLabelWidth" prop="name">
                    <el-input :disabled="data.setGroup == 'edit'" v-model="data.groupForm.name" autocomplete="off">
                    </el-input>
                </el-form-item>
                <el-form-item label="group type" :label-width="data.formLabelWidth" prop="catalog">
                    <el-select v-model="data.groupForm.catalog" placeholder="please select" style="width: 100%">
                        <el-option label="Stream" value="CATALOG_STREAM"></el-option>
                        <el-option label="Measure" value="CATALOG_MEASURE"></el-option>
                        <el-option label="Unspecified(Property)" value="CATALOG_UNSPECIFIED"></el-option>
                    </el-select>
                </el-form-item>
                <el-form-item label="shard num" :label-width="data.formLabelWidth" prop="shardNum">
                    <el-input-number v-model="data.groupForm.shardNum" :min="1" />
                </el-form-item>
                <el-form-item label="segment interval unit" :label-width="data.formLabelWidth" prop="segmentIntervalUnit">
                    <el-select v-model="data.groupForm.segmentIntervalUnit" placeholder="please select" style="width: 100%">
                        <el-option label="Hour" value="UNIT_HOUR"></el-option>
                        <el-option label="Day" value="UNIT_DAY"></el-option>
                    </el-select>
                </el-form-item>
                <el-form-item label="segment interval num" :label-width="data.formLabelWidth" prop="segmentIntervalNum">
                    <el-input-number v-model="data.groupForm.segmentIntervalNum" :min="1" />
                </el-form-item>
                <el-form-item label="ttl unit" :label-width="data.formLabelWidth" prop="ttlUnit">
                    <el-select v-model="data.groupForm.ttlUnit" placeholder="please select" style="width: 100%">
                        <el-option label="Hour" value="UNIT_HOUR"></el-option>
                        <el-option label="Day" value="UNIT_DAY"></el-option>
                    </el-select>
                </el-form-item>
                <el-form-item label="ttl num" :label-width="data.formLabelWidth" prop="ttlNum">
                    <el-input-number v-model="data.groupForm.ttlNum" :min="1" />
                </el-form-item>
            </el-form>
            <div slot="footer" class="dialog-footer footer">
                <el-button @click="cancelCreateEditDialog">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{ data.setGroup }}
                </el-button>
            </div>
        </el-dialog>
        <div v-if="data.showRightMenu" class="right-menu box-shadow" :style="{ top: `${data.top}px`, left: `${data.left}px` }">
            <div v-for="m in data.operationMenus" @click="m.fn">{{ m.label }}</div>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.aside-search {
    margin: 10px;
    width: calc(100% - 20px);
}

.resize {
    cursor: col-resize;
    position: absolute;
    right: 0;
    height: 100%;
    width: 5px;
}

.right-menu {
    width: 120px;
    position: fixed;
    z-index: 9999 !important;
    padding: 10px;
    font-size: 14px;
    div {
        height: 30px;
        line-height: 30px;
        padding-left: 20px;
        cursor: pointer;
        &:hover {
            background-color: #eee;
        }

    }
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