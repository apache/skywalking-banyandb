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
import stores from '@/stores/index'
import { getStreamOrMeasure, getTableList } from '@/api/index'
import { computed, reactive } from '@vue/runtime-core'
import { getCurrentInstance } from "@vue/runtime-core"
import { ref, watch } from 'vue'

const { proxy } = getCurrentInstance()
const $loadingClose = proxy.$loadingClose
const $loadingCreate = proxy.$loadingCreate
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

const { tags } = stores()
const currentMenu = computed(() => {
    return tags.currentMenu
})
const type = computed(() => {
    return tags.currentMenu && tags.currentMenu.metadata ? tags.currentMenu.metadata.type : ''
})
const group = computed(() => {
    return tags.currentMenu && tags.currentMenu.metadata ? tags.currentMenu.metadata.group : ''
})
const name = computed(() => {
    return tags.currentMenu && tags.currentMenu.metadata ? tags.currentMenu.metadata.name : ''
})
const multipleTable = ref()
//data
let data = reactive({
    loading: false,
    total: 25,
    queryInfo: {
        pagenum: 1,
        pagesize: 6
    },
    tableTags: [],
    tableData: [],
})
// let total = 25
/* let queryInfo = {
    pagenum: 1,
    pagesize: 6
} */
// let tableTags = []
// let tableData = []
let multipleSelection = []
let checkAllFlag = false
let fileData = {}
// let loading = false
let index = 0
let param = {
    metadata: {
        group: '',
        name: '',
        createRevision: '',
        modRevision: '',
        id: ''
    },
    offset: null,
    limit: null,
    criteria: [
        {
            tagFamilyName: ''
        }
    ],
    projection: {
        tagFamilies: [
            {
                name: '',
                tags: []
            }
        ]
    }
}

watch(() => tags.currentMenu, () => {
    initData()
})
watch(() => data.tableTags, () => {
    let tagFamily = fileData.tagFamilies[index]
    let tagsList = []
    tagFamily.tags.forEach((item) => {
        tagsList.push(item.name)
    })
    param.projection.tagFamilies[0].name = tagFamily.name
    param.projection.tagFamilies[0].tags = tagsList
    param.criteria[0].tagFamilyName = tagFamily.name
})

$bus.on('checkAll', () => {
    if (!checkAllFlag) {
        data.tableData.forEach(row => {
            multipleTable.toggleRowSelection(row)
        })
        checkAllFlag = true
    }
})
$bus.on('checkNone', () => {
    multipleTable.clearSelection()
})
$bus.on('changeTagFamilies', (i) => {
    index = i
    data.tableTags = fileData.tagFamilies[i].tags
    getTable()
})
$bus.on('refresh', () => {
    initData()
})

const emit = defineEmits(['drawerRight'])

// methods
/**
 * init data
 */
function initData() {
    if (!currentMenu.value) {
        return
    }
    $loadingCreate()
    getStreamOrMeasure(type.value, group.value, name.value)
        .then((res) => {
            if (res.status == 200) {
                fileData = res.data[type.value]
                index = 0
                data.tableTags = fileData.tagFamilies[0].tags
                emit('drawerRight', fileData)
                setTagFamily()
                getTable()
            }
        })
        .finally(() => {
            $loadingClose()
        })
}
/**
 * get table data
 */
function getTable() {
    //data.loading = true
    let paramList = param
    paramList.offset = data.queryInfo.pagenum
    paramList.limit = data.queryInfo.pagesize
    paramList.metadata = fileData.metadata
    /* getTableList(paramList)
        .then((res) => {
            if (res.status == 200) {
                data.tableData = res.data.elements
            }
        })
        .finally(() => {
            data.loading = false
        }) */
}
/**
 * sort table data
 */
function sortChange(object) {
    
}
/**
 * set table tags, set navigation select
 */
function setTagFamily() {
    let tagFamilies = fileData.tagFamilies
    let tagOptions = tagFamilies.map((item, index) => {
        return { label: item.name, value: index }
    })
    $bus.emit('setOptions', tagOptions)
}
function handleSelectionChange(val) {
    multipleSelection = val;
}
function handleSizeChange() { }
function handleCurrentChange() { }
initData()
</script>

<template>
    <div>
        <el-table v-loading="data.loading" element-loading-text="loading" element-loading-spinner="el-icon-loading"
            element-loading-background="rgba(0, 0, 0, 0.8)" ref="multipleTable" max-height=700 stripe border
            :data="data.tableData" highlight-current-row tooltip-effect="dark" @selection-change="handleSelectionChange"
            empty-text="No data yet">
            <el-table-column type="selection" width="55">
            </el-table-column>
            <el-table-column type="index" label="number" width="90">
            </el-table-column>
            <el-table-column v-for="item in data.tableTags" sortable :sort-change="sortChange" :key="item.name"
                :label="item.name" :prop="item.name" show-overflow-tooltip>
            </el-table-column>
        </el-table>

        <el-pagination v-if="data.tableData.length > 0" class="margin-top-bottom" @size-change="handleSizeChange"
            @current-change="handleCurrentChange" :current-page="data.queryInfo.pagenum" :page-sizes="[6, 12, 18, 24]"
            :page-size="data.queryInfo.pagesize" layout="total, sizes, prev, pager, next, jumper" :total="data.total">
        </el-pagination>
    </div>
</template>