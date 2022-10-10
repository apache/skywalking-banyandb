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
import { computed } from '@vue/runtime-core'
import { getCurrentInstance } from "@vue/runtime-core"
import { ref } from 'vue'

const { proxy } = getCurrentInstance()
const $loadingClose = proxy.$loadingClose
const $loadingCreate = proxy.$loadingCreate
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

const { tags } = stores()
const currentMenu = computed(() => {
    return tags.currentMenu
})
const type = computed(() => {
    return currentMenu.metadata.type
})
const group = computed(() => {
    return currentMenu.metadata.group
})
const name = computed(() => {
    return currentMenu.metadata.name
})
const multipleTable = ref()

//data
let total = 25
let queryInfo = {
    pagenum: 1,
    pagesize: 6
}
let tableTags = []
let tableData = []
let multipleSelection = []
let checkAllFlag = false
let fileData = {}
let loading = false
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

watch(() => currentMenu, () => {
    initData()
})
watch(() => tableTags, () => {
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
        tableData.forEach(row => {
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
    tableTags = fileData.tagFamilies[i].tags
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
    $loadingCreate()
    getStreamOrMeasure(type, group, name)
        .then((res) => {
            if (res.status == 200) {
                fileData = res.data[type]
                index = 0
                tableTags = fileData.tagFamilies[0].tags
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
    loading = true
    let param = param
    param.offset = queryInfo.pagenum
    param.limit = queryInfo.pagesize
    param.metadata = fileData.metadata
    getTableList(param)
        .then((res) => {
            if (res.status == 200) {
                tableData = res.data.elements
            }
        })
        .finally(() => {
            loading = false
        })
}
/**
 * sort table data
 */
function sortChange(object) {
    console.log(object);
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
</script>

<template>
    <div>
        <el-table v-loading="loading" element-loading-text="loading" element-loading-spinner="el-icon-loading"
            element-loading-background="rgba(0, 0, 0, 0.8)" ref="multipleTable" max-height=700 stripe :data="tableData"
            highlight-current-row tooltip-effect="dark" @selection-change="handleSelectionChange" border
            empty-text="No data yet">
            <el-table-column type="selection" width="55">
            </el-table-column>
            <el-table-column type="index" label="number" width="80">
            </el-table-column>
            <el-table-column v-for="item in tableTags" sortable :sort-change="sortChange" :key="item.name"
                :label="item.name" :prop="item.name">
            </el-table-column>
        </el-table>

        <el-pagination v-if="tableData.length > 0" class="margin-top-bottom" @size-change="handleSizeChange"
            @current-change="handleCurrentChange" :current-page="queryInfo.pagenum" :page-sizes="[6, 12, 18, 24]"
            :page-size="queryInfo.pagesize" layout="total, sizes, prev, pager, next, jumper" :total="total">
        </el-pagination>
    </div>
</template>