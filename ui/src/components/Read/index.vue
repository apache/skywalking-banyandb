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
import { reactive, ref } from 'vue';
import { useRoute } from 'vue-router'
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { getStreamOrMeasure, getTableList } from '@/api/index'
import { Search, RefreshRight } from '@element-plus/icons-vue'
import { jsonToYaml, yamlToJson } from '@/utils/yaml'
import CodeMirror from '@/components/CodeMirror/index.vue'
import { ElMessage } from 'element-plus'
import { computed } from '@vue/runtime-core'

const route = useRoute()

const yamlRef = ref()

// Loading
const { proxy } = getCurrentInstance()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose
const tagType = {
    'TAG_TYPE_UNSPECIFIED': 'null',
    'TAG_TYPE_STRING': 'str',
    'TAG_TYPE_INT': 'int',
    'TAG_TYPE_STRING_ARRAY': 'strArray',
    'TAG_TYPE_INT_ARRAY': 'intArray',
    'TAG_TYPE_DATA_BINARY': 'binaryData'
}
const fieldTypes = {
    'FIELD_TYPE_UNSPECIFIED': 'null',
    'FIELD_TYPE_STRING': 'str',
    'FIELD_TYPE_INT': 'int',
    'FIELD_TYPE_FLOAT': 'float',
    'FIELD_TYPE_DATA_BINARY': 'binaryData'
}
const shortcuts = [
    {
        text: 'Last 15 minutes',
        value: () => {
            const end = new Date()
            const start = new Date()
            start.setTime(start.getTime() - 900 * 1000)
            return [start, end]
        }
    },
    {
        text: 'Last week',
        value: () => {
            const end = new Date()
            const start = new Date()
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 7)
            return [start, end]
        },
    },
    {
        text: 'Last month',
        value: () => {
            const end = new Date()
            const start = new Date()
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 30)
            return [start, end]
        },
    },
    {
        text: 'Last 3 months',
        value: () => {
            const end = new Date()
            const start = new Date()
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 90)
            return [start, end]
        },
    },
]
const param = {
    metadata: {
        group: '',
        name: '',
        createRevision: '',
        modRevision: '',
        id: ''
    },
    offset: null,
    limit: null,
    //criteria: {},
    projection: {
        tagFamilies: [
            {
                name: '',
                tags: []
            }
        ]
    }
}
const data = reactive({
    fields: [],
    tableFields: [],
    handleFields: "",
    group: route.params.group,
    name: route.params.name,
    type: route.params.type,
    tagFamily: 0,
    tagFamilies: [],
    options: [],
    resourceData: null,
    timeValue: null,
    loading: false,
    total: 100,
    /* queryInfo: {
        pagenum: 1,
        pagesize: 100
    }, */
    tableTags: [],
    tableData: [],
    code: null,
    codeStorage: []
})
const tableHeader = computed(() => {
    return data.tableTags.concat(data.tableFields)
})
watch(() => data.handleFields, () => {
    if (data.handleFields.length > 0) {
        data.tableTags = data.tableTags.map(item => {
            item.label = `${data.options[data.tagFamily].label}.${item.name}`
            return item
        })
    } else {
        data.tableTags = data.tableTags.map(item => {
            item.label = item.name
            return item
        })
    }
})
watch(() => route, () => {
    data.group = route.params.group
    data.name = route.params.name
    data.type = route.params.type
    data.tableData = []
    data.tableTags = []
    data.tableFields = []
    data.fields = []
    data.handleFields = ""

    if (data.group && data.name && data.type) {
        initCode()
        initData()
    }
}, {
    immediate: true,
    deep: true
})
watch(() => data.code, () => {
    let index = data.codeStorage.findIndex(item => {
        return item.params.group == route.params.group && item.params.name == route.params.name
    })
    if (index >= 0) {
        data.codeStorage[index].params.code = data.code
    } else {
        route.params.code = data.code
        data.codeStorage.push(JSON.parse(JSON.stringify(route)))
    }
})
function initCode() {
    let index = data.codeStorage.findIndex(item => {
        return item.params.group == route.params.group && item.params.name == route.params.name
    })
    if (index >= 0) {
        data.code = data.codeStorage[index].params.code
    } else {
        let timeRange = {
            timeRange: {
                begin: new Date(new Date() - 15),
                end: new Date()
            }
        }
        timeRange.timeRange.begin.setTime(timeRange.timeRange.begin.getTime() - 900 * 1000)
        timeRange = jsonToYaml(timeRange).data
        data.code = ref(
            `${timeRange}offset: 1
limit: 10
orderBy:
  indexRuleName: ""
  sort: SORT_UNSPECIFIED
`)
    }
}
function changeCode(name, value) {
    let code = yamlToJson(data.code).data
    code[name] = value
    data.code = jsonToYaml(code).data
}
function initData() {
    $loadingCreate()
    getStreamOrMeasure(data.type, data.group, data.name)
        .then(res => {
            if (res.status == 200) {
                data.resourceData = res.data[data.type]
                data.tableTags = res.data[data.type].tagFamilies[0].tags.map(item => {
                    item.label = item.name
                    return item
                })
                data.options = res.data[data.type].tagFamilies.map((item, index) => {
                    return { label: item.name, value: index }
                })
                data.tagFamily = 0
                data.fields = res.data[data.type].fields ? res.data[data.type].fields : []
                handleCodeData()
            }
        })
        .finally(() => {
            $loadingClose()
        })
}
function getTableData() {
    data.tableData = []
    data.loading = true
    setTableParam()
    let paramList = JSON.parse(JSON.stringify(param))
    if (data.type == 'measure') {
        paramList.tagProjection = paramList.projection
        if (data.handleFields.length > 0) {
            paramList.fieldProjection = {
                names: data.handleFields
            }
        }
        delete paramList.projection
    }
    /* paramList.offset = data.queryInfo.pagenum
    paramList.limit = data.queryInfo.pagesize */
    paramList.metadata = data.resourceData.metadata
    getTableList(paramList, data.type)
        .then((res) => {
            if (res.status == 200) {
                if (data.type == 'stream') {
                    setTableData(res.data.elements)
                } else {
                    setTableData(res.data.dataPoints)
                }

            }
        })
        .catch(() => {
            data.loading = false
        })
}
function setTableData(elements) {
    const tags = data.resourceData.tagFamilies[data.tagFamily].tags
    const tableFields = data.tableFields
    data.tableData = elements.map(item => {
        let dataItem = {}
        item.tagFamilies[0].tags.forEach(tag => {
            const index = tags.findIndex(item => item.name == tag.key)
            const type = tags[index].type
            if (tag.value[tagType[type]] == null) {
                return dataItem[tag.key] = 'Null'
            }
            dataItem[tag.key] = Object.hasOwnProperty.call(tag.value[tagType[type]], 'value') ? tag.value[tagType[type]].value : tag.value[tagType[type]]
        })
        if (data.type == 'measure' && tableFields.length > 0) {
            item.fields.forEach(field => {
                const name = field.name
                const fieldType = tableFields.filter(tableField => {
                    return tableField.name == name
                })[0].fieldType || ''
                if (field.value[fieldTypes[fieldType]] == null) {
                    return dataItem[name] = 'Null'
                }
                dataItem[name] = Object.hasOwnProperty.call(field.value[fieldTypes[fieldType]], 'value') ? field.value[fieldTypes[fieldType]].value : field.value[fieldTypes[fieldType]]
            })
        }

        dataItem.timestamp = item.timestamp
        return dataItem
    })
    data.loading = false
}
function setTableParam() {
    let tagFamily = data.resourceData.tagFamilies[data.tagFamily]
    let tagsList = []
    tagFamily.tags.forEach((item) => {
        tagsList.push(item.name)
    })
    param.projection.tagFamilies[0].name = tagFamily.name
    param.projection.tagFamilies[0].tags = tagsList
    //param.criteria[0].tagFamilyName = tagFamily.name
}
function changeTagFamilies() {
    data.tableTags = data.resourceData.tagFamilies[data.tagFamily].tags.map(item => {
        item.label = item.name
        if (data.handleFields.length > 0) {
            item.label = `${data.options[data.tagFamily].label}.${item.name}`
        }
        return item
    })
    getTableData()
}
function handleCodeData() {
    const json = yamlToJson(data.code).data
    param.offset = json.offset ? json.offset : param.offset
    param.limit = json.limit ? json.limit : param.limit
    /* json.orderBy ? param.orderBy = json.orderBy : null */
    delete param.timeRange
    if (json.timeRange && !isNaN(Date.parse(json.timeRange.begin)) && !isNaN(Date.parse(json.timeRange.end))) {
        data.timeValue = [json.timeRange.begin, json.timeRange.end]
        param.timeRange = json.timeRange
    } else if (json.timeRange.begin || json.timeRange.end) {
        data.timeValue = []
        ElMessage({
            dangerouslyUseHTMLString: true,
            showClose: true,
            message: 'Warning: Wrong time type',
            type: 'warning',
            duration: 5000
        })
    } else {
        data.timeValue = []
    }
    json.orderBy ? param.orderBy = json.orderBy : delete param.orderBy
    getTableData()
}
function searchTableData() {
    yamlRef.value.checkYaml(data.code).then(() => {
        handleCodeData()
    })
        .catch((err) => {
            ElMessage({
                dangerouslyUseHTMLString: true,
                showClose: true,
                message: `<div>${err.message}</div>`,
                type: 'error',
                duration: 5000
            })
        })
}
function changeDatePicker() {
    let json = yamlToJson(data.code)
    if (!json.data.hasOwnProperty('timeRange')) {
        json.data.timeRange = {
            begin: "",
            end: ""
        }
    }
    json.data.timeRange.begin = data.timeValue ? data.timeValue[0] : null
    json.data.timeRange.end = data.timeValue ? data.timeValue[1] : null
    data.code = jsonToYaml(json.data).data
}
function changeFields() {
    data.tableFields = data.handleFields.map(fieldName => {
        let item = data.fields.filter(field => {
            return field.name == fieldName
        })[0]
        item.label = item.name
        return item
    })
    getTableData()
}
</script>

<template>
    <div>
        <el-card shadow="always">
            <template #header>
                <div class="flex">
                    <span class="text-bold">Group：</span>
                    <span style="margin-right: 20px;">{{ data.group }}</span>
                    <span class="text-bold">Name：</span>
                    <span style="margin-right: 20px;">{{ data.name }}</span>
                    <span class="text-bold">Operation：</span>
                    <span>Read</span>
                </div>
            </template>
            <el-row>
                <el-col :span="12">
                    <div class="flex align-item-center" style="height: 40px; width: 100%;">
                        <el-select v-model="data.tagFamily" @change="changeTagFamilies" filterable
                            placeholder="Please select">
                            <el-option v-for="item in data.options" :key="item.value" :label="item.label"
                                :value="item.value">
                            </el-option>
                        </el-select>
                        <el-select v-if="data.type == 'measure'" v-model="data.handleFields" collapse-tags
                            style="margin: 0 0 0 10px; width: 400px;" @change="changeFields" filterable multiple
                            placeholder="Please select Fields">
                            <el-option v-for="item in data.fields" :key="item.name" :label="item.name" :value="item.name">
                            </el-option>
                        </el-select>
                        <el-date-picker @change="changeDatePicker" style="margin: 0 10px 0 10px" v-model="data.timeValue"
                            type="datetimerange" :shortcuts="shortcuts" range-separator="to" start-placeholder="begin"
                            end-placeholder="end" align="right">
                        </el-date-picker>
                        <el-button size="normal" :icon="Search" @click="searchTableData" color="#6E38F7" plain></el-button>
                    </div>
                </el-col>
                <el-col :span="12">
                    <div class="flex align-item-center justify-end" style="height: 30px;">
                        <el-button size="normal" :icon="RefreshRight" @click="getTableData" plain></el-button>
                    </div>
                </el-col>
            </el-row>
            <CodeMirror ref="yamlRef" v-model="data.code" mode="yaml" style="height: 200px" :lint="true" :readonly="false">
            </CodeMirror>
        </el-card>
        <el-card shadow="always">
            <el-table v-loading="data.loading" element-loading-text="loading" element-loading-spinner="el-icon-loading"
                element-loading-background="rgba(0, 0, 0, 0.8)" ref="multipleTable" stripe border highlight-current-row
                tooltip-effect="dark" empty-text="No data yet" @selection-change="handleSelectionChange"
                :data="data.tableData">
                <el-table-column type="selection" width="55">
                </el-table-column>
                <el-table-column type="index" label="number" width="90">
                </el-table-column>
                <el-table-column label="timestamp" width="260" key="timestamp" prop="timestamp"></el-table-column>
                <el-table-column v-for="item in tableHeader" sortable :sort-change="sortChange" :key="item.name"
                    :label="item.label" :prop="item.name" show-overflow-tooltip>
                </el-table-column>
            </el-table>
        </el-card>
    </div>
</template>

<style lang="scss" scoped>
::v-deep {
    .el-card {
        margin: 15px;
    }
}
</style>