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
import { getPropertyByGroup, deleteProperty } from '@/api/index';
import { watch, getCurrentInstance } from "@vue/runtime-core";
import { useRoute } from 'vue-router';
import { ElMessage } from 'element-plus';
import { onMounted, reactive, ref } from 'vue';
import { RefreshRight } from '@element-plus/icons-vue';
import PropertyEditror from './PropertyEditror.vue';

const { proxy } = getCurrentInstance()
// Loading
const route = useRoute()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose
const propertyEditorRef = ref()
const data = reactive({
    group: "",
    tableData: []
})
const getProperty = () => {
    $loadingCreate()
    const group = route.params.group
    getPropertyByGroup(group)
        .then(res => {
            if (res.status == 200 && group == route.params.group) {
                data.tableData = res.data.property.map(item => {
                    item.tags.forEach(tag => {
                        tag.value = JSON.stringify(tag.value)
                    })
                    return item
                })
            }
        })
        .catch((err) => {
            ElMessage({
                message: 'An error occurred while obtaining group data. Please refresh and try again. Error: ' + err,
                type: "error",
                duration: 3000
            })
        })
        .finally(() => {
            $loadingClose()
        })
}
const openEditField = (index) => {
    const item = data.tableData[index]
    const param = {
        group: item.metadata.container.group,
        name: item.metadata.container.name,
        containerID: item.metadata.container.id,
        modRevision: item.metadata.container.modRevision,
        createRevision: item.metadata.container.createRevision,
        id: item.metadata.id,
        tags: JSON.parse(JSON.stringify(item.tags))
    }
    propertyEditorRef?.value.openDialog(true, param)
        .then(() => {
            getProperty()
        })
}
const openAddProperty = () => {
    let dataForm = {
        group: data.group
    }
    propertyEditorRef?.value.openDialog(false, dataForm)
        .then(() => {
            getProperty()
        })
}
const deleteTableData = (index) => {
    const item = data.tableData[index]
    $loadingCreate()
    deleteProperty(item.metadata.container.group, item.metadata.container.name, item.metadata.id, item.tags)
        .then((res) => {
            if (res.status == 200) {
                ElMessage({
                    message: 'successed',
                    type: "success",
                    duration: 5000
                })
                getProperty()
            }
        })
        .catch(err => {
            ElMessage({
                message: 'Please refresh and try again. Error: ' + err,
                type: "error",
                duration: 3000
            })
        })
        .finally(() => {
            $loadingClose()
        })
}
watch(() => route, () => {
    data.group = route.params.group
    data.tableData = []
    getProperty()
}, {
    deep: true,
    immediate: true
})
onMounted(() => {
    getProperty()
})
</script>
<template>
    <div>
        <el-card shadow="always">
            <template #header>
                <div class="flex">
                    <span class="text-bold">Group：</span>
                    <span style="margin-right: 20px;">{{ data.group }}</span>
                    <span class="text-bold">Operation：</span>
                    <span>Read</span>
                </div>
            </template>
            <div class="button-group-operator">
                <el-button size="small" type="primary" color="#6E38F7" @click="openAddProperty">Apply
                    Property</el-button>
                <el-button size="small" :icon="RefreshRight" @click="getProperty" plain></el-button>
            </div>

            <el-table :data="data.tableData" style="width: 100%; margin-top: 20px;" border>
                <el-table-column label="Container">
                    <el-table-column label="Group" prop="metadata.container.group" width="100"></el-table-column>
                    <el-table-column label="Name" prop="metadata.container.name" width="120"></el-table-column>
                    <el-table-column label="ID" prop="metadata.container.id" width="100"></el-table-column>
                    <el-table-column label="ModRevision" prop="metadata.container.modRevision"
                        width="120"></el-table-column>
                    <el-table-column label="CreateRevision" prop="metadata.container.createRevision"
                        width="140"></el-table-column>
                </el-table-column>
                <el-table-column label="ID" prop="metadata.id" width="150"></el-table-column>
                <el-table-column label="Tags">
                    <template #default="scope">
                        <el-table :data="scope.row.tags">
                            <el-table-column label="Key" prop="key" width="150"></el-table-column>
                            <el-table-column label="Value" prop="value"></el-table-column>
                        </el-table>
                    </template>
                </el-table-column>
                <el-table-column label="Operator" width="150">
                    <template #default="scope">
                        <el-button link type="primary" @click.prevent="openEditField(scope.$index)"
                            style="color: var(--color-main); font-weight: bold;">Edit</el-button>
                        <el-popconfirm @confirm="deleteTableData(scope.$index)" title="Are you sure to delete this?">
                            <template #reference>
                                <el-button link type="danger" style="color: red;font-weight: bold;">Delete</el-button>
                            </template>
                        </el-popconfirm>
                    </template>
                </el-table-column>
            </el-table>
        </el-card>
        <PropertyEditror ref="propertyEditorRef"></PropertyEditror>
    </div>
</template>
<style lang="scss" scoped>
:deep(.el-card) {
    margin: 15px;
}

.button-group-operator {
    display: flex;
    flex-direction: row;
    justify-content: space-between;
}
</style>