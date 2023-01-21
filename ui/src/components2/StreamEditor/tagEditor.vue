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

<script lang="ts" setup>
import { reactive } from "@vue/reactivity"
import type { TableColumnCtx } from 'element-plus'
const data = reactive({
    tableData: [],
    dialogVisible: false,
    form: {
        tagFamily: '',
        tag: '',
        type: 'TAG_TYPE_INT',
        indexedOnly: false
    }
})
const tagFamilies = [
    
]
const typeOptions = [
    {
        value: 'TAG_TYPE_INT',
        label: 'TAG_TYPE_INT'
    },
    {
        value: 'TAG_TYPE_STRING',
        label: 'TAG_TYPE_STRING'
    },
    {
        value: 'TAG_TYPE_INT_ARRAY',
        label: 'TAG_TYPE_INT_ARRAY'
    },
    {
        value: 'TAG_TYPE_STRING_ARRAY',
        label: 'TAG_TYPE_STRING_ARRAY'
    },
    {
        value: 'TAG_TYPE_DATA_BINARY',
        label: 'TAG_TYPE_DATA_BINARY'
    },
    {
        value: 'TAG_TYPE_ID',
        label: 'TAG_TYPE_ID'
    },
    {
        value: 'TAG_TYPE_UNSPECIFIED',
        label: 'TAG_TYPE_UNSPECIFIED'
    }
]
const validateTag = (rule: any, value: any, callback: any) => {
    if (value == '') {
        callback(new Error('Please input the tag.'))
    } else {
        const index = data.tableData.findIndex(item => {
            return item.tag == value
        })
        if (index >= 0) {
            return callback(new Error('The tag is exists'))
        }
        callback()
    }
}
const rules = {
    tagFamily: [
        {
            required: true, message: 'Please input the tag family', trigger: 'blur'
        }
    ],
    tag: [
        {
            required: true, validator: validateTag, trigger: 'blur'
        }
    ]
}
interface User {
    tagFamily: string
    tag: string
    type: string
    indexedOnly: Boolean
}
interface SpanMethodProps {
    row: User
    column: TableColumnCtx<User>
    rowIndex: number
    columnIndex: number
}
data.tableData = [
    {
        tagFamily: 'searchOnly',
        tag: 'ID',
        type: 'String',
        indexedOnly: true
    },
    {
        tagFamily: 'searchOnly',
        tag: 'name',
        type: 'String',
        indexedOnly: true
    },
    {
        tagFamily: 'searchOnly2',
        tag: 'ID2',
        type: 'String',
        indexedOnly: true
    },
    {
        tagFamily: 'searchOnly2',
        tag: 'name2',
        type: 'String',
        indexedOnly: true
    },
    {
        tagFamily: 'searchOnly2',
        tag: 'type2',
        type: 'String',
        indexedOnly: true
    }
]
const objectSpanMethod = ({
    row, column, rowIndex, columnIndex
}: SpanMethodProps) => {
    if (columnIndex === 0) {
        const tagFamily = data.tableData[rowIndex].tagFamily
        const index = data.tableData.findIndex(item => {
            return item.tagFamily == tagFamily
        })
        if (rowIndex == index) {
            let len = 1
            for (let i = index + 1; i < data.tableData.length; i++) {
                if (data.tableData[i].tagFamily !== tagFamily) {
                    break
                }
                len++
            }
            return {
                rowspan: len,
                colspan: 1
            }
        }
        return {
            rowspan: 0,
            colspan: 0
        }
    }
}
function addTagFamily() {
    data.dialogVisible = true
}
</script>

<template>
    <el-button size="small" type="primary" color="#6E38F7" style="margin-top: 20px;"
        @click="addTagFamily">Add</el-button>
    <el-table :data="data.tableData" :span-method="objectSpanMethod" style="width: 100%; margin-top: 20px;" border>
        <el-table-column label="Tag Family" prop="tagFamily"></el-table-column>
        <el-table-column label="Tag" prop="tag"></el-table-column>
        <el-table-column label="Type" prop="type"></el-table-column>
        <el-table-column label="IndexedOnly" prop="indexedOnly"></el-table-column>
        <el-table-column label="Operator">
            <template #default="scope">
                <el-button link type="primary" style="color:#409EFF !important">Edit</el-button>
                <el-button link type="danger">Delete</el-button>
            </template>
        </el-table-column>
    </el-table>
    <el-dialog v-model="data.dialogVisible" :close-on-click-modal="false" align-center title="Create Tag Family"
        width="30%">
        <el-form ref="ruleForm" :rules="rules" :model="data.form" label-width="120" label-position="left">
            <el-form-item label="Tag Family" prop="tagFamily">
                <el-input v-model="data.form.tagFamily"></el-input>
            </el-form-item>
            <el-form-item label="Tag" prop="tag">
                <el-input v-model="data.form.tag"></el-input>
            </el-form-item>
            <el-form-item label="Type" prop="type">
                <el-select style="width: 100%" v-model="data.form.type" class="m-2" placeholder="Select" size="small">
                    <el-option v-for="item in typeOptions" :key="item.value" :label="item.label" :value="item.value" />
                </el-select>
            </el-form-item>
            <el-form-item label="IndexedOnly" prop="indexedOnly">
                <el-switch v-model="data.form.indexedOnly" />
            </el-form-item>
        </el-form>
        <span class="dialog-footer">
            <div style="width:100%" class="flex center">
                <el-button size="small" @click="data.dialogVisible = false">Cancel</el-button>
                <el-button size="small" type="primary" color="#6E38F7" @click="data.dialogVisible = false">
                    Confirm
                </el-button>
            </div>
        </span>
    </el-dialog>
</template>
  

<style lang="scss" scoped>

</style>