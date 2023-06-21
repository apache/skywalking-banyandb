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
import type { FormInstance } from 'element-plus'
import { ref } from 'vue'

const ruleFormRef = ref<FormInstance>()
const data = reactive({
    tableData: [],
    dialogVisible: false,
    form: {
        name: '',
        fieldType: '',
        encodingMethod: '',
        compressionMethod: ''
    },
    fieldOperator: 'Add',
    fieldEditIndex: -1
})

const typeOptions = [
    {
        value: 'FIELD_TYPE_UNSPECIFIED',
        label: 'UNSPECIFIED'
    },
    {
        value: 'FIELD_TYPE_STRING',
        label: 'STRING'
    },
    {
        value: 'FIELD_TYPE_INT',
        label: 'INT'
    },
    {
        value: 'FIELD_TYPE_DATA_BINARY',
        label: 'DATA_BINARY'
    },
    {
        value: 'FIELD_TYPE_FLOAT',
        label: 'FLOAT'
    }
]
const encodingMethodOptions = [
    {
        value: 'ENCODING_METHOD_UNSPECIFIED',
        label: 'UNSPECIFIED'
    },
    {
        value: 'ENCODING_METHOD_GORILLA',
        label: 'GORILLA'
    }
]
const compressionMethodOptions = [
    {
        value: 'COMPRESSION_METHOD_UNSPECIFIED',
        label: 'UNSPECIFIED'
    },
    {
        value: 'COMPRESSION_METHOD_ZSTD',
        label: 'ZSTD'
    }
]
const validateName = (rule: any, value: any, callback: any) => {
    if (value == '') {
        callback(new Error('Please input the field name.'))
    } else {
        const index = data.tableData.findIndex(item => {
            return item.name == value
        })
        if (index >= 0) {
            if (data.fieldOperator == 'Edit' && data.fieldEditIndex == index) {
                return callback()
            }
            return callback(new Error('The name is exists'))
        }
        callback()
    }
}
const rules = {
    name: [
        {
            required: true, validator: validateName, trigger: 'blur'
        }
    ],
    fieldType: [
        {
            required: true, message: 'Please select the field type', trigger: 'blur'
        }
    ],
    encodingMethod: [
        {
            required: true, message: 'Please select the encoding method', trigger: 'blur'
        }
    ],
    compressionMethod: [
        {
            required: true, message: 'Please select the compression method', trigger: 'blur'
        }
    ],
}
const confirmForm = async (formEl: FormInstance | undefined) => {
    if (!formEl) return
    await formEl.validate((valid) => {
        if (valid) {
            if (data.fieldOperator == 'Add') {
                data.tableData.push(data.form)
                initForm()
            } else {
                data.tableData[data.fieldEditIndex] = data.form
                initForm()
            }
            data.dialogVisible = false
        }
    })
}
function initForm() {
    data.form = {
        name: '',
        fieldType: '',
        encodingMethod: '',
        compressionMethod: ''
    }
}
function openAddField() {
    data.fieldOperator = 'Add'
    data.dialogVisible = true
}
function openEditField(index) {
    data.form = JSON.parse(JSON.stringify(data.tableData[index]))
    data.fieldEditIndex = index
    data.fieldOperator = 'Edit'
    data.dialogVisible = true
}
function deleteTableData(index) {
    data.tableData.splice(index, 1)
}
function getFields() {
    return data.tableData
}
function setFields(value) {
    data.tableData = value
}
defineExpose({
    getFields,
    setFields
})
</script>

<template>
    <el-button size="small" type="primary" color="#6E38F7" style="margin-top: 20px;"
        @click="openAddField">Add Field</el-button>
    <el-table :data="data.tableData" style="width: 100%; margin-top: 20px;" border>
        <el-table-column label="Name" prop="name"></el-table-column>
        <el-table-column label="Field Type" prop="fieldType"></el-table-column>
        <el-table-column label="Encoding Method" prop="encodingMethod"></el-table-column>
        <el-table-column label="Compression Method" prop="compressionMethod"></el-table-column>
        <el-table-column label="Operator">
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
    <el-dialog v-model="data.dialogVisible" :close-on-click-modal="false" align-center
        :title="data.fieldOperator == 'Add' ? 'Create Field' : 'Edit Field'" width="30%">
        <el-form ref="ruleFormRef" :rules="rules" :model="data.form" label-width="180" label-position="left">
            <el-form-item label="Name" prop="name">
                <el-input v-model="data.form.name" placeholder="Input the field name"
                    :disabled="data.fieldOperator == 'Edit'"></el-input>
            </el-form-item>
            <el-form-item label="Field Type" prop="fieldType">
                <el-select v-model="data.form.fieldType" default-first-option :reserve-keyword="false"
                    placeholder="Choose field type" style="width: 100%;">
                    <el-option v-for="item in typeOptions" :key="item.value" :label="item.label" :value="item.value" />
                </el-select>
            </el-form-item>

            <el-form-item label="Encoding Method" prop="encodingMethod">
                <el-select style="width: 100%" v-model="data.form.encodingMethod" placeholder="Choose encoding method">
                    <el-option v-for="item in encodingMethodOptions" :key="item.value" :label="item.label"
                        :value="item.value" />
                </el-select>
            </el-form-item>
            <el-form-item label="Compression Method" prop="compressionMethod">
                <el-select style="width: 100%" v-model="data.form.compressionMethod"
                    placeholder="Choose compression method">
                    <el-option v-for="item in compressionMethodOptions" :key="item.value" :label="item.label"
                        :value="item.value" />
                </el-select>
            </el-form-item>
        </el-form>
        <span class="dialog-footer">
            <div style="width:100%" class="flex center">
                <el-button size="small" @click="data.dialogVisible = false; initForm()">Cancel</el-button>
                <el-button size="small" type="primary" color="#6E38F7" @click=" confirmForm(ruleFormRef) ">
                    Confirm
                </el-button>
            </div>
        </span>
    </el-dialog>
</template>
  

<style lang="scss" scoped></style>