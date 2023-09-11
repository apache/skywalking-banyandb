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
import { applyProperty } from '@/api';
import { reactive, ref } from 'vue';
import { getCurrentInstance } from '@vue/runtime-core'
import TagEditor from './TagEditor.vue';
import { ElMessage } from 'element-plus';
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose
const showDialog = ref(false)
const title = ref('')
const tagEditorRef = ref()
const ruleForm = ref()
const formData = reactive({
    strategy: 'STRATEGY_UNSPECIFIED',
    group: '',
    name: '',
    containerID: '',
    modRevision: 0,
    createRevision: 0,
    id: '',
    tags: []
})
const validateTags = (rule, value, callback) => {
    if (value.length <= 0) {
        callback(new Error('Please add the tags'))
    } else {
        callback()
    }
}
const rules = {
    strategy: [{ required: true, message: 'please select the apply method.', trigger: 'blur' }],
    group: [{ required: true, message: 'Please enter the group.', trigger: 'blur' }],
    name: [{ required: true, message: 'Please enter the name.', trigger: 'blur' }],
    containerID: [{ required: true, message: 'Please enter the container id.', trigger: 'blur' }],
    modRevision: [{ required: true, message: 'Please enter the container Mod Revision.', trigger: 'blur' }],
    createRevision: [{ required: true, message: 'Please enter the container Create Revision.', trigger: 'blur' }],
    id: [{ required: true, message: 'Please enter the ID.', trigger: 'blur' }],
    tags: [{ required: true, validator: validateTags, trigger: 'blur' }],
}
const strategyGroup = [
    { label: 'STRATEGY_UNSPECIFIED', value: 'STRATEGY_UNSPECIFIED' },
    { label: 'STRATEGY_MERGE', value: 'STRATEGY_MERGE' },
    { label: 'STRATEGY_REPLACE', value: 'STRATEGY_REPLACE' }
]
const formConfig = [
    { label: 'Strategy', prop: 'strategy', type: 'select', selectGroup: strategyGroup },
    { label: 'Container Group', prop: 'group', type: 'input', disabled: true },
    { label: 'Container Name', prop: 'name', type: 'input' },
    { label: 'Container ID', prop: 'containerID', type: 'input' },
    { label: 'Container ModRevision', prop: 'modRevision', type: 'number' },
    { label: 'Container CreateRevision', prop: 'createRevision', type: 'number' },
    { label: 'ID', prop: 'id', type: 'input' },
]
let promiseResolve

const initData = () => {
    formData.strategy = 'STRATEGY_UNSPECIFIED'
    formData.group = ''
    formData.name = ''
    formData.containerID = ''
    formData.modRevision = 0
    formData.createRevision = 0
    formData.id = ''
    formData.tags = []
}
const closeDialog = () => {
    showDialog.value = false
    initData()
}
const openEditTag = (index) => {
    tagEditorRef.value.openDialog(formData.tags[index])
        .then(res => {
            formData.tags[index].key = res.key
            formData.tags[index].value = res.value
        })
}
const deleteTag = (index) => {
    formData.tags.splice(index, 1)
}
const openAddTag = () => {
    tagEditorRef.value.openDialog()
        .then(res => {
            formData.tags.push(res)
        })
}
const confirmApply = async () => {
    if (!ruleForm.value) return
    await ruleForm.value.validate((valid) => {
        if (valid) {
            $loadingCreate()
            const param = {
                strategy: formData.strategy,
                property: {
                    metadata: {
                        container: {
                            createRevision: formData.createRevision,
                            group: formData.group,
                            id: formData.containerID,
                            modRevision: formData.modRevision,
                            name: formData.name
                        },
                        id: formData.id
                    },
                    tags: formData.tags.map(item => {
                        return {
                            key: item.key,
                            value: JSON.parse(item.value)
                        }
                    })
                }
            }
            applyProperty(formData.group, formData.name, formData.id, param)
                .then(res => {
                    if (res.status == 200) {
                        ElMessage({
                            message: 'successed',
                            type: "success",
                            duration: 5000
                        })
                        showDialog.value = false
                        promiseResolve()
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
    })
}
const openDialog = (edit, data) => {
    showDialog.value = true
    if (edit === true) {
        title.value = 'Edit Property'
    } else {
        title.value = 'Apply Property'
    }
    formData.group = data?.group || ''
    formData.name = data?.name || ''
    formData.containerID = data?.containerID || ''
    formData.modRevision = data?.modRevision || 0
    formData.createRevision = data?.createRevision || 0
    formData.id = data?.id || ''
    formData.tags = JSON.parse(JSON.stringify(data?.tags || []))
    return new Promise((resolve) => {
        promiseResolve = resolve
    })
}
defineExpose({
    openDialog
})
</script>

<template>
    <el-dialog v-model="showDialog" :title="title" width="50%">
        <el-form ref="ruleForm" :rules="rules" :model="formData" label-position="left">
            <el-form-item v-for="item in formConfig" :key="item.prop" :label="item.label" :prop="item.prop"
                label-width="200">
                <el-select v-if="item.type === 'select'" v-model="formData[item.prop]" placeholder="please select"
                    style="width: 100%;">
                    <el-option v-for="option in item.selectGroup" :key="option.value" :label="option.label"
                        :value="option.value"></el-option>
                </el-select>
                <el-input v-if="item.type === 'input'" v-model="formData[item.prop]" :disabled="item.disabled"
                    autocomplete="off"></el-input>
                <el-input-number v-if="item.type === 'number'" v-model="formData[item.prop]" :min="0"></el-input-number>
            </el-form-item>
            <el-form-item label="Tags" prop="tags" label-width="200">
                <el-button size="small" type="primary" color="#6E38F7" @click="openAddTag">Add Tag</el-button>
                <el-table style="margin-top: 10px;" :data="formData.tags" border>
                    <el-table-column label="Key" prop="key"></el-table-column>
                    <el-table-column label="Value" prop="value"></el-table-column>
                    <el-table-column label="Operator" width="150">
                        <template #default="scope">
                            <el-button link type="primary" @click.prevent="openEditTag(scope.$index)"
                                style="color: var(--color-main); font-weight: bold;">Edit</el-button>
                            <el-popconfirm @confirm="deleteTag(scope.$index)" title="Are you sure to delete this?">
                                <template #reference>
                                    <el-button link type="danger" style="color: red;font-weight: bold;">Delete</el-button>
                                </template>
                            </el-popconfirm>
                        </template>
                    </el-table-column>
                </el-table>
            </el-form-item>
        </el-form>
        <template #footer>
            <span class="dialog-footer footer">
                <el-button @click="closeDialog">Cancel</el-button>
                <el-button type="primary" @click="confirmApply">
                    Confirm
                </el-button>
            </span>
        </template>
    </el-dialog>
    <TagEditor ref="tagEditorRef"></TagEditor>
</template>

<style lang="scss" scoped>
.footer {
    width: 100%;
    display: flex;
    justify-content: center;
}
</style>