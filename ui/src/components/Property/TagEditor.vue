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
import { reactive, ref } from 'vue'

const showDialog = ref(false)
//const editValue = ref(false)
const ruleForm = ref()
const title = ref('')
const formData = reactive({
    key: '',
    value: ''
})
let promiseResolve
const initData = () => {
    formData.key = ''
    formData.value = ''
}
const closeDialog = () => {
    initData()
    showDialog.value = false
}
const confirmApply = async () => {
    if (!ruleForm.value) return
    await ruleForm.value.validate((valid) => {
        if (valid) {
            promiseResolve(JSON.parse(JSON.stringify(formData)))
            initData()
            showDialog.value = false
        }
    })
}
const openDialog = (data) => {
    if (data) {
        formData.key = data.key
        formData.value = data.value
        title.value = 'Edit Tag'
    } else {
        title.value = 'Add Tag'
    }
    showDialog.value = true
    return new Promise((resolve) => {
        promiseResolve = resolve
    })
}
defineExpose({
    openDialog
})
</script>

<template>
    <el-dialog v-model="showDialog" :title="title" width="30%">
        <el-form ref="ruleForm" :model="formData" label-position="left">
            <el-form-item label="Key" prop="key" required label-width="150">
                <el-input v-model="formData.key" autocomplete="off"></el-input>
            </el-form-item>
            <el-form-item label="Value" prop="value" required label-width="150">
                <el-input v-model="formData.value" type="textarea" autocomplete="off"></el-input>
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
</template>

<style scoped lang="scss">
.footer {
    width: 100%;
    display: flex;
    justify-content: center;
}
</style>