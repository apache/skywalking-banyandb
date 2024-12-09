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
const showDialog = ref(false)
const title = ref('')
const valueData = reactive({
    data: '',
    formattedData: ''
})

const numSpaces = 2
const initData = () => {
    valueData.data = temp
}
const closeDialog = () => {
    showDialog.value = false
    initData()
}

const downloadValue = () => {
    const dataBlob = new Blob([valueData.formattedData], { type: 'text/JSON' })
    var a = document.createElement('a');
    a.download = 'value.txt';
    a.href = URL.createObjectURL(dataBlob);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

const openDialog = (data) => {
    title.value = "Value of key " + data.key
    showDialog.value = true
    valueData.data = data.value
    valueData.formattedData = JSON.stringify(JSON.parse(valueData.data), null, numSpaces)
}
defineExpose({
    openDialog
})
</script>

<template>
    <el-dialog v-model="showDialog" :title="title">
        <pre>{{valueData.formattedData}}</pre>
        <template #footer>
            <span class="dialog-footer footer">
                <el-button @click="closeDialog">Cancel</el-button>
                <el-button type="primary" @click.prevent="downloadValue()">
                    Download
                </el-button>
            </span>
        </template>
    </el-dialog>
</template>

<style lang="scss" scoped>
.footer {
    width: 100%;
    display: flex;
    justify-content: center;
}
</style>