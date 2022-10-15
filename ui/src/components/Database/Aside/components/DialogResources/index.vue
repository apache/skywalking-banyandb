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
import { watch, ref, reactive } from 'vue'
const rule = {
    name: [
        {
            required: true, message: 'Please enter the name', trigger: 'blur'
        }
    ],
    group: [
        {
            required: true, message: 'Please enter the group', trigger: 'blur'
        }
    ]
}
const props = defineProps({
    visible: {
        type: Boolean,
        default: false,
        required: true
    },
    operation: {
        type: String,
        default: 'create',
        required: true
    },
    type: {
        type: String,
        default: 'stream',
        required: true
    },
    group: {
        type: String,
        required: true
    }
})

let data = reactive({
    dialogVisible: false,
    form: {
        metadata: {
            group: '',
            name: ''
        }
    }
})
let rules = rule
let tableData = [{
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}, {
    tagFamilies: 'searchable',
    name: 'stream-ids',
    type: 'String'
}]
const ruleForm = ref()
const emit = defineEmits(['confirm', 'cancel'])

watch(() => props.visible, () => {
    data.dialogVisible = props.visible
})
watch(() => props.group, () => {
    data.form.metadata.group = props.group
})

function confirmForm() {
    ruleForm.value.validate((valid) => {
        if (valid) {
            emit('confirm', data.form)
        }
    })
}
function cancelForm() {
    emit('cancel')
}
function objectSpanMethod({ row, column, rowIndex, columnIndex }) {
    if (columnIndex === 0) {

        if (rowIndex % 2 === 0) {
            return {
                rowspan: 2,
                colspan: 1
            };
        } else {
            return {
                rowspan: 0,
                colspan: 0
            };
        }
    }
}
</script>

<template>
    <div>
        <el-dialog width="25%" center :title="`${operation} ${type}`" @close="cancelForm" v-model="data.dialogVisible">
            <el-form ref="ruleForm" :rules="rules" :model="data.form.metadata" label-position="left">
                <el-form-item label="group" label-width="100px" prop="group">
                    <el-input disabled v-model="data.form.metadata.group" style="width: 300px;">
                    </el-input>
                </el-form-item>
                <el-form-item label="name" label-width="100px" prop="name">
                    <el-input :disabled="operation == 'edit'" v-model="data.form.metadata.name" style="width: 300px;">
                    </el-input>
                </el-form-item>
            </el-form>
            <div slot="footer" class="dialog-footer">
                <el-button @click="cancelForm">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{operation}}
                </el-button>
            </div>
        </el-dialog>
    </div>
</template>

<style lang="scss" scoped>

</style>