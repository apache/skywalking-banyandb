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
    tagFamily: [
        {
            required: true, message: 'Please enter the tagFamily name', trigger: 'blur'
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
        default: 'Add',
        required: true
    },
    type: {
        type: String,
        default: 'Tag'
    },
    group: {
        type: String,
        required: true
    },
    name: {
        type: String,
        required: true
    }
})

let data = reactive({
    dialogVisible: false,
    form: {
        metadata: {
            group: '',
            name: '',
            tagFamily: ''
        }
    }
})
let rules = rule
let tags = [{
    name: 'start_time',
    type: 'TAG_TYPE_INT',
    indexedOnly: false
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
        <el-dialog width="35%" center :title="`${operation} ${type}`" :show-close="false" v-model="data.dialogVisible"
            :align-center="true">
            <el-form ref="ruleForm" :rules="rules" :model="data.form.metadata" label-position="left">
                <el-form-item label="group" label-width="100px" prop="group">
                    <el-input disabled v-model="data.form.metadata.group">
                    </el-input>
                </el-form-item>
                <el-form-item label="name" label-width="100px" prop="name">
                    <el-input disabled v-model="data.form.metadata.name">
                    </el-input>
                </el-form-item>
                <el-form-item label="tagFamily" label-width="100px" prop="tagFamily">
                    <el-input v-model="data.form.metadata.tagFamily">
                    </el-input>
                </el-form-item>
                <div class="flex justify-between justify-between opeartorTag">
                    <div>
                        <span>Tag</span>
                    </div>
                    <div style="width: 50%" class="flex justify-end">
                        <el-button type="primary" size="small">Add tag</el-button>
                        <el-button size="small">Batch deletion</el-button>
                    </div>
                </div>
                <el-table :data="tags" border>
                    <el-table-column type="selection" width="55" />
                    <el-table-column label="Name" prop="name" />
                    <el-table-column label="Type" prop="type" />
                    <el-table-column label="indexedOnly" prop="indexedOnly" />
                    <el-table-column fixed="right" label="Operations" width="150">
                        <template #default>
                            <el-button size="small" @click="handleClick">Detele</el-button>
                            <el-button size="small">Edit</el-button>
                        </template>
                    </el-table-column>
                </el-table>
            </el-form>
            <div slot="footer" class="dialog-footer footer">
                <el-button @click="cancelForm">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{ operation }}
                </el-button>
            </div>
        </el-dialog>
    </div>
</template>

<style lang="scss" scoped>
.footer {
    width: 100%;
    display: flex;
    justify-content: center;
}

.opeartorTag {
    width: 100%;
    height: 40px;
}
</style>