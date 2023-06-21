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
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { reactive, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router'
import TagEditor from './tagEditor.vue'
import FieldsEditor from './fieldsEditor.vue'
import type { FormInstance } from 'element-plus'
import { ElMessage } from 'element-plus'
import { createResources, editResources, getStreamOrMeasureList, getStreamOrMeasure } from '@/api/index'

const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus

const router = useRouter()
const route = useRoute()
const ruleFormRef = ref<FormInstance>()
const tagEditorRef = ref()
const fieldEditorRef = ref()
const rules = {
    group: [
        {
            required: true, message: 'Please enter the group', trigger: 'blur'
        }
    ],
    name: [
        {
            required: true, message: 'Please select the name', trigger: 'blur'
        }
    ]
}

const data = reactive({
    type: route.params.type,
    operator: route.params.operator,
    form: {
        group: route.params.group,
        name: route.params.group,
        interval: 1,
        intervalUnit: 'ns'
    }
})

const options = [
    {
        label: 'ns',
        value: 'ns'
    },
    {
        label: 'us',
        value: 'us'
    },
    {
        label: 'µs',
        value: 'µs'
    },
    {
        label: 'ms',
        value: 'ms'
    },
    {
        label: 's',
        value: 's'
    },
    {
        label: 'm',
        value: 'm'
    },
    {
        label: 'h',
        value: 'h'
    },
    {
        label: 'd',
        value: 'd'
    }
]

watch(() => route, () => {
    data.form.group = route.params.group
    data.form.name = route.params.name
    data.type = route.params.type + ''
    data.operator = route.params.operator
    initData()
}, {
    immediate: true,
    deep: true
})
const submit = async (formEl: FormInstance | undefined) => {
    if (!formEl) return
    await formEl.validate((valid) => {
        if (valid) {
            const arr = tagEditorRef.value.getTagFamilies()
            const tagFamilies = []
            const entity = []
            arr.forEach(item => {
                const index = tagFamilies.findIndex(tagItem => {
                    return tagItem.name == item.tagFamily
                })
                if (item.entity == true) {
                    entity.push(item.tag)
                }
                if (index >= 0) {
                    let obj = {
                        name: item.tag,
                        type: item.type,
                        indexedOnly: item.indexedOnly
                    }
                    return tagFamilies[index].tags.push(obj)
                }
                let obj = {
                    name: item.tagFamily,
                    tags: [
                        {
                            name: item.tag,
                            type: item.type,
                            indexedOnly: item.indexedOnly
                        }
                    ]
                }
                tagFamilies.push(obj)
            })
            if (entity.length == 0) {
                return ElMessage({
                    message: 'At least one Entity is required',
                    type: "error",
                    duration: 5000
                })
            }
            const form = {
                metadata: {
                    group: data.form.group,
                    name: data.form.name
                },
                tagFamilies: tagFamilies,
                entity: {
                    tagNames: entity
                }
            }
            if (data.type == 'measure') {
                const fields = fieldEditorRef.value.getFields()
                form['fields'] = fields
                form['interval'] = data.form.interval + data.form.intervalUnit
            }
            $loadingCreate()
            let params = {}
            params[data.type + ''] = form
            if (data.operator == 'edit' && data.form.group && data.form.name) {
                return editResources(data.type, data.form.group, data.form.name, params)
                    .then((res) => {
                        if (res.status == 200) {
                            ElMessage({
                                message: 'Edit successed',
                                type: "success",
                                duration: 5000
                            })
                            $bus.emit('refreshAside')
                            $bus.emit('deleteResource', data.form.name)
                            openResourses()
                        }
                    })
                    .finally(() => {
                        $loadingClose()
                    })
            }
            createResources(data.type, params)
                .then((res) => {
                    if (res.status == 200) {
                        ElMessage({
                            message: 'Create successed',
                            type: "success",
                            duration: 5000
                        })
                        $bus.emit('refreshAside')
                        $bus.emit('deleteGroup', data.form.group)
                        openResourses()
                    }
                })
                .finally(() => {
                    $loadingClose()
                })
        }
    })
}
function openResourses() {
    const route = {
        name: data.type + '',
        params: {
            group: data.form.group,
            name: data.form.name,
            operator: 'read',
            type: data.type + ''
        }
    }
    router.push(route)
    const add = {
        label: data.form.name,
        type: 'Read',
        route
    }
    $bus.emit('changeAside', data.form)
    $bus.emit('AddTabs', add)
}
function initData() {
    if (data.operator == 'edit' && data.form.group && data.form.name) {
        $loadingCreate()
        getStreamOrMeasure(data.type, data.form.group, data.form.name)
            .then(res => {
                if (res.status == 200) {
                    const tagFamilies = res.data[data.type + ''].tagFamilies
                    const entity = res.data[data.type + ''].entity.tagNames
                    const arr = []
                    tagFamilies.forEach(item => {
                        item.tags.forEach(tag => {
                            let index = entity.findIndex(entityItem => {
                                return entityItem == tag.name
                            })
                            let obj = {
                                tagFamily: item.name,
                                tag: tag.name,
                                type: tag.type,
                                indexedOnly: tag.indexedOnly,
                                entity: index >= 0 ? true : false
                            }
                            arr.push(obj)
                        })
                    })
                    tagEditorRef.value.setTagFamilies(arr)
                    if (data.type == 'measure') {
                        const fields = res.data[data.type + ''].fields
                        const intervalArr = res.data[data.type + ''].interval.split('')
                        let interval = 0
                        let intervalUnit = ''
                        intervalArr.forEach(char => {
                            let code = char.charCodeAt()
                            if (code >= 48 && code < 58) {
                                interval = interval * 10 + (char - 0)
                            } else {
                                intervalUnit = intervalUnit + char
                            }
                        })
                        data.form.interval = interval
                        data.form.intervalUnit = intervalUnit
                        fieldEditorRef.value.setFields(fields)
                    }
                }
            })
            .finally(() => {
                $loadingClose()
            })
    }
}
</script>

<template>
    <div>
        <el-card shadow="always">
            <template #header>
                <el-row>
                    <el-col :span="12">
                        <div class="flex align-item-center" style="height: 30px; width: 100%;">
                            <div class="flex" style="height: 30px;">
                                <span class="text-bold">Catalog：</span>
                                <span style="margin-right: 20px;">{{ data.type }}</span>
                                <span class="text-bold">Group：</span>
                                <span style="margin-right: 20px;">{{ data.form.group }}</span>
                                <span class="text-bold" v-if="data.form.name">Name：</span>
                                <span style="margin-right: 20px;" v-if="data.form.name">{{ data.form.name }}</span>
                                <span class="text-bold">Operation：</span>
                                <span>{{ data.operator }}</span>
                            </div>
                        </div>
                    </el-col>
                    <el-col :span="12">
                        <div class="flex align-item-center justify-end" style="height: 30px;">
                            <el-button size="small" type="primary" @click="submit(ruleFormRef)"
                                color="#6E38F7">submit</el-button>
                        </div>
                    </el-col>
                </el-row>
            </template>
            <el-form ref="ruleFormRef" :model="data.form" label-width="80px" label-position="left" :rules="rules"
                :inline="true" style="height: 30px;">
                <el-form-item label="group" prop="group">
                    <el-input clearable disabled v-model="data.form.group"></el-input>
                </el-form-item>
                <el-form-item label="name" prop="name">
                    <el-input clearable v-model="data.form.name"></el-input>
                </el-form-item>
                <el-form-item v-if="data.type == 'measure'" label="interval" prop="interval">
                    <el-input-number v-model="data.form.interval" min="1" />
                    <el-select v-model="data.form.intervalUnit" style="width: 100px; margin-left: 5px;">
                        <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value" />
                    </el-select>
                </el-form-item>
            </el-form>
            <TagEditor ref="tagEditorRef"></TagEditor>
            <el-divider v-if="data.type == 'measure'" border-style="dashed" />
            <FieldsEditor ref="fieldEditorRef" v-if="data.type == 'measure'"></FieldsEditor>
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