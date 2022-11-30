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
import { computed, reactive, watch } from '@vue/runtime-core'
import DetailListComponent from './components/detail-list.vue'
import DetailTableComponent from './components/detail-table.vue'
const props = defineProps({
    fileData: {
        type: Object,
    }
})
const emit = defineEmits(['closeDetail'])
// data
let data = reactive({
    closeColor: 'var(--color-main-font)',
    closeBackGroundColor: 'var(--color-white)',
    detailList: [],
    options: [],
    tagFamily: 0
})

const tableData = computed(() => {
    console.log('tableData')
    let tags = props.fileData.tagFamilies[data.tagFamily].tags
    console.log(tags)
    let arr = tags.map((item) => {
        return { tags: item.name, type: item.type }
    })
    console.log('arr', arr)
    return arr
})
watch(() => props.fileData, () => {
    data.options = props.fileData.tagFamilies.map((item, index) => {
        return { label: item.name, value: index }
    })
    initData()
})
data.options = props.fileData.tagFamilies.map((item, index) => {
    return { label: item.name, value: index }
})
initData()

function initData() {
    let metadata = props.fileData.metadata
    let tagsNumber = 0
    props.fileData.tagFamilies.forEach((item) => {
        tagsNumber += item.tags.length
    })
    let detail = [
        {
            key: "Url",
            value: `${metadata.group}/${metadata.name}`
        }, {
            key: "CreateRevision",
            value: metadata.createRevision
        }, {
            key: "ModRevision",
            value: metadata.modRevision
        }, {
            key: "Tags families number",
            value: props.fileData.tagFamilies.length
        }, {
            key: "Tags number",
            value: tagsNumber
        }, {
            key: "UpdatedAt",
            value: props.fileData.updatedAt == null ? 'null' : props.fileData.updatedAt
        }]
    data.detailList = detail
}
function handleOver() {
    data.closeColor = 'var(--color-main)'
    data.closeBackgroundColor = 'var(--color-select)'
}
function handleLeave() {
    data.closeColor = "var(--color-main-font)"
    data.closeBackgroundColor = "var(--color-white)"
}
function closeDetail() {
    emit('closeDetail')
}
</script>

<template>
    <div class="drawer-container flex column">
        <el-card style="max-height: calc(100% - 30px); margin: 15px;">
            <div class="detail-content-container">
                <div class="detail-content-title flex justify-between">
                    <div class="text-main-color text-title text-family">
                        <span :title="fileData.metadata.name">
                            {{ fileData.metadata.name }}
                        </span>
                    </div>
                    <el-icon class="el-icon-close pointer detail-close icon" @click="closeDetail"
                        @mouseover="handleOver" @mouseleave="handleLeave"
                        :style="{ color: data.closeColor, backgroundColor: data.closeBackgroundColor }">
                        <Close />
                    </el-icon>
                </div>
                <div class="text-secondary-color text-tips text-start text-family margin-top-bottom-little">
                    {{ fileData.metadata.group }}</div>
                <div v-for="item in data.detailList" :key="item.key">
                    <detail-list-component :keyName="item.key" :value="item.value"></detail-list-component>
                </div>
                <div class="text-main-color text-tips text-start text-family margin-top-bottom-little"
                    style="margin-top: 20px;">Tags
                    families</div>
                <div class="flex align-start" style="margin-bottom: 10px;">
                    <el-select v-model="data.tagFamily" style="width: 100%;" filterable placeholder="Please select">
                        <el-option v-for="item in data.options" :key="item.value" :label="item.label"
                            :value="item.value">
                        </el-option>
                    </el-select>
                </div>
                <!--div class="text-main-color text-tips text-start text-family margin-top-bottom-little">Tags
                    Configuration Information</div-->

                <detail-table-component :tableData="tableData"></detail-table-component>
            </div>
        </el-card>
    </div>
</template>

<style lang="scss" scoped>
.drawer-container {
    width: 100%;
    height: 100%;

    .detail-title {
        width: 100%;
        height: 15px;
        line-height: 15px;
    }

    .detail-content {
        margin-top: 15px;
        width: 100%;
        height: calc(100% - 30px);
        background: var(--color-white);

        .detail-content-container {
            width: calc(100% - 40px);
            height: calc(100% - 40px);

            .detail-content-title {
                width: 100%;
                height: 20px;

                .detail-close {
                    width: 20px;
                    height: 20px;
                }
            }
        }
    }
}
</style>