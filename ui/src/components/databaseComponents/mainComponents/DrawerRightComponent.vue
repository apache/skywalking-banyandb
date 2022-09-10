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
<template>
    <div class="drawer-container flex column">
        <div class="text-secondary-color text-tips text-start text-family detail-title">Details</div>
        <div class="detail-content border-radius-little">
            <div class="detail-content-container margin-all">
                <div class="detail-content-title flex justify-between">
                    <div class="text-main-color text-title text-family">
                        <span :title="fileData.metadata.name">
                            {{fileData.metadata.name}}
                        </span>
                    </div>
                    <i class="el-icon-close pointer detail-close icon" @click="closeDetail" @mouseover="handleOver"
                        @mouseleave="handleLeave"
                        :style="{ color: closeColor, backgroundColor: closeBackgroundColor }"></i>
                </div>
                <div class="text-secondary-color text-tips text-start text-family margin-top-bottom-little">
                    {{fileData.metadata.group}}</div>
                <div v-for="item in detailList" :key="item.key">
                    <detail-list-component :keyName="item.key" :value="item.value"></detail-list-component>
                </div>
                <div class="text-main-color text-tips text-start text-family margin-top-bottom-little"
                    style="margin-top: 20px;">Tags
                    Configuration Information</div>
                <div class="flex align-start">
                    <el-select v-model="tagFamily" @change="changeTagFamilies" style="width: 100%;" filterable
                        placeholder="Please select">
                        <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value">
                        </el-option>
                    </el-select>
                </div>
                <!--div class="text-main-color text-tips text-start text-family margin-top-bottom-little">Tags
                    Configuration Information</div-->

                <detail-table-component :tableData="tableData"></detail-table-component>
            </div>
        </div>
    </div>
</template>

<script>
import DetailListComponent from './drawerRightComponents/DetailListComponent.vue'
import DetailTableComponent from './drawerRightComponents/DetailTableComponent.vue'
export default {
    name: 'DrawerRightComponent',
    props: {
        fileData: {
            type: Object,
        }
    },
    computed: {
        tableData() {
            let tags = this.fileData.tagFamilies[this.tagFamily].tags
            return tags.map((item) => {
                return { tags: item.name, type: item.type }
            })
        }
    },
    watch: {
        fileData: {
            handler() {
                this.options = this.fileData.tagFamilies.map((item, index) => {
                    return { label: item.name, value: index }
                })
            },
            immediate: true
        }
    },
    data() {
        return {
            tagFamily: 0,
            options: [],
            closeColor: "var(--color-main-font)",
            closeBackgroundColor: "var(--color-white)",
            detailList: []
        }
    },
    components: {
        DetailListComponent,
        DetailTableComponent
    },
    created() {
        let metadata = this.fileData.metadata
        let tagsNumber = 0
        this.fileData.tagFamilies.forEach((item) => {
            tagsNumber += item.tags.length
        })
        let detailList = [
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
                value: this.fileData.tagFamilies.length
            }, {
                key: "Tags number",
                value: tagsNumber
            }, {
                key: "UpdatedAt",
                value: this.fileData.updatedAt == null ? 'null' : this.fileData.updatedAt
            }]
        this.detailList = detailList
        console.log('fileData', this.fileData);
    },
    methods: {
        handleOver() {
            this.closeColor = "var(--color-main)",
                this.closeBackgroundColor = "var(--color-select)"
        },
        handleLeave() {
            this.closeColor = "var(--color-main-font)",
                this.closeBackgroundColor = "var(--color-white)"
        },
        closeDetail() {
            this.$emit('closeDetail')
        }
    },
}
</script>

<style lang="scss" scoped>
.drawer-container {
    width: calc(100% - 20px);
    height: calc(100% - 20px);
    margin: 10px;

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