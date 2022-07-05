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
        <div class="detail-content border-radius">
            <div class="detail-content-container margin-all">
                <div class="detail-content-title flex justify-between">
                    <div class="text-main-color text-title text-family">Name DB</div>
                    <i class="el-icon-close pointer detail-close icon" @click="closeDetail" @mouseover="handleOver"
                        @mouseleave="handleLeave"
                        :style="{ color: closeColor, backgroundColor: closeBackgroundColor }"></i>
                </div>
                <div class="text-secondary-color text-tips text-start text-family margin-top-bottom-little">Group:
                    Stream</div>
                <div v-for="item in detailList" :key="item.key">
                    <detail-list-component :keyName="item.key" :value="item.value"></detail-list-component>
                </div>
                <div  class="text-main-color text-tips text-start text-family margin-top-bottom-little">Tags Configuration Information</div>
                <detail-table-component></detail-table-component>
            </div>
        </div>
    </div>
</template>

<script>
import DetailListComponent from './drawerRightComponents/DetailListComponent.vue'
import DetailTableComponent from './drawerRightComponents/DetailTableComponent.vue'
export default {
    name: 'DrawerRightComponent',
    data() {
        return {
            closeColor: "var(--color-main-font)",
            closeBackgroundColor: "var(--color-white)",
            detailList: [{
                key: "Url",
                value: "Database/node/Stream"
            }, {
                key: "Start Time",
                value: "2022/04/15 17:30:15"
            }, {
                key: "End Time",
                value: "2022/04/15 17:50:17"
            }, {
                key: "Updated at",
                value: "2022/04/15 17:44:59"
            }, {
                key: "Duration",
                value: "5000ms"
            }, {
                key: "Size",
                value: "30B"
            }, {
                key: "Tags Number",
                value: "15"
            }]
        }
    },
    components: {
        DetailListComponent,
        DetailTableComponent
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
    width: calc(100% - 30px);
    height: calc(100% - 30px);
    margin: 15px;

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