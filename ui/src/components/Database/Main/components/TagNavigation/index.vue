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
import stores from '../../../../../stores/index'
import { computed } from 'vue'
const { tags } = stores()
const tagsList = computed(() => {
    return tags.tagsList
})
const currentMenu = computed(() => {
    return tags.currentMenu
})
function changeMenu(item) {
    tags.selectMenu(item)
}
function handleClose(item, index) {
    tags.closeTag(item)
}
</script>

<template>
    <div class="justify-start flex">
        <el-tag class="pointer flex align-item-center" size="large" v-for="(item, index) in tagsList"
            :key="item.metadata.name" closable @click="changeMenu(item)" @close="handleClose(item, index)"
            style="width:130px;" :effect="currentMenu.metadata.group === item.metadata.group &&
            currentMenu.metadata.type === item.metadata.type &&
            currentMenu.metadata.name === item.metadata.name
            ? 'dark'
            : 'plain'">
            <span :title="item.metadata.group + ' / ' + item.metadata.type + ' / ' + item.metadata.name"
                class="text-overflow-hidden" style="width:100%">{{
                item.metadata.name
                }}</span>
        </el-tag>
    </div>
</template>
  
<style lang="scss" scoped>
div {
    margin: 10px 10px 0 10px;

    .el-tag {
        margin-right: 10px;
    }
}
</style>