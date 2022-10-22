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
defineProps({
    icon: String,
    name: String
})
let bdColor = 'white'
let colorStr = 'var(--color-main-font)'
function handleOver() {
    bdColor = '#ecf5ff'
    colorStr = 'var(--color-main)'
}
function handleLeave() {
    bdColor = "white"
    colorStr = "var(--color-main-font)"
}
</script>

<template>
    <div class="flex right-menu-item align-item-center pointer" @mouseover="handleOver" @mouseleave="handleLeave"
        :style="{ backgroundColor: bdColor, color: colorStr}" @click="$emit('handleRightItem')">
        <i :class="icon"></i>
        <el-icon>
            <Folder v-if="icon === 'el-icon-folder'" />
            <Document v-if="icon === 'el-icon-document'" />
            <RefreshRight v-if="icon === 'el-icon-refresh-right'" />
            <Delete v-if="icon === 'el-icon-delete'" />
        </el-icon>
        <div style="width: 70%;" class="text-overflow-hidden margin-left-small">{{ name }}</div>
    </div>
</template>

<style lang="scss" scoped>
.right-menu-item {
    padding: 3px 6px 3px 6px;
    width: calc(100% - 12px);
    height: 40px;
    text-align: start;
}

i {
    font-size: 25px;
    color: var(--color-main);
}
</style>