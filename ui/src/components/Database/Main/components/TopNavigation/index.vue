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
import TopButton from './components/top-button.vue'

const emit = defineEmits(['handleNavigation'])
// data
let button = [{
    icon: "el-icon-upload2",
    name: "Import"
}, {
    icon: "el-icon-download",
    name: "Export"
}, {
    icon: "el-icon-truck",
    name: "Filter"
}, {
    icon: "el-icon-sort",
    name: "Sort"
}, {
    icon: "el-icon-plus",
    name: "AddData"
}, {
    icon: "el-icon-delete",
    name: "Truncate"
}, {
    icon: "el-icon-reading",
    name: "Detail"
}, {
    icon: "el-icon-files",
    name: "Design"
}]
let drawer = false

// methods
function handleNavigation(item, index) {
    emit('handleNavigation', item, index)
}
function handleClose(done) {
    done()
}
</script>

<template>
    <div class="flex center top-contain">
        <div v-for="(item, index) in button" @click="handleNavigation(item, index)" :key="item.name"
            class="buttonItem poiner">
            <top-button :icon="item.icon" :name="item.name"></top-button>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.top-contain {
    width: 100%;
    height: 50px;
    background-color: var(--color-Quad-border);
}

.buttonItem {
    height: 100%;
    flex-grow: 1;
}
</style>