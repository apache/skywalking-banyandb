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
import AsideComponent from '../components/Database/Aside/index.vue'
import MainComponent from '../components/Database/Main/index.vue'
import MainStartComponent from '../components/Database/Main/components/MainStart/index.vue'
import stores from '../stores/index'
import { computed } from 'vue'
const { aside, tags } = stores()

// pinia data
const fatherWidth = computed(() => {
    return aside.fatherWidth
})
const mainComponent = computed(() => {
    return tags.currentMenu == null ? 'mainStartCom' : 'mainCom'
})

// data
let isShrink = false

// methods
function shrinkMove(e) {
    if (isShrink) {
        let wid = e.screenX + 5
        if (wid <= 65) {
            aside.changeCollapse(true)
            aside.changeFatherWidth('65px')
        } else {
            aside.changeCollapse(false)
            aside.changeFatherWidth(`${wid}px`)
        }
    }
}
function shrinkDown(e) {
    isShrink = true
}
function shrinkUp(e) {
    isShrink = false
}
</script>

<template>
    <div @mousemove="shrinkMove" @mouseup="shrinkUp" style="width:100%; height:100%">
        <el-container>
            <el-aside :width="fatherWidth" class="bd-top flex" style="position:relative;">
                <AsideComponent type="measure"></AsideComponent>
                <div class="resize" @mousedown="shrinkDown" title="Shrink sidebar"></div>
            </el-aside>
            <el-main style="background-color: var(--color-background)">
                <component :is="MainStartComponent" type="measure"></component>
            </el-main>
        </el-container>
    </div>
</template>

<style lang="scss" scoped>
.resize {
    cursor: col-resize;
    position: absolute;
    right: 0;
    height: 100%;
    width: 5px;
}
</style>