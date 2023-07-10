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
import RightMenu from '@/components/RightMenu/index.vue'
import { ref, reactive } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { watch, getCurrentInstance } from '@vue/runtime-core'
import { getPropertyList } from '@/api/index'
import request from '@/utils/axios'

const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose
const { ctx: that } = getCurrentInstance()

const data = reactive({
  groupLists: [],
  isCollapse: false,
  activeMenu: ''
})

function openCreateGroup() {

}

function rightClickGroup(e, index) {

}

const param = {
  metadata: {
    container: {
      group: 'sw'
    }
  }
}

function initData() {

}
request({
  url: '/api/v1/property/lists/sw',
  method: 'get'
}).then(res => {
  console.log('res', res)
})

</script>

<template>
  <div style="display: flex; flex-direction: column; width: 100%;">
    <div class="size flex" style="display: flex; flex-direction: column; width: 100%;">
      <el-menu v-if="data.groupLists.length > 0" :collapse="data.isCollapse" :default-active="data.activeMenu">
        <div v-for="(item, index) in data.groupLists" :key="item.metadata.name"
          @contextmenu.prevent="rightClickGroup($event, index)">

        </div>
      </el-menu>
    </div>
    <div class="flex center add" @click="openCreateGroup" style="height: 50px; width: 100%;"
      v-if="data.groupLists.length == 0">
      <el-icon>
        <Plus></Plus>
      </el-icon>
    </div>
  </div>
</template>

<style lang="scss" scoped>
.aside-search {
  margin: 10px;
  width: calc(100% - 20px);
}

.el-menu {
  width: 100%;
  border-right: none;
  text-align: start;
  text-justify: middle;
}

.resize {
  cursor: col-resize;
  position: absolute;
  right: 0;
  height: 100%;
  width: 5px;
}

.right-menu {
  width: 170px;
  position: fixed;
  z-index: 9999999999999999999999999999 !important;
  background-color: white;
}

.footer {
  width: 100%;
  display: flex;
  justify-content: center;
}

.add {
  cursor: pointer;
}
</style>