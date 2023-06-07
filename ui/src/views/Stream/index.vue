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
import Aside from '@/components/Aside/index.vue'
import TopNav from '@/components/TopNav/index.vue'
import { reactive } from 'vue';

const data = reactive({
  width: '200px'
})

function setWidth(width) {
  data.width = width
}
</script>

<template>
  <div class="size">
    <el-container>
      <el-aside :width="data.width" class="bd-top flex box-shadow" style="position:relative; background-color: white;">
        <Aside type="stream" @setWidth="setWidth"></Aside>
      </el-aside>
      <el-main>
        <TopNav></TopNav>
        <RouterView></RouterView>
      </el-main>
    </el-container>
  </div>
</template>

<style lang="scss" scoped>
.el-container {
  height: 100%;
  padding: 0;
  margin: 0;

  .el-header {
    background-color: #ffffff;
    padding: 0;
    margin: 0;
  }

  .el-main {
    background-color: #eaedf1;
    padding: 0;
    margin: 0;
  }
}
</style>