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
  import { ref, watch } from 'vue';
  import { ElImage, ElMenu, ElMenuItem, ElSubMenu } from 'element-plus';
  import { useRoute } from 'vue-router';
  import userImg from '@/assets/banyandb_small.jpg';
  import { MENU_ACTIVE_COLOR, MENU_DEFAULT_PATH, MenuConfig } from './constants';

  const route = useRoute();
  const getActiveMenu = (path) => {
    const parts = path.split('/').filter(Boolean);
    return parts.length >= 2 ? `/${parts[0]}/${parts[1]}` : MENU_DEFAULT_PATH;
  };
  const activeMenu = ref(getActiveMenu(route.path));

  watch(
    () => route.path,
    (newPath) => {
      activeMenu.value = getActiveMenu(newPath);
    },
    { immediate: true },
  );
</script>

<template>
  <div class="size flex align-item-center justify-between bd-bottom">
    <div class="image">
      <el-image :src="userImg" class="flex center" fit="fill">
        <div slot="error" class="image-slot">
          <i class="el-icon-picture-outline"></i>
        </div>
      </el-image>
      <span class="title text-main-color text-title text-family text-weight-lt" role="img" aria-label="BanyanDB Manager">BanyanDB Manager</span>
    </div>
    <nav class="navigation">
      <el-menu
        :active-text-color="MENU_ACTIVE_COLOR"
        router
        :ellipsis="false"
        mode="horizontal"
        :default-active="activeMenu"
      >
        <template v-for="item in MenuConfig" :key="item.index">
          <el-sub-menu v-if="item.children" :index="item.index">
            <template #title>{{ item.label }}</template>
            <el-menu-item v-for="child in item.children" :key="child.index" :index="child.index">
              {{ child.label }}
            </el-menu-item>
          </el-sub-menu>
          <el-menu-item v-else :index="item.index">{{ item.label }}</el-menu-item>
        </template>
      </el-menu>
    </nav>
    <div class="flex-block"> </div>
  </div>
</template>

<style lang="scss" scoped>
  $header-height: 60px;
  $logo-size: 59px;

  .image {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 100%;

    .el-image {
      width: $logo-size;
      height: $logo-size;
      flex-shrink: 0;
    }

    .title {
      height: 100%;
      line-height: $logo-size;
      white-space: nowrap;
      margin: 0;
      font-size: inherit;
    }
  }

  .navigation {
    display: flex;
    align-items: center;
    justify-content: center;
    margin-right: 20%;

    :deep(.el-menu) {
      display: flex;
      align-items: center;
      border-bottom: none;
      height: $header-height;
    }
  }

  .el-menu-item,
  :deep(.el-sub-menu__title) {
    font-weight: var(--weight-lt);
    font-size: var(--size-lt);
    font-family: var(--font-family-main);
    height: $header-height;
    line-height: $header-height;
    display: flex;
    align-items: center;

    &:hover {
      color: var(--el-menu-active-color) !important;
    }
  }

  :deep(.el-sub-menu) {
    height: $header-height;
    display: flex;
    align-items: center;
  }

  :deep(.el-sub-menu__title) {
    border-bottom: none;
  }

  .flex-block {
    width: 140px;
    margin-right: 30px;
  }
</style>
