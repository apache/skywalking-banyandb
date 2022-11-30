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
import HeaderComponent from './components/Header/index.vue'
import RightMenuComponent from './components/Database/Aside/components/RightMenu/index.vue'
import { computed } from '@vue/runtime-core'
import stores from './stores/index'
import { useRouter, useRoute } from 'vue-router'
import { getCurrentInstance } from "@vue/runtime-core";

const route = useRoute()
const router = useRouter()
const { menuState, header } = stores()
const { proxy } = getCurrentInstance()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
const showRightMenu = computed(() => {
  return menuState.showRightMenu
})
const left = computed(() => {
  return menuState.left
})
const top = computed(() => {
  return menuState.top
})
const rightMenuList = computed(() => {
  return menuState.rightMenuList
})
let activePath = ""
let show = true
let showButton = true

const initData = (() => {
  let path = route.path
  let name = route.name
  if (name == "NotFound") {
    show = false
  } else {
    activePath = path
  }
  if (name == "Stream" || name == "Measure") {
    header.changeShowButton(true)
  } else {
    header.changeShowButton(false)
  }
})()

const appMouseDown = () => {
  menuState.changeShowRightMenu(false)
}
const handleRightItem = (index) => {
  $bus.emit('handleRightItem', index)
}
</script>

<template>
  <div id="app">
    <div @mousedown="appMouseDown" style="width: 100%; height:100%">
      <el-container>
        <el-header v-if="show">
          <header-component :active="activePath" :showButton="showButton"></header-component>
        </el-header>
        <el-main>
          <keep-alive>
            <router-view v-if="$route.meta.keepAlive" />
          </keep-alive>
          <router-view v-if="!$route.meta.keepAlive"></router-view>
        </el-main>
      </el-container>
    </div>
    <div v-show="showRightMenu" class="right-menu border-radius-little box-shadow"
      :style="{ top: top + 'px', left: left + 'px' }">
      <right-menu-component @handleRightItem="handleRightItem" :rightMenuList="rightMenuList">
      </right-menu-component>
    </div>
  </div>
</template>

<style lang="scss">
@use './styles/elementPlus.scss' as *;
</style>

<style lang="scss">
html,
body {
  padding: 0;
  margin: 0;
  width: 100%;
  height: 100%;
}

#app {
  color: #2c3e50;
  height: 100%;
  margin: 0;
  padding: 0;
}

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

.right-menu {
  width: 170px;
  position: fixed;
  z-index: 9999999999999999999999999999 !important;
  background-color: white;
}
</style>

<style scoped>
header {
  line-height: 1.5;
  max-height: 100vh;
}

.logo {
  display: block;
  margin: 0 auto 2rem;
}

nav {
  width: 100%;
  font-size: 12px;
  text-align: center;
  margin-top: 2rem;
}

nav a.router-link-exact-active {
  color: var(--color-text);
}

nav a.router-link-exact-active:hover {
  background-color: transparent;
}

nav a {
  display: inline-block;
  padding: 0 1rem;
  border-left: 1px solid var(--color-border);
}

nav a:first-of-type {
  border: 0;
}

@media (min-width: 1024px) {
  header {
    display: flex;
    place-items: center;
    padding-right: calc(var(--section-gap) / 2);
  }

  .logo {
    margin: 0 2rem 0 0;
  }

  header .wrapper {
    display: flex;
    place-items: flex-start;
    flex-wrap: wrap;
  }

  nav {
    text-align: left;
    margin-left: -1rem;
    font-size: 1rem;

    padding: 1rem 0;
    margin-top: 1rem;
  }
}
</style>
