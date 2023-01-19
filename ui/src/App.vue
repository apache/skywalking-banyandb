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
</script>

<template>
  <div id="app">
    <div style="width: 100%; height:100%">
      <RouterView></RouterView>
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
