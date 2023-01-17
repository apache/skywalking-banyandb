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
import { reactive } from 'vue';
import { useRoute } from 'vue-router'
const route = useRoute()
const data = reactive({
    type: ''
})

// function
function initData() {
    data.type = route.meta.type
}
initData()
</script>

<template>
    <div class="size flex center column">
        <div class="text-big text-placeholder-color text-family margin-top-bottom">Welcome to BanyanDB Manager!</div>
        <div class="text-title text-placeholder-color text-famil">Please open the {{ data.type }} to start working!</div>
    </div>
</template>

<style lang="scss" scoped>

</style>