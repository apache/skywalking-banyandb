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
    icon: {
        type: String,
    },
    name: {
        type: String
    }
})

// data
let active = 'background-color: #F2F6FC'

// methods
function handleOver() {
    active = "background-color: #ecf5ff; color: var(--color-main); font-color: var(--color-main)"
}
function handleLeave() {
    active = "background-color: #F2F6FC; color: var(--color-main-font); font-color: var(--color-main-font)"
}
</script>
    
<template>
    <div class="button-item flex center text-main-color text-family" :style="active" @mouseover="handleOver"
        @mouseleave="handleLeave">
        <i :class="icon" class="text-big"></i>
        <div class="text-main-color text-family text-title" :style="active">{{name}}</div>
    </div>
</template>
    
<style lang="scss" scoped>
.button-item {
    height: 100%;
    cursor: pointer;
}

i {
    margin-right: 10px;
}
</style>