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

<template>
    <el-container>
        <el-aside width="200px" class="bd-top">
            <aside-component></aside-component>
        </el-aside>
        <el-main>
            <h1>
                this is database
            </h1>
        </el-main>
    </el-container>
</template>

<script>
import AsideComponent from '../components/AsideComponent.vue'
export default {
    name: 'Database',
    data() {
        return {

        }
    },
    components: {
        AsideComponent
    }
}
</script>

<style lang="scss" scoped>
</style>