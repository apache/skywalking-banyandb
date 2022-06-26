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
    <div>
        <el-card>
            <top-navigation-component></top-navigation-component>
            <second-navigation-component></second-navigation-component>
        </el-card>
    </div>
</template>

<script>
import TopNavigationComponent from './mainComponents/TopNavigationComponent.vue'
import SecondNavigationComponent from './mainComponents/SecondNavigationComponent.vue'
export default {
    name: 'MainComponent',
    data() {
        return {

        }
    },
    components: {
        TopNavigationComponent,
        SecondNavigationComponent
    }
}
</script>

<style lang="scss">
.el-card {
    margin: 20px;
    padding: 0;
}

.el-card__body {
    padding: 0;
}
</style>