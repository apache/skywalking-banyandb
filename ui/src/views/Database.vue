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
        <el-aside :width="fatherWidth" class="bd-top">
            <aside-component></aside-component>
        </el-aside>
        <el-main style="background-color: var(--color-background)">
            <component :is="mainComponent"></component>
        </el-main>
    </el-container>
</template>

<script>
import AsideComponent from '../components/databaseComponents/AsideComponent.vue'
import MainComponent from '../components/databaseComponents/MainComponent.vue'
import MainStartComponent from '../components/databaseComponents/MainStartComponent.vue'
import { mapState } from 'vuex'
export default {
    name: 'Database',
    data() {
        return {
            mainComponent: "mainStartCom"
        }
    },
    created() {
        this.$loading.create()
    },
    mounted() {
        this.$loading.close()
    },
    computed: {
        ...mapState({
            fatherWidth: (state) => state.aside.fatherWidth
        })
    },
    components: {
        AsideComponent,
        mainCom: MainComponent,
        mainStartCom: MainStartComponent
    },
    activated() {
        console.log('this component is activated!')
    },
    deactivated() {
        console.log('this component is deactivated!')
    },
}
</script>

<style lang="scss" scoped>
</style>