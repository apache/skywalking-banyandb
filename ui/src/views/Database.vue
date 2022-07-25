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
    <div @mousemove="shrinkMove" @mouseup="shrinkUp" style="width:100%; height:100%">
        <el-container>
            <el-aside :width="fatherWidth" class="bd-top flex" style="position:relative;">
                <aside-component></aside-component>
                <div class="resize" @mousedown="shrinkDown" title="Shrink sidebar"></div>
            </el-aside>
            <el-main style="background-color: var(--color-background)">
                <component :is="mainComponent"></component>
            </el-main>
        </el-container>
    </div>

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
            mainComponent: "mainStartCom",
            isShrink: false,
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
    methods: {
        shrinkMove(e) {
            if (this.isShrink) {
                console.log(e.screenX)
                let wid = e.screenX + 5
                if (wid <= 65) {
                    this.$store.commit('changeCollapse', true)
                    this.$store.commit('changeFatherWidth', '65px')
                }else {
                    this.$store.commit('changeCollapse', false)
                    this.$store.commit('changeFatherWidth', wid + 'px')
                }
            }
        },
        shrinkDown(e) {
            console.log(e)
            this.isShrink = true
        },
        shrinkUp(e) {
            this.isShrink = false
            console.log(this.isShrink)
        }
    }
}
</script>

<style lang="scss" scoped>
.resize {
    cursor: col-resize;
    position: absolute;
    right: 0;
    height: 100%;
    width: 5px;
}
</style>