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
    <el-menu default-active="2" active-text-color="#6E38F7" style="height: 100%" :collapse="isCollapse"
        :collapse-transition="false">

        <el-submenu index="1">
            <template slot="title">
                <i class="el-icon-folder"></i>
                <span slot="title">Database</span>
            </template>
            <el-submenu index="1-1">
                <template slot="title">
                    <i class="el-icon-folder"></i>
                    <span slot="title">Node name</span>
                </template>
                <el-submenu index="1-1-1">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title">Stream</span>
                    </template>
                    <el-menu-item index="1-1-3-1">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">db</span>
                        </template>
                    </el-menu-item>
                    <el-menu-item index="1-1-3-2">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">product</span>
                        </template>
                    </el-menu-item>
                    <el-menu-item index="1-1-3-3">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">xxxxxxx</span>
                        </template>
                    </el-menu-item>
                </el-submenu>
            </el-submenu>
            <el-submenu index="1-2">
                <template slot="title">
                    <i class="el-icon-folder"></i>
                    <span slot="title">node</span>
                </template>
                <el-submenu index="1-2-1">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title">Stream</span>
                    </template>
                    <el-menu-item index="1-2-3-1">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">db</span>
                        </template>
                    </el-menu-item>
                    <el-menu-item index="1-2-3-2">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">product</span>
                        </template>
                    </el-menu-item>
                    <el-menu-item index="1-2-3-3">
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title">xxxxxx</span>
                        </template>
                    </el-menu-item>
                </el-submenu>
            </el-submenu>
        </el-submenu>

    </el-menu>
</template>

<script>
import { mapState } from 'vuex'
export default {
    name: 'AsideComponent',
    data() {
        return {
        }
    },
    computed: {
        ...mapState({
            isCollapse: (state) => state.aside.isCollapse
        })
    }
}
</script>

<style lang="scss" scoped>
.el-menu {
    width: 100%;
    border-right: none;
    text-align: start;
    text-justify: middle;
}

i {
    font-size: 25px;
    color: var(--color-main);
}
</style>