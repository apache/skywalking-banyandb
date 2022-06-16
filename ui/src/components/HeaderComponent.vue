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
    <div class="flex align-item-center justify-between bd-bottom">
        <div class="image flex align-item-center">
            <el-image :src="userImg" class="flex center" fit="fill">
                <div slot="error" class="image-slot">
                    <i class="el-icon-picture-outline"></i>
                </div>
            </el-image>
            <div class="title text-main-color text-title text-family text-weight-lt">BanyanDB Manager</div>
        </div>
        <div>
            <el-menu active-text-color="#6E38F7" router class="el-menu-demo" mode="horizontal" :default-active="active"
                @select="handleSelect">
                <el-menu-item index="/home">Home</el-menu-item>
                <el-menu-item index="/database">Database</el-menu-item>
                <el-menu-item index="/structure">Structure</el-menu-item>
                <el-menu-item index="/about">About Us</el-menu-item>
            </el-menu>
        </div>
        <div class="person flex justify-around">
            <!--el-image :src="noticeImg" style="width:20px; height:20px"></el-image>
            <el-image :src="personImg" style="width:20px; height:20px"></el-image-->
            <div class="text-normal text-main-color text-family text-weight-lt text-title">Admin</div>
        </div>
    </div>
</template>

<script>
export default {
    name: 'HeaderComponent',
    data() {
        return {
            userImg: require('../../../assets/banyandb_small.jpg'),
            // noticeImg: require('../assets/icon/notice.png'),
            // personImg: require('../assets/icon/person.png')
        }
    },
    props:{
        active:{
            type: String,
            default: '/home'
        }
    }
}
</script>

<style lang="scss" scoped>
.image {
    width: 250px;
    height: 59px;

    .el-image {
        width: 59px;
        height: 59px;
    }

    .title {
        height: 100%;
        line-height: 59px;
    }
}

.person {
    width: 140px;
    height: 100%;
    margin-right: 30px;
}

.el-menu-item {
    font-weight: var(--weight-lt);
    font-size: var(--size-title);
    font-family: var(--font-family-main);
}
</style>