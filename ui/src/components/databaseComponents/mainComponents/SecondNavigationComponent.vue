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
    <div class="flex second-nav-contain align-item-center justify-around">
        <el-button class="nav-button" @click="checkAll">Check All</el-button>
        <el-button class="nav-button" @click="checkNone">Check None</el-button>
        <div class="flex align-item-center date-picker">
            <div class="text-family text-main-color text-normal">Time</div>
            <el-date-picker class="picker" v-model="value" align="right" type="date" placeholder="Select time"
                :picker-options="pickerOptions">
            </el-date-picker>
        </div>
        <el-input class="search-input" placeholder="Search by Tags" clearable v-model="query">
            <el-button slot="append" icon="el-icon-search"></el-button>
        </el-input>
        <div class="flex refresh align-item-center justify-around pointer border-radius-little"
            :style="{ fontColor: refreshStyle.fontColor, color: refreshStyle.color, backgroundColor: refreshStyle.backgroundColor }"
            @mouseover="handleOver" @mouseleave="handleLeave">
            <i class="el-icon-refresh-right icon"></i>
            <div>Refresh</div>
        </div>
    </div>
</template>

<script>
export default {
    name: 'SecondNavigationComponent',
    data() {
        return {
            refreshStyle: {
                fontColor: "var(--color-main-font)",
                color: "var(--color-main-font)",
                backgroundColor: "var(--color-white)"
            },
            query: "",
            value: "",
            pickerOptions: {
                disabledDate(time) {
                    return time.getTime() > Date.now();
                },
                shortcuts: [{
                    text: 'Today',
                    onClick(picker) {
                        picker.$emit('pick', new Date());
                    }
                }, {
                    text: 'Yesterday',
                    onClick(picker) {
                        const date = new Date();
                        date.setTime(date.getTime() - 3600 * 1000 * 24);
                        picker.$emit('pick', date);
                    }
                }, {
                    text: 'A week ago',
                    onClick(picker) {
                        const date = new Date();
                        date.setTime(date.getTime() - 3600 * 1000 * 24 * 7);
                        picker.$emit('pick', date);
                    }
                }]
            },
        }
    },
    methods: {
        handleOver() {
            this.refreshStyle.fontColor = "var(--color-main)"
            this.refreshStyle.color = "var(--color-main)"
            this.refreshStyle.backgroundColor = "var(--color-select)"
        },
        handleLeave() {
            this.refreshStyle.fontColor = "var(--color-main-font)"
            this.refreshStyle.color = "var(--color-main-font)"
            this.refreshStyle.backgroundColor = "var(--color-white)"
        },
        checkAll() {
            this.$bus.$emit('checkAll')
        },
        checkNone() {
            this.$bus.$emit('checkNone')
        }
    },
}
</script>

<style lang="scss" scoped>
.nav-button {
    width: 8%;
}

.second-nav-contain {
    width: 100%;
    height: 50px;

    .date-picker {
        width: 35%;
    }

    .search-input {
        width: 35%;
    }

    .refresh {
        width: 5%;
        height: 80%;
    }
}

.picker {
    width: 95%;
    margin-left: 10px;
}
</style>