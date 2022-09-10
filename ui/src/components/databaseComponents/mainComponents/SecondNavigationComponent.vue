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
        <!--div class="flex align-item-center date-picker">
            <div class="text-family text-main-color text-normal">Time</div>
            <el-date-picker class="picker date-picker" v-model="value" align="right" type="date"
                placeholder="Select time" :picker-options="pickerOptions">
            </el-date-picker>
        </div-->
        <el-select v-model="tagFamily" @change="changeTagFamilies" filterable placeholder="Please select">
            <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value">
            </el-option>
        </el-select>
        <el-date-picker class="date-picker" v-model="value" type="datetimerange" :picker-options="pickerOptions" range-separator="to"
            start-placeholder="begin" end-placeholder="end" align="right">
        </el-date-picker>
        <el-input class="search-input" placeholder="Search by Tags" clearable v-model="query">
            <el-button slot="append" icon="el-icon-search"></el-button>
        </el-input>
        <!--div class="flex refresh align-item-center justify-around pointer border-radius-little"
            :style="{ fontColor: refreshStyle.fontColor, color: refreshStyle.color, backgroundColor: refreshStyle.backgroundColor }"
            @mouseover="handleOver" @mouseleave="handleLeave">
            <i class="el-icon-refresh-right icon"></i>
            <div>Refresh</div>
        </div-->
        <el-button class="nav-button" @click="refresh" icon="el-icon-refresh-right">Refresh</el-button>
        <el-button class="nav-button" @click="openDetail">{{ showDrawer ? "Close Detail" : "Open Detail" }}</el-button>
        <el-button class="nav-button" @click="openDesign">Open Design</el-button>
    </div>
</template>

<script>
export default {
    name: 'SecondNavigationComponent',
    data() {
        return {
            options: [],
            tagFamily: 0,
            refreshStyle: {
                fontColor: "var(--color-main-font)",
                color: "var(--color-main-font)",
                backgroundColor: "var(--color-white)"
            },
            query: "",
            value: "",
            pickerOptions: {
                shortcuts: [{
                    text: 'Last week',
                    onClick(picker) {
                        const end = new Date();
                        const start = new Date();
                        start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
                        picker.$emit('pick', [start, end]);
                    }
                }, {
                    text: 'Last month',
                    onClick(picker) {
                        const end = new Date();
                        const start = new Date();
                        start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
                        picker.$emit('pick', [start, end]);
                    }
                }, {
                    text: 'Last three months',
                    onClick(picker) {
                        const end = new Date();
                        const start = new Date();
                        start.setTime(start.getTime() - 3600 * 1000 * 24 * 90);
                        picker.$emit('pick', [start, end]);
                    }
                }]
            },
        }
    },
    props: {
        showDrawer: {
            type: Boolean,
            default: false
        }
    },
    created() {
        this.$bus.$on('setOptions', (options) => {
            this.options = options
        })
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
        changeTagFamilies() {
            this.$bus.$emit('changeTagFamilies', this.tagFamily)
        },
        refresh() {
            this.$bus.$emit('refresh')
        },
        openDetail() {
            this.$emit('openDetail')
        },
        openDesign() {

        }
        /*checkAll() {
            this.$bus.$emit('checkAll')
        },
        checkNone() {
            this.$bus.$emit('checkNone')
        }*/
    },
}
</script>

<style lang="scss" scoped>
.nav-button {
    width: 8%;
    margin: 0;
}

.second-nav-contain {
    width: 100%;
    height: 50px;

    .date-picker {
        width: 28%;
        margin: 0;
    }

    .search-input {
        width: 28%;
        margin: 0;
    }

    .refresh {
        width: 5.5%;
        height: 80%;
    }
}

.picker {
    width: 95%;
    margin-left: 10px;
}
</style>