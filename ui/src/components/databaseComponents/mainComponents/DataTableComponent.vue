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
        <el-table ref="multipleTable" max-height=700 stripe :data="tableData" highlight-current-row
            tooltip-effect="dark" @selection-change="handleSelectionChange" empty-text="No data yet">
            <el-table-column type="selection" width="55">
            </el-table-column>
            <el-table-column type="index" label="number" width="80">
            </el-table-column>
            <el-table-column v-for="item in tableTags" :key="item.name" :label="item.name" :prop="item.name">
            </el-table-column>
        </el-table>

        <el-pagination v-if="tableData.length > 0" class="margin-top-bottom" @size-change="handleSizeChange"
            @current-change="handleCurrentChange" :current-page="queryInfo.pagenum" :page-sizes="[6, 12, 18, 24]"
            :page-size="queryInfo.pagesize" layout="total, sizes, prev, pager, next, jumper" :total="total">
        </el-pagination>
    </div>
</template>

<script>
import { mapState } from "vuex"
import { getStreamOrMeasure } from '@/api/index'
export default {
    computed: {
        ...mapState({
            currentMenu: (state) => state.tags.currentMenu
        }),
        type() {
            return this.currentMenu.metadata.type
        },
        group() {
            return this.currentMenu.metadata.group
        },
        name() {
            return this.currentMenu.metadata.name
        },
    },
    watch: {
        currentMenu: {
            handler() {
                this.initData()
            }
        }
    },
    data() {
        return {
            total: 25,
            queryInfo: {
                pagenum: 1,
                pagesize: 6,
            },
            tableTags: [],
            tableData: [],
            multipleSelection: [],
            checkAllFlag: false,
            fileData: {}, // stream or measure
        }
    },

    created() {
        this.$bus.$on('checkAll', () => {
            if (!this.checkAllFlag) {
                this.tableData.forEach(row => {
                    this.$refs.multipleTable.toggleRowSelection(row)
                })
                this.checkAllFlag = true
            }
        })
        this.$bus.$on('checkNone', () => {
            this.$refs.multipleTable.clearSelection()
            this.checkAllFlag = false
        })
        this.$bus.$on('changeTagFamilies', (index) => {
            this.tableTags = this.fileData.tagFamilies[index].tags
        })
        this.$bus.$on('refresh', () => {
            this.initData()
        })
        this.initData()
    },

    methods: {
        initData() {
            this.$loading.create()
            getStreamOrMeasure(this.type, this.group, this.name)
                .then((res) => {
                    if (res.status == 200) {
                        this.fileData = res.data[this.type]
                        this.tableTags = this.fileData.tagFamilies[0].tags
                        this.$emit('drawerRight', this.fileData)
                        this.setTagFamily()
                    }
                })
                .finally(() => {
                    this.$loading.close()
                })
        },
        setTagFamily() {
            let tagFamilies = this.fileData.tagFamilies
            let tagOptions = tagFamilies.map((item, index) => {
                return { label: item.name, value: index }
            })
            this.$bus.$emit('setOptions', tagOptions)
        },
        handleSelectionChange(val) {
            this.multipleSelection = val;
        },
        handleSizeChange() {

        },
        handleCurrentChange() {

        }
    }
}
</script>