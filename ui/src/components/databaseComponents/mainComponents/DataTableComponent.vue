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
        <el-table v-loading="loading" element-loading-text="loading" element-loading-spinner="el-icon-loading"
            element-loading-background="rgba(0, 0, 0, 0.8)" ref="multipleTable" max-height=700 stripe :data="tableData"
            highlight-current-row tooltip-effect="dark" @selection-change="handleSelectionChange" border
            empty-text="No data yet">
            <el-table-column type="selection" width="55">
            </el-table-column>
            <el-table-column type="index" label="number" width="80">
            </el-table-column>
            <el-table-column v-for="item in tableTags" sortable :sort-change="sortChange" :key="item.name"
                :label="item.name" :prop="item.name">
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
import { getStreamOrMeasure, getTableList } from '@/api/index'
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
        },
        tableTags: {
            handler() {
                let tagFamily = this.fileData.tagFamilies[this.index]
                let tags = []
                tagFamily.tags.forEach((item) => {
                    tags.push(item.name)
                })
                this.param.projection.tagFamilies[0].name = tagFamily.name
                this.param.projection.tagFamilies[0].tags = tags
                this.param.criteria[0].tagFamilyName = tagFamily.name
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
            loading: false,
            index: 0,
            param: {
                metadata: {
                    group: '',
                    name: '',
                    createRevision: '',
                    modRevision: '',
                    id: ''
                },
                offset: null,
                limit: null,
                criteria: [
                    {
                        tagFamilyName: ''
                    }
                ],
                projection: {
                    tagFamilies: [
                        {
                            name: '',
                            tags: []
                        }
                    ]
                }
            }
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
            this.index = index
            this.tableTags = this.fileData.tagFamilies[index].tags
            this.getTable()
        })
        this.$bus.$on('refresh', () => {
            this.initData()
        })
        this.initData()
    },

    methods: {
        /**
         * init data
         * @author wuchusheng
         */
        initData() {
            this.$loading.create()
            getStreamOrMeasure(this.type, this.group, this.name)
                .then((res) => {
                    if (res.status == 200) {
                        this.fileData = res.data[this.type]
                        this.index = 0
                        this.tableTags = this.fileData.tagFamilies[0].tags
                        this.$emit('drawerRight', this.fileData)
                        this.setTagFamily()
                        this.getTable()
                    }
                })
                .finally(() => {
                    this.$loading.close()
                })
        },

        /**
         * get table data
         * @author wuchusheng
         */
        getTable() {
            this.loading = true
            let param = this.param
            param.offset = this.queryInfo.pagenum
            param.limit = this.queryInfo.pagesize
            param.metadata = this.fileData.metadata
            getTableList(param)
                .then((res) => {
                    if(res.status == 200) {
                        this.tableData = res.data.elements
                    }
                })
                .finally(() => {
                    this.loading = false
                })
        },

        /**
         * sort table data
         */
        sortChange(object) {
            console.log(object);
        },

        /**
         * set table tags, set navigation select
         * @author wuchusheng
         */
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