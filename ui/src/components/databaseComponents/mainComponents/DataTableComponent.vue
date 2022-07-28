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
        <el-table ref="multipleTable" max-height=625 stripe :data="tableData" highlight-current-row
            tooltip-effect="dark" @selection-change="handleSelectionChange">
            <el-table-column type="selection" width="55">
            </el-table-column>
            <el-table-column label="Date" width="120">
                <template slot-scope="scope">{{ scope.row.date }}</template>
            </el-table-column>
            <el-table-column prop="name" label="Name" width="120">
            </el-table-column>
            <el-table-column prop="address" label="Address" show-overflow-tooltip>
            </el-table-column>
        </el-table>

        <el-pagination class="margin-top-bottom" @size-change="handleSizeChange" @current-change="handleCurrentChange"
            :current-page="queryInfo.pagenum" :page-sizes="[6, 12, 18, 24]" :page-size="queryInfo.pagesize"
            layout="total, sizes, prev, pager, next, jumper" :total="total">
        </el-pagination>
    </div>
</template>

<script>
export default {
    data() {
        return {
            total: 25,
            queryInfo: {
                pagenum: 1,
                pagesize: 6,
            },
            tableData: [{
                date: '2016-05-03',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-02',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-04',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-01',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-08',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-08',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-08',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-08',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-06',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }, {
                date: '2016-05-07',
                name: 'Xiaohu Wang',
                address: 'Shaihai'
            }],
            multipleSelection: [],
            checkAllFlag: false
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
    },

    methods: {
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