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
        <el-dialog width="25%" center :title="`${operation} ${type}`" @close="cancelForm" :visible.sync="dialogVisible">
            <el-form ref="ruleForm" :model="form" label-position="left">
                <el-form-item label="group" label-width="100px" prop="group">
                    <el-input disabled v-model="form.metadata.group" autocomplete="off" style="width: 300px;">
                    </el-input>
                </el-form-item>
                <el-form-item label="name" label-width="100px" prop="name">
                    <el-input :disabled="operation == 'edit'" v-model="form.metadata.name" autocomplete="off"
                        style="width: 300px;"></el-input>
                </el-form-item>
                <el-table :data="tableData" :span-method="objectSpanMethod" border
                    style="width: 100%; margin-top: 20px">
                    <el-table-column prop="tagFamilies" label="tag family" width="180">
                    </el-table-column>
                    <el-table-column prop="name" label="tag">
                    </el-table-column>
                    <el-table-column prop="type" label="type">
                    </el-table-column>
                </el-table>
                <!--el-form-item label="group type" label-width="100px" prop="catalog">
                    <el-select v-model="groupForm.catalog" style="width: 300px;" placeholder="please select">
                        <el-option label="CATALOG_STREAM" value="CATALOG_STREAM"></el-option>
                        <el-option label="CATALOG_MEASURE" value="CATALOG_MEASURE"></el-option>
                    </el-select>
                </el-form-item-->
            </el-form>
            <div slot="footer" class="dialog-footer">
                <el-button @click="cancelForm">cancel</el-button>
                <el-button type="primary" @click="confirmForm">{{operation}}
                </el-button>
            </div>
        </el-dialog>
    </div>
</template>

<script>
export default {

    props: {
        visible: {
            type: Boolean,
            default: false,
            required: true
        },
        operation: {
            type: String,
            default: 'create',
            required: true
        },
        type: {
            type: String,
            default: 'stream',
            required: true
        }
    },

    watch: {
        visible: {
            handler() {
                this.dialogVisible = this.visible
            },
            immediate: true
        }
    },

    data() {
        return {
            dialogVisible: false,
            form: {
                metadata: {
                    group: '',
                    name: ''
                },
                tagFamilies: [
                    {
                        name: '',
                        tags: [
                            {
                                name: '',
                                type: ''
                            }
                        ]
                    }
                ]
            },
            tableData: [{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            },{
                tagFamilies: 'searchable',
                name: 'stream-ids',
                type: 'String'
            }]
        }
    },

    methods: {
        confirmForm() {
            this.$emit('confirm')
        },
        cancelForm() {
            this.$emit('cancel')
        },
        objectSpanMethod({ row, column, rowIndex, columnIndex }) {
            if (columnIndex === 0) {
                
                if (rowIndex % 2 === 0) {
                    return {
                        rowspan: 2,
                        colspan: 1
                    };
                } else {
                    return {
                        rowspan: 0,
                        colspan: 0
                    };
                }
            }
        }
    },
}
</script>

<style lang="scss" scoped>

</style>