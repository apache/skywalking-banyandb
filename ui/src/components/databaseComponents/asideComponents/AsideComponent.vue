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
    <div style="width:100%; height:100%">
        <el-menu default-active="2" active-text-color="#6E38F7" style="height: 100%;" :collapse="isCollapse"
            :collapse-transition="false">
            <div v-for="(item, index) in groupLists" :key="item.metadata.name">
                <el-submenu :index="item.metadata.name + '-' + index">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title" :title="item.metadata.name" style="width: 70%"
                            class="text-overflow-hidden">{{
                                    item.metadata.name
                            }}</span>
                    </template>
                    <el-submenu :index="item.metadata.name + '-' + index + '-stream'">
                        <template slot="title">
                            <i class="el-icon-folder"></i>
                            <span slot="title">Stream</span>
                        </template>
                        <el-menu-item :index="'streamFile1' + index">
                            <template slot="title">
                                <i class="el-icon-document"></i>
                                <span slot="title" title="streamFile" style="width: 90%"
                                    class="text-overflow-hidden">streamFile1</span>
                            </template>
                        </el-menu-item>
                    </el-submenu>
                    <el-submenu :index="item.metadata.name + '-' + index + '-measure'">
                        <template slot="title">
                            <i class="el-icon-folder"></i>
                            <span slot="title">Measure</span>
                        </template>
                        <div v-for="(itemMeasure, indexMeasure) in item.measure" :key="itemMeasure.metadata.name">
                            <div @contextmenu.prevent="rightClick(index, indexMeasure, $event)">
                                <el-menu-item
                                    :index="item.metadata.name + '-' + index + '-' + itemMeasure.metadata.name">
                                    <template slot="title">
                                        <i class="el-icon-document"></i>
                                        <span slot="title" :title="itemMeasure.metadata.name" style="width: 90%"
                                            class="text-overflow-hidden">{{ itemMeasure.metadata.name }}</span>
                                    </template>
                                </el-menu-item>
                            </div>
                        </div>
                    </el-submenu>
                </el-submenu>
            </div>
        </el-menu>
        <div v-if="showRightMenu" class="right-menu border-radius-little box-shadow" :style="{top: topNumber+'px', left: leftNumber+'px'}">
            <right-menu-component></right-menu-component>
        </div>
    </div>
</template>
 
<script>
import { mapState } from 'vuex'
import RightMenuComponent from './RightMenuComponent.vue'
export default {
    name: 'AsideComponent',
    data() {
        return {
            groupLists: [{
                "metadata": {
                    "group": "",
                    "name": "measure-default",
                    "id": 0,
                    "createRevision": "0",
                    "modRevision": "0"
                },
                "catalog": "CATALOG_MEASURE",
                "resourceOpts": {
                    "shardNum": 1,
                    "blockNum": 12,
                    "ttl": "420d"
                },
                "updatedAt": null,
                "measure": [
                    {
                        "metadata": {
                            "group": "measure-default",
                            "name": "browser_app_error_rate",
                            "id": 0,
                            "createRevision": "764",
                            "modRevision": "764"
                        },
                        "tagFamilies": [
                            {
                                "name": "default",
                                "tags": [
                                    {
                                        "name": "entity_id",
                                        "type": "TAG_TYPE_STRING"
                                    },
                                    {
                                        "name": "denominator",
                                        "type": "TAG_TYPE_INT"
                                    },
                                    {
                                        "name": "numerator",
                                        "type": "TAG_TYPE_INT"
                                    },
                                    {
                                        "name": "time_bucket",
                                        "type": "TAG_TYPE_INT"
                                    },
                                    {
                                        "name": "id",
                                        "type": "TAG_TYPE_ID"
                                    }
                                ]
                            }
                        ],
                        "fields": [
                            {
                                "name": "percentage",
                                "fieldType": "FIELD_TYPE_INT",
                                "encodingMethod": "ENCODING_METHOD_GORILLA",
                                "compressionMethod": "COMPRESSION_METHOD_ZSTD"
                            }
                        ],
                        "entity": {
                            "tagNames": [
                                "id"
                            ]
                        },
                        "interval": "1h",
                        "updatedAt": null
                    },
                    {
                        "metadata": {
                            "group": "measure-default",
                            "name": "browser_app_error_sum",
                            "id": 0,
                            "createRevision": "769",
                            "modRevision": "769"
                        },
                        "tagFamilies": [
                            {
                                "name": "default",
                                "tags": [
                                    {
                                        "name": "entity_id",
                                        "type": "TAG_TYPE_STRING"
                                    },
                                    {
                                        "name": "time_bucket",
                                        "type": "TAG_TYPE_INT"
                                    },
                                    {
                                        "name": "id",
                                        "type": "TAG_TYPE_ID"
                                    }
                                ]
                            }
                        ],
                        "fields": [
                            {
                                "name": "value",
                                "fieldType": "FIELD_TYPE_INT",
                                "encodingMethod": "ENCODING_METHOD_GORILLA",
                                "compressionMethod": "COMPRESSION_METHOD_ZSTD"
                            }
                        ],
                        "entity": {
                            "tagNames": [
                                "id"
                            ]
                        },
                        "interval": "1h",
                        "updatedAt": null
                    },
                    {
                        "metadata": {
                            "group": "measure-default",
                            "name": "browser_app_page_ajax_error_sum",
                            "id": 0,
                            "createRevision": "790",
                            "modRevision": "790"
                        },
                        "tagFamilies": [
                            {
                                "name": "default",
                                "tags": [
                                    {
                                        "name": "entity_id",
                                        "type": "TAG_TYPE_STRING"
                                    },
                                    {
                                        "name": "service_id",
                                        "type": "TAG_TYPE_STRING"
                                    },
                                    {
                                        "name": "time_bucket",
                                        "type": "TAG_TYPE_INT"
                                    },
                                    {
                                        "name": "id",
                                        "type": "TAG_TYPE_ID"
                                    }
                                ]
                            }
                        ],
                        "fields": [
                            {
                                "name": "value",
                                "fieldType": "FIELD_TYPE_INT",
                                "encodingMethod": "ENCODING_METHOD_GORILLA",
                                "compressionMethod": "COMPRESSION_METHOD_ZSTD"
                            }
                        ],
                        "entity": {
                            "tagNames": [
                                "id"
                            ]
                        },
                        "interval": "1h",
                        "updatedAt": null
                    }
                ]
            },
            {
                "metadata": {
                    "group": "",
                    "name": "stream-browser_error_log",
                    "id": 0,
                    "createRevision": "0",
                    "modRevision": "0"
                },
                "catalog": "CATALOG_STREAM",
                "resourceOpts": {
                    "shardNum": 2,
                    "blockNum": 0,
                    "ttl": "420d"
                },
                "updatedAt": null
            },
            {
                "metadata": {
                    "group": "",
                    "name": "stream-default",
                    "id": 0,
                    "createRevision": "0",
                    "modRevision": "0"
                },
                "catalog": "CATALOG_STREAM",
                "resourceOpts": {
                    "shardNum": 1,
                    "blockNum": 0,
                    "ttl": "420d"
                },
                "updatedAt": null
            },],
            //showRightMenu: false,
            topNumber: 0,
            leftNumber: 0
        }
    },

    components: {
        RightMenuComponent
    },

    async created() {
        console.log('this is aside created')
        this.getGroupLists()
    },

    computed: {
        ...mapState({
            isCollapse: (state) => state.aside.isCollapse,
            showRightMenu: (state) => state.menuState.showRightMenu
        })
    },

    methods: {
        async getGroupLists() {
            try {
                const data = await this.$http.get('/api/v1/group/schema/lists')
                if (data.status != 200) {
                    this.$message.error(data.status, data.statusText)
                } else {
                    this.groupLists = data.data.group
                    console.log(data)
                }
            } catch (err) {
                console.log(err)
                this.$message.errorNet()
            }
        },
        rightClick(index, indexMeasure, e) {
            this.$store.commit("changeShowRightMenu", true)
            this.topNumber = e.pageY
            this.leftNumber = e.pageX
        }
    },
}
</script>

<style lang="scss" scoped>
.el-menu {
    width: 100%;
    border-right: none;
    text-align: start;
    text-justify: middle;
}
.right-menu {
    width: 130px;
    position: fixed;
    z-index: 1000;
    background-color: white;
}

i {
    font-size: 25px;
    color: var(--color-main);
}
</style>