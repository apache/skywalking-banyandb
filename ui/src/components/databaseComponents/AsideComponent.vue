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
    <el-menu default-active="2" active-text-color="#6E38F7" style="height: 100%;" :collapse="isCollapse"
        :collapse-transition="false">
        <div v-for="(item, index) in groupLists" :key="item.metadata.name">
            <el-submenu :index="item.metadata.name">
                <template slot="title">
                    <i class="el-icon-folder"></i>
                    <span slot="title" :title="item.metadata.name" style="width: 70%" class="text-overflow-hidden">{{
                            item.metadata.name
                    }}</span>
                </template>
                <el-submenu :index="item.metadata.name + '-' + index + '-stream'">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title">Stream</span>
                    </template>
                    <el-menu-item>
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title" title="streamFile" style="width: 90%"
                                class="text-overflow-hidden">streamFile</span>
                        </template>
                    </el-menu-item>
                </el-submenu>
                <el-submenu :index="item.metadata.name + '-' + index + '-measure'">
                    <template slot="title">
                        <i class="el-icon-folder"></i>
                        <span slot="title">Measure</span>
                    </template>
                    <el-menu-item>
                        <template slot="title">
                            <i class="el-icon-document"></i>
                            <span slot="title" title="measureFile" style="width: 90%"
                                class="text-overflow-hidden">measureFile</span>
                        </template>
                    </el-menu-item>
                </el-submenu>
            </el-submenu>
        </div>
    </el-menu>
</template>
 
<script>
import { mapState } from 'vuex'
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
                "updatedAt": null
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
        }
    },
    async created() {
        console.log('this is aside created')
        this.getGroupLists()
    },

    computed: {
        ...mapState({
            isCollapse: (state) => state.aside.isCollapse
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

i {
    font-size: 25px;
    color: var(--color-main);
}

</style>