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
  <div class="justify-start flex">
    <el-tag class="pointer" size="medium" v-for="(item, index) in tags" :key="item.name" closable
      @click="changeMenu(item)" @close="handleClose(item, index)" effect="dark">{{ item.name }}
    </el-tag>
  </div>
</template>

<script>
import { mapState, mapMutations } from "vuex"
export default {
  name: 'TagNavigationComponent',
  computed: {
    ...mapState({
      tags: (state) => state.tags.tagsList,
    }),
  },
  created() {

  },
  methods: {
    ...mapMutations({
      close: 'closeTag',
    }),
    changeMenu(item) {
      console.log(item)
    },
    handleClose(item, index) {
      console.log(item, index)
    }
  }
}
</script>

<style lang="scss" scoped>
div {
  margin: 20px 20px 0 20px;

  .el-tag {
    margin-right: 15px;
  }
}
</style>