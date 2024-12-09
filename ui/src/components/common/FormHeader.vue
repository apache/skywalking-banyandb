<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->
<script setup>
defineProps({
  fields: {
    type: Object,
    default: () => ({}),
  },
});
</script>

<template>
  <div class="flex header-content">
    <span class="text-bold">Group: </span>
    <span class="item-value">{{ fields.group }}</span>
    <span class="text-bold" v-show="fields.name">Name: </span>
    <span class="item-value">{{ fields.name }}</span>
    <span class="text-bold" v-show="fields.type">{{fields.catalog ? `Catalog:` : `Type:`}}</span>
    <span class="item-value">{{ fields.type }}</span>
    <span class="text-bold">Operation: </span>
    <span class="item-value">{{ fields.operator == 'edit' ? fields.operator : `Read` }}</span>
  </div>
</template>

<style lang="scss" scoped>
:deep(.el-card) {
  margin: 15px;
}
.item-value {
  margin-right: 20px;
  margin-left: 5px;
}
.header-content {
  height: 30px;
  width: 100%;
  line-height: 30px;
}
</style>