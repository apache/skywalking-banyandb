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

<template>
  <div class="trace-table">
    <div class="trace-table-header">
      <div class="method" :style="`width: ${method}px`">
        <span class="dragger" ref="dragger">
          <el-icon><ArrowLeft /></el-icon>
          <el-icon><MoreFilled /></el-icon>
          <el-icon><ArrowRight /></el-icon>
        </span>
        {{ TraceConstant[0].value }}
      </div>
      <div :class="item.label" v-for="(item, index) in TraceConstant.slice(1)" :key="index">
        {{ item.value }}
      </div>
    </div>
    <TableItem
      :method="method"
      v-for="(item, index) in tableData"
      :data="item"
      :key="`key${index}`"
      :selectedMaxTimestamp="selectedMaxTimestamp"
      :selectedMinTimestamp="selectedMinTimestamp"
      @selectedSpan="selectItem"
    />
    <slot></slot>
  </div>
</template>
<script setup>
  import { ref, onMounted } from "vue";
  import TableItem from "./TableItem.vue";
  import { TraceConstant } from "./data.js";
  import { ArrowLeft, MoreFilled, ArrowRight } from "@element-plus/icons-vue";

  const props = defineProps({
    tableData: Array,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const emits = defineEmits(["select"]);

  const method = ref(300);
  const dragger = ref(null);

  onMounted(() => {
    const drag = dragger.value;
    if (!drag) {
      return;
    }
    drag.onmousedown = (event) => {
      event.stopPropagation();
      const diffX = event.clientX;
      const copy = method.value;
      document.onmousemove = (documentEvent) => {
        const moveX = documentEvent.clientX - diffX;
        method.value = copy + moveX;
      };
      document.onmouseup = () => {
        document.onmousemove = null;
        document.onmouseup = null;
      };
    };
  });
  function selectItem(span) {
    emits("select", span);
  }
</script>
<style lang="scss" scoped>
  @import url("./table.scss");

  .trace-table {
    font-size: 12px;
    height: 100%;
    overflow: auto;
    width: 100%;
  }

  .dragger {
    float: right;
    cursor: move;
  }

  .trace-table-header {
    white-space: nowrap;
    user-select: none;
    border-left: 0;
    border-right: 0;
    border-bottom: 1px solid var(--sw-trace-list-border);
  }

  .trace-table-header div {
    display: inline-block;
    background-color: var(--sw-table-header);
    padding: 0 4px;
    border: 1px solid transparent;
    border-right: 1px dotted silver;
    overflow: hidden;
    line-height: 30px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
