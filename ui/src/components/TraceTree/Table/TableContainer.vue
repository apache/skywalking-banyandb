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
    <div class="trace-table-header" v-if="type === TraceGraphType.STATISTICS">
      <div :class="item.label" v-for="(item, index) in headerData" :key="index">
        {{ item.value }}
        <span
          class="r cp"
          @click="sortStatistics(item.key || '')"
          :key="componentKey"
          v-if="item.key !== 'endpointName' && item.key !== 'type'"
        >
          <el-icon><DCaret /></el-icon>
        </span>
      </div>
    </div>
    <div class="trace-table-header" v-else>
      <div class="method" :style="`width: ${method}px`">
        <span class="cp dragger" ref="dragger">
          <el-icon><ArrowLeft /></el-icon>
          <el-icon><MoreFilled /></el-icon>
          <el-icon><ArrowRight /></el-icon>
        </span>
        {{ headerData[0].value }}
      </div>
      <div :class="item.label" v-for="(item, index) in headerData.slice(1)" :key="index">
        {{ item.value }}
      </div>
    </div>
    <TableItem
      :method="method"
      :traceId="traceId"
      v-for="(item, index) in tableData"
      :data="item"
      :key="`key${index}`"
      :type="type"
      :headerType="headerType"
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
  import { TraceConstant, StatisticsConstant } from "./data.js";
  import { TraceGraphType } from "../VisGraph/constant.js";
  import { ArrowLeft, MoreFilled, DCaret, ArrowRight } from "@element-plus/icons-vue";

  const props = defineProps({
    tableData: Array,
    type: String,
    headerType: String,
    traceId: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const emits = defineEmits(["select"]);

  const method = ref(300);
  const componentKey = ref(300);
  const flag = ref(true);
  const dragger = ref(null);
  let headerData = TraceConstant;

  if (props.type === TraceGraphType.STATISTICS) {
    headerData = StatisticsConstant;
  }
  onMounted(() => {
    if (props.type === TraceGraphType.STATISTICS) {
      return;
    }
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
  function sortStatistics(key) {
    const element = props.tableData;
    for (let i = 0; i < element.length; i++) {
      for (let j = 0; j < element.length - i - 1; j++) {
        let val1;
        let val2;
        if (key === "maxTime") {
          val1 = element[j].maxTime;
          val2 = element[j + 1].maxTime;
        }
        if (key === "minTime") {
          val1 = element[j].minTime;
          val2 = element[j + 1].minTime;
        }
        if (key === "avgTime") {
          val1 = element[j].avgTime;
          val2 = element[j + 1].avgTime;
        }
        if (key === "sumTime") {
          val1 = element[j].sumTime;
          val2 = element[j + 1].sumTime;
        }
        if (key === "count") {
          val1 = element[j].count;
          val2 = element[j + 1].count;
        }
        if (flag.value) {
          if (val1 && val2 && val1 < val2) {
            const tmp = element[j];
            element[j] = element[j + 1];
            element[j + 1] = tmp;
          }
        } else {
          if (val1 && val2 && val1 > val2) {
            const tmp = element[j];
            element[j] = element[j + 1];
            element[j + 1] = tmp;
          }
        }
      }
    }
    componentKey.value += 1;
    flag.value = !flag.value;
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
