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
  <div v-if="type === TraceGraphType.STATISTICS">
    <div class="trace-item">
      <div :class="['method']">
        <el-tooltip :content="data.groupRef?.endpointName" placement="top" :show-after="300">
          <span>
            {{ data.groupRef?.endpointName }}
          </span>
        </el-tooltip>
      </div>
      <div :class="['type']">
        <el-tooltip :content="data.groupRef?.type" placement="top" :show-after="300">
          <span>
            {{ data.groupRef?.type }}
          </span>
        </el-tooltip>
      </div>
      <div class="max-time">
        {{ data.maxTime }}
      </div>
      <div class="min-time">
        {{ data.minTime }}
      </div>
      <div class="sum-time">
        {{ data.sumTime }}
      </div>
      <div class="avg-time">
        {{ parseInt(data.avgTime || "0") }}
      </div>
      <div class="count">
        {{ data.count }}
      </div>
    </div>
  </div>
  <div v-else>
    <div
      :class="[
        'trace-item',
        'level' + ((data.level || 0) - 1),
        { 'trace-item-error': data.isError },
        { profiled: data.profiled === false },
        `trace-item-${data.key}`,
        { highlighted: inTimeRange },
      ]"
      :data-text="data.profiled === false ? 'No Thread Dump' : ''"
      @click="hideActionBox"
    >
      <div
        :class="['method', 'level' + ((data.level || 0) - 1)]"
        :style="{
          'text-indent': ((data.level || 0) - 1) * 10 + 'px',
          width: `${method}px`,
        }"
        @click="selectSpan"
        @click.stop
      >
        <el-icon
          :style="!displayChildren ? 'transform: rotate(-90deg);' : ''"
          @click.stop="toggle"
          v-if="data.children && data.children.length"
          class="mr-5"
          @click="hideActionBox"
        >
          <ArrowDown />
        </el-icon>
      </div>
      <!-- <div class="start-time">
        {{ data.startTime ? dateFormat(data.startTime) : "" }}
      </div> -->
      <div class="exec-ms">
        {{ data.endTime - data.startTime ? data.endTime - data.startTime : "0" }}
      </div>
      <div class="exec-percent">
        <div class="outer-progress_bar" :style="{ width: outterPercent }">
          <div class="inner-progress_bar" :style="{ width: innerPercent }"></div>
        </div>
      </div>
      <div class="self">
        {{ data.dur ? data.dur + "" : "0" }}
      </div>
    </div>
    <div v-show="data.children && data.children.length > 0 && displayChildren" class="children-trace">
      <table-item
        v-for="(child, index) in data.children"
        :method="method"
        :key="index"
        :data="child"
        :type="type"
        :headerType="headerType"
        :selectedMaxTimestamp="selectedMaxTimestamp"
        :selectedMinTimestamp="selectedMinTimestamp"
        @selectedSpan="selectItem"
      />
    </div>
  </div>
</template>
<script setup>
  import { ref, computed } from "vue";
  import { ArrowDown } from "@element-plus/icons-vue";
  import { TraceGraphType } from "../VisGraph/constant.js";

  const emits = defineEmits(["selectedSpan"]);
  const props = defineProps({
    data: Object,
    method: Number,
    type: String,
    headerType: String,
    traceId: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const displayChildren = ref(true);
  const showDetail = ref(false);
  const selfTime = computed(() => (props.data.dur ? props.data.dur : 0));
  const execTime = computed(() =>
    props.data.endTime - props.data.startTime > 0 ? props.data.endTime - props.data.startTime : 0,
  );
  const outterPercent = computed(() => {
    if (props.data.level === 1) {
      return "100%";
    } else {
      const { data } = props;
      let result = (execTime.value / (data.totalExec || 0)) * 100;
      result = result > 100 ? 100 : result;
      const resultStr = result.toFixed(4) + "%";
      return resultStr === "0.0000%" ? "0.9%" : resultStr;
    }
  });
  const innerPercent = computed(() => {
    const result = (selfTime.value / execTime.value) * 100;
    const resultStr = result.toFixed(4) + "%";
    return resultStr === "0.0000%" ? "0.9%" : resultStr;
  });
  const inTimeRange = computed(() => {
    if (props.selectedMinTimestamp === undefined || props.selectedMaxTimestamp === undefined) {
      return true;
    }

    return props.data.startTime <= props.selectedMaxTimestamp && props.data.endTime >= props.selectedMinTimestamp;
  });
  function toggle() {
    displayChildren.value = !displayChildren.value;
  }
  function selectItem(span) {
    emits("selectedSpan", span);
  }
  function showSelectSpan(dom) {
    if (!dom) {
      return;
    }
    const items = Array.from(document.querySelectorAll(".trace-item"));
    for (const item of items) {
      item.style.background = "transparent";
    }
    dom.style.background = "var(--sw-trace-table-selected)";
    const p = document.getElementsByClassName("profiled")[0];
    if (p) {
      p.style.background = "var(--border-color-primary)";
    }
  }
  function selectSpan(event) {
    emits("selectedSpan", props.data);
    const dom = event
      .composedPath()
      .find((d) => d.className.includes("trace-item"));
    selectedItem(props.data);
    viewSpanDetail(dom);
    if (props.type === TraceGraphType.STATISTICS) {
      return;
    }
    const item = document.querySelector("#trace-action-box");
    const tableBox = document.querySelector(".trace-table-charts")?.getBoundingClientRect();
    if (!tableBox || !item) {
      return;
    }
    const offsetX = event.x - tableBox.x;
    const offsetY = event.y - tableBox.y;
    item.style.display = "block";
    item.style.top = `${offsetY + 20}px`;
    item.style.left = `${offsetX + 10}px`;
  }
  function viewSpan(event) {
    showDetail.value = true;
    const dom = event
      .composedPath()
      .find((d) => d.className.includes("trace-item"));
    selectedItem(props.data);
    viewSpanDetail(dom);
  }
  function selectedItem(span) {
    emits("selectedSpan", span);
  }
  function viewSpanDetail(dom) {
    showSelectSpan(dom);
    if (props.type === TraceGraphType.STATISTICS) {
      showDetail.value = true;
    }
  }
  function hideActionBox() {
    const item = document.querySelector("#trace-action-box");
    if (item) {
      item.style.display = "none";
    }
  }
</script>
<style lang="scss" scoped>
  @import url("./table.scss");

  .event-tag {
    width: 12px;
    height: 12px;
    border-radius: 12px;
    border: 1px solid #e66;
    color: #e66;
    display: inline-block;
  }

  .trace-item.level0 {
    &:hover {
      background: rgb(0 0 0 / 4%);
    }
  }

  .highlighted {
    color: var(--el-color-primary);
  }

  .profiled {
    background-color: var(--sw-table-header);
    position: relative;
  }

  .profiled::before {
    content: attr(data-text);
    position: absolute;
    top: 30px;
    left: 220px;
    width: 100px;
    padding: 10px;
    border-radius: 5px;
    border: 1px solid var(--disabled-color);
    background-color: var(--font-color);
    color: var(--text-color);
    text-align: center;
    box-shadow: var(--box-shadow-color) 0 2px 3px;
    display: none;
  }

  .profiled::after {
    content: "";
    position: absolute;
    left: 250px;
    top: 20px;
    border: 6px solid var(--font-color);
    border-color: transparent transparent var(--font-color);
    display: none;
  }

  .profiled:hover::before,
  .profiled:hover::after {
    display: block;
    z-index: 999;
  }

  .trace-item-error {
    color: #e54c17;
  }

  .trace-item {
    white-space: nowrap;
    position: relative;
  }

  .trace-item.selected {
    background-color: var(--sw-list-selected);
  }

  .trace-item:not(.level0):hover {
    background-color: var(--sw-list-hover);
  }

  .trace-item > div {
    padding: 0 5px;
    display: inline-block;
    border: 1px solid transparent;
    border-right: 1px dotted silver;
    overflow: hidden;
    line-height: 30px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .trace-item > div.method {
    padding-left: 10px;
    cursor: pointer;
  }

  .trace-item div.exec-percent {
    width: 100px;
    height: 30px;
    padding: 0 8px;

    .outer-progress_bar {
      width: 100%;
      height: 6px;
      border-radius: 3px;
      background: rgb(63 177 227);
      position: relative;
      margin-top: 11px;
      border: none;
    }

    .inner-progress_bar {
      position: absolute;
      background: rgb(110 64 170);
      height: 4px;
      border-radius: 2px;
      left: 0;
      border: none;
      top: 1px;
    }
  }

  .link-span {
    text-decoration: underline;
  }
</style>
