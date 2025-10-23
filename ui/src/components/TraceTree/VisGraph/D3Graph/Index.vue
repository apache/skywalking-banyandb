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
  <div class="trace-t-loading" v-show="loading">
    <Icon iconName="spinner" size="sm" />
  </div>
  <TableContainer
    v-if="type === TraceGraphType.TABLE"
    :tableData="segmentId"
    :type="type"
    :headerType="headerType"
    :traceId="traceId"
    :selectedMaxTimestamp="selectedMaxTimestamp"
    :selectedMinTimestamp="selectedMinTimestamp"
    @select="handleSelectSpan"
  >
    <div class="trace-tips" v-if="!segmentId.length">{{ $t("noData") }}</div>
  </TableContainer>
  <div v-else ref="traceGraph" class="d3-graph"></div>
  <div id="trace-action-box">
    <div @click="viewSpanDetails">Span details</div>
    <div v-for="span in parentSpans" :key="span.segmentId" @click="viewParentSpan(span)">
      {{ `Parent span: ${span.endpointName} -> Start time: ${visDate(span.startTime)}` }}
    </div>
    <div v-for="span in refParentSpans" :key="span.segmentId" @click="viewParentSpan(span)">
      {{ `Ref to span: ${span.endpointName} -> Start time: ${visDate(span.startTime)}` }}
    </div>
  </div>
</template>
<script setup>
  import { ref, watch, onBeforeUnmount, onMounted, nextTick } from "vue";
  import * as d3 from "d3";
  import dayjs from "dayjs";
  import ListGraph from "./utils/d3-trace-list";
  import TreeGraph from "./utils/d3-trace-tree";
  // import SpanDetail from "./SpanDetail.vue";
  import TableContainer from "../../Table/TableContainer.vue";
  import { debounce } from "@/utils/debounce";
  import { mutationObserver } from "@/utils/mutation";
  import { TraceGraphType } from "../constant";
  import { buildSegmentForest, collapseTree, getRefsAllNodes } from "./utils/helper";

  const props = defineProps({
    data: Array,
    traceId: String,
    type: String,
    headerType: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
    minTimestamp: Number,
    maxTimestamp: Number,
  });
  const emits = defineEmits(["select"]);
  const loading = ref(false);
  const showDetail = ref(false);
  const fixSpansSize = ref(0);
  const segmentId = ref([]);
  const currentSpan = ref(null);
  const refSpans = ref([]);
  const tree = ref(null);
  const traceGraph = ref(null);
  const parentSpans = ref([]);
  const refParentSpans = ref([]);
  const debounceFunc = debounce(draw, 500);
  // Store previous timestamp values to check for significant changes
  const prevSelectedMaxTimestamp = ref(props.selectedMaxTimestamp || 0);
  const prevSelectedMinTimestamp = ref(props.selectedMinTimestamp || 0);

  const visDate = (date, pattern = "YYYY-MM-DD HH:mm:ss:SSS") => dayjs(date).format(pattern);
  // Debounced version of onSpanPanelToggled to prevent excessive re-renders
  const debouncedOnSpanPanelToggled = debounce(draw, 150);

  // Check if timestamp change is significant enough to warrant a redraw
  function isTimestampChangeSignificant(newMax, newMin) {
    const maxDiff = Math.abs(newMax - prevSelectedMaxTimestamp.value);
    const minDiff = Math.abs(newMin - prevSelectedMinTimestamp.value);
    const totalRange = props.maxTimestamp - props.minTimestamp;

    // Consider change significant if it's more than 0.1% of the total range
    const threshold = totalRange * 0.001;

    return maxDiff > threshold || minDiff > threshold;
  }
  onMounted(async () => {
    loading.value = true;
    changeTree();
    await nextTick();
    draw();
    loading.value = false;
    // monitor segment list width changes.
    mutationObserver.create("trigger-resize", () => {
      d3.selectAll(".d3-tip").remove();
      debounceFunc();
    });
    window.addEventListener("resize", debounceFunc);
    window.addEventListener("spanPanelToggled", draw);
  });

  function draw() {
    if (props.type === TraceGraphType.TABLE) {
      segmentId.value = setLevel(segmentId.value);
      return;
    }
    if (!traceGraph.value) {
      return;
    }
    d3.selectAll(".d3-tip").remove();
    if (props.type === TraceGraphType.LIST) {
      tree.value = new ListGraph({ el: traceGraph.value, handleSelectSpan: handleSelectSpan });
      tree.value.init({
        data: { label: "TRACE_ROOT", children: segmentId.value },
        row: getRefsAllNodes({ label: "TRACE_ROOT", children: segmentId.value }),
        fixSpansSize: fixSpansSize.value,
        selectedMaxTimestamp: props.selectedMaxTimestamp,
        selectedMinTimestamp: props.selectedMinTimestamp,
      });
      tree.value.draw();
      selectInitialSpan();
      return;
    }
    if (props.type === TraceGraphType.TREE) {
      tree.value = new TreeGraph({ el: traceGraph.value, handleSelectSpan });
      tree.value.init({
        data: { label: `${props.traceId}`, children: segmentId.value },
        row: getRefsAllNodes({ label: "TRACE_ROOT", children: segmentId.value }),
        selectedMaxTimestamp: props.selectedMaxTimestamp,
        selectedMinTimestamp: props.selectedMinTimestamp,
      });
    }
  }
  function selectInitialSpan() {
    if (segmentId.value && segmentId.value.length > 0) {
      const root = segmentId.value[0];
      // traceStore.setCurrentSpan(root);
      if (tree.value && typeof tree.value.highlightSpan === "function") {
        tree.value.highlightSpan(root);
      }
    }
  }
  function handleSelectSpan(i) {
    const spans = [];
    const refSpans = [];
    parentSpans.value = [];
    refParentSpans.value = [];
    if (props.type === TraceGraphType.TABLE) {
      currentSpan.value = i;
      emits("select", i);
    } else {
      currentSpan.value = i.data;
    }
    if (!currentSpan.value) {
      return;
    }
    for (const ref of currentSpan.value.refs || []) {
      refSpans.push(ref);
    }
    if (currentSpan.value.parentSpanId > -1) {
      spans.push({
        parentSegmentId: currentSpan.value.segmentId,
        parentSpanId: currentSpan.value.parentSpanId,
        traceId: currentSpan.value.traceId,
      });
    }
    for (const span of refSpans) {
      const item = props.data.find(
        (d) => d.segmentId === span.parentSegmentId && d.spanId === span.parentSpanId && d.traceId === span.traceId,
      );
      item && refParentSpans.value.push(item);
    }
    for (const span of spans) {
      const item = props.data.find(
        (d) => d.segmentId === span.parentSegmentId && d.spanId === span.parentSpanId && d.traceId === span.traceId,
      );
      item && parentSpans.value.push(item);
    }
  }
  function viewParentSpan(span) {
    if (props.type === TraceGraphType.TABLE) {
      setTableSpanStyle(span);
      return;
    }
    tree.value.highlightParents(span);
  }
  function viewSpanDetails() {
    showDetail.value = true;
    hideActionBox();
  }
  function setTableSpanStyle(span) {
    const itemDom = document.querySelector(`.trace-item-${span.key}`);
    const items = Array.from(document.querySelectorAll(".trace-item"));
    for (const item of items) {
      item.style.background = "transparent";
    }
    if (itemDom) {
      itemDom.style.background = "var(--sw-trace-table-selected)";
    }
    hideActionBox();
  }
  function hideActionBox() {
    const box = document.querySelector("#trace-action-box");
    box.style.display = "none";
  }
  function changeTree() {
    if (!props.data.length) {
      return [];
    }
    const { roots, fixSpansSize: fixSize, refSpans: refs } = buildSegmentForest(props.data, props.traceId);
    segmentId.value = roots;
    fixSpansSize.value = fixSize;
    refSpans.value = refs;
    for (const root of segmentId.value) {
      collapseTree(root, refSpans.value);
    }
  }
  function setLevel(arr, level = 1, totalExec) {
    for (const item of arr) {
      item.level = level;
      totalExec = totalExec || item.endTime - item.startTime;
      item.totalExec = totalExec;
      if (item.children && item.children.length > 0) {
        setLevel(item.children, level + 1, totalExec);
      }
    }
    return arr;
  }

  onBeforeUnmount(() => {
    d3.selectAll(".d3-tip").remove();
    window.removeEventListener("resize", debounceFunc);
    mutationObserver.deleteObserve("trigger-resize");
    window.removeEventListener("spanPanelToggled", draw);
  });
  watch(
    () => props.data,
    () => {
      if (!props.data.length) {
        return;
      }
      loading.value = true;
      changeTree();
      draw();
      loading.value = false;
    },
  );
  watch(
    () => [props.selectedMaxTimestamp, props.selectedMinTimestamp],
    ([newMax, newMin]) => {
      // Only trigger redraw if the change is significant
      if (isTimestampChangeSignificant(newMax, newMin)) {
        // Update previous values
        prevSelectedMaxTimestamp.value = newMax;
        prevSelectedMinTimestamp.value = newMin;

        // Use debounced version to prevent excessive re-renders
        debouncedOnSpanPanelToggled();
      }
    },
  );
</script>
<style lang="scss">
  .d3-graph {
    height: 100%;
  }

  .trace-node .group {
    cursor: pointer;
    fill-opacity: 0;
  }

  .trace-node .node-text {
    font: 12px sans-serif;
    pointer-events: none;
  }

  .trace-node.highlighted .node-text,
  .trace-node.highlightedParent .node-text {
    font-weight: bold;
    fill: #409eff;
  }

  .highlightedParent .node,
  .highlighted .node {
    stroke-width: 4;
    fill: var(--font-color);
    stroke: var(--font-color);
  }

  .trace-node.highlighted .trace-node-text,
  .trace-node.highlightedParent .trace-node-text {
    font-weight: bold;
    fill: #409eff;
  }

  #trace-action-box {
    position: absolute;
    color: var(--font-color);
    cursor: pointer;
    border: var(--sw-topology-border);
    border-radius: 3px;
    background-color: var(--theme-background);
    padding: 10px 0;
    display: none;

    div {
      height: 30px;
      line-height: 30px;
      text-align: left;
      padding: 0 15px;
    }

    div:hover {
      color: var(--active-color);
      background-color: #eee;
    }
  }
</style>
