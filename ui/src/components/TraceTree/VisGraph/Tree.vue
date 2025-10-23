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
  <div class="trace-tree-charts flex-v">
    <div style="padding: 10px 0">
      <a class="trace-tree-btn mr-10" @click="charts.tree.setDefault()">
        Default
      </a>
      <a class="trace-tree-btn mr-10" @click="charts.tree.getTopSlow()">
        Top Slow
      </a>
      <a class="trace-tree-btn mr-10" @click="charts.tree.getTopChild()">
        Top Children
      </a>
    </div>
    <div class="trace-tree">
      <Graph
        ref="charts"
        :data="data"
        :traceId="traceId"
        :type="TraceGraphType.TREE"
        :selectedMaxTimestamp="selectedMaxTimestamp"
        :selectedMinTimestamp="selectedMinTimestamp"
        :minTimestamp="minTimestamp"
        :maxTimestamp="maxTimestamp"
      />
    </div>
  </div>
</template>
<script setup>
  import Graph from "./D3Graph/Index.vue";
  import { ref } from "vue";
  import { TraceGraphType } from "./constant";

  defineProps({
    data: Array,
    traceId: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
    minTimestamp: Number,
    maxTimestamp: Number,
  });
  const charts = ref(null);
</script>
<style lang="scss" scoped>
  .trace-tree {
    min-height: 400px;
    overflow: auto;
  }

  .trace-tree-btn {
    display: inline-block;
    border-radius: 4px;
    padding: 0 7px;
    background-color: #40454e;
    color: #eee;
    font-size: 11px;
  }

  .trace-tree-charts {
    overflow: auto;
    padding: 10px;
    // position: relative;
    height: 100%;
    width: 100%;
  }

  .time-charts-item {
    display: inline-block;
    padding: 2px 8px;
    border: 1px solid;
    font-size: 11px;
    border-radius: 4px;
    margin: 3px;
  }
</style>
