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
  <div class="trace-list-charts">
      <Graph
        :data="data"
        :traceId="traceId"
        :type="TraceGraphType.LIST"
        :selectedMaxTimestamp="selectedMaxTimestamp"
        :selectedMinTimestamp="selectedMinTimestamp"
        :minTimestamp="minTimestamp"
        :maxTimestamp="maxTimestamp"
      />
    </div>
</template>
<script setup>
  import Graph from "./D3Graph/Index.vue";
  import { TraceGraphType } from "./constant";

  defineProps({
    data: Array,
    traceId: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
    minTimestamp: Number,
    maxTimestamp: Number,
    type: String,
  });
</script>
<style lang="scss" scoped>
  .trace-list-charts {
    overflow: auto;
    padding: 10px;
    height: 100%;
    width: 100%;
  }
</style>
