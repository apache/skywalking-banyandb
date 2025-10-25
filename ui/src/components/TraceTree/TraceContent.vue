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
  <div class="detail-section-timeline" style="display: flex; flex-direction: column;">
    <MinTimeline
      v-show="minTimelineVisible"
      :spanList="spanList"
      :minTimestamp="minTimestamp"
      :maxTimestamp="maxTimestamp"
      @updateSelectedMaxTimestamp="handleSelectedMaxTimestamp"
      @updateSelectedMinTimestamp="handleSelectedMinTimestamp"
    />
    <TimelineTool @toggleMinTimeline="toggleMinTimeline" @updateSpansGraphType="handleSpansGraphTypeUpdate" />
    <component
      :is="graphs[spansGraphType]"
      :data="traceData"
      :traceId="trace?.traceId"
      :selectedMaxTimestamp="selectedMaxTimestamp"
      :selectedMinTimestamp="selectedMinTimestamp"
      :minTimestamp="minTimestamp"
      :maxTimestamp="maxTimestamp"
    />
  </div>
</template>

<script setup>
  import { ref, computed } from "vue";
  import MinTimeline from "./MinTimeline.vue";
  import TimelineTool from "./TimelineTool.vue";
  import graphs from "./VisGraph/index";
  import { GraphTypeOptions } from "./VisGraph/constant";
  import { getAllNodes } from "./VisGraph/D3Graph/utils/helper";

  const props = defineProps({
    trace: Object,
  });
  const traceData = computed(() => {
    return props.trace.spans.map(span => convertTree(span));
  });
  const spanList = computed(() => {
    return getAllNodes({ label: "TRACE_ROOT", children: traceData.value });
  });
  // Time range like xScale domain [0, max]
  const minTimestamp = computed(() => {
    if (!traceData.value.length) return 0;
    return Math.min(...spanList.value.filter(s => s.startTime > 0).map((s) => s.startTime));
  });

  const maxTimestamp = computed(() => {
    const timestamps = spanList.value.map((span) => span.endTime || 0);
    if (timestamps.length === 0) return 0;

    return Math.max(...timestamps);
  });
  const selectedMaxTimestamp = ref(maxTimestamp.value);
  const selectedMinTimestamp = ref(minTimestamp.value);
  const minTimelineVisible = ref(true);
  const spansGraphType = ref(GraphTypeOptions[2].value);

  function handleSelectedMaxTimestamp(value) {
    selectedMaxTimestamp.value = value;
  }

  function handleSelectedMinTimestamp(value) {
    selectedMinTimestamp.value = value;
  }

  function toggleMinTimeline() {
    minTimelineVisible.value = !minTimelineVisible.value;
  }

  function handleSpansGraphTypeUpdate(value) {
    spansGraphType.value = value;
  }

  function convertTree(d, spans) {
    d.endTime = new Date(d.endTime).getTime();
    d.startTime = new Date(d.startTime).getTime();
    d.duration = Number(d.duration);
    d.label = d.message;
    
    if (d.children && d.children.length > 0) {
      let dur = d.endTime - d.startTime;
      for (const i of d.children) {
        i.endTime = new Date(i.endTime).getTime();
        i.startTime = new Date(i.startTime).getTime();
        dur -= i.endTime - i.startTime;
      }
      d.dur = dur < 0 ? 0 : dur;
      for (const i of d.children) {
        convertTree(i, spans);
      }
    } else {
      d.dur = d.endTime - d.startTime;
    }
    
    return d;
  }
</script>

<style lang="scss" scoped>
  .trace-info h3 {
    margin: 0 0 10px;
    color: var(--el-text-color-primary);
    font-size: 18px;
    font-weight: 600;
  }

  .trace-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
  }

  .detail-section-timeline {
    width: 100%;
  }
</style>
