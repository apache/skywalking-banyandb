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
  <div class="trace-min-timeline">
    <div class="timeline-marker-fixed">
      <svg width="100%" height="20px">
        <MinTimelineMarker :minTimestamp="minTimestamp" :maxTimestamp="maxTimestamp" :lineHeight="20" />
      </svg>
    </div>
    <div
      class="timeline-content scroll_bar_style"
      :style="{ paddingRight: (trace.spans.length + 1) * rowHeight < 200 ? '20px' : '14px' }"
    >
      <svg ref="svgEle" width="100%" :height="`${(trace.spans.length + 1) * rowHeight}px`">
        <MinTimelineOverlay
          :minTimestamp="minTimestamp"
          :maxTimestamp="maxTimestamp"
          @setSelectedMinTimestamp="setSelectedMinTimestamp"
          @setSelectedMaxTimestamp="setSelectedMaxTimestamp"
        />
        <MinTimelineSelector
          :minTimestamp="minTimestamp"
          :maxTimestamp="maxTimestamp"
          :selectedMinTimestamp="selectedMinTimestamp"
          :selectedMaxTimestamp="selectedMaxTimestamp"
          @setSelectedMinTimestamp="setSelectedMinTimestamp"
          @setSelectedMaxTimestamp="setSelectedMaxTimestamp"
        />
        <g
          v-for="(item, index) in trace.spans"
          :key="index"
          :transform="`translate(0, ${(index + 1) * rowHeight + 3})`"
        >
          <SpanNode :span="item" :minTimestamp="minTimestamp" :maxTimestamp="maxTimestamp" :depth="index + 1" />
        </g>
      </svg>
    </div>
  </div>
</template>
<script setup>
  import { ref } from "vue";
  import SpanNode from "./SpanNode.vue";
  import MinTimelineMarker from "./MinTimelineMarker.vue";
  import MinTimelineOverlay from "./MinTimelineOverlay.vue";
  import MinTimelineSelector from "./MinTimelineSelector.vue";

  const props = defineProps({
    trace: Object,
    minTimestamp: Number,
    maxTimestamp: Number,
  });
  const svgEle = ref(null);
  const rowHeight = 12;

  const selectedMinTimestamp = ref(props.minTimestamp);
  const selectedMaxTimestamp = ref(props.maxTimestamp);

  const emit = defineEmits(["updateSelectedMaxTimestamp", "updateSelectedMinTimestamp"]);

  const setSelectedMinTimestamp = (value) => {
    selectedMinTimestamp.value = value;
    emit("updateSelectedMinTimestamp", value);
  };
  const setSelectedMaxTimestamp = (value) => {
    selectedMaxTimestamp.value = value;
    emit("updateSelectedMaxTimestamp", value);
  };
</script>
<style lang="scss" scoped>
  .trace-min-timeline {
    width: 100%;
    max-height: 200px;
    border-bottom: 1px solid var(--el-border-color-light);
    display: flex;
    flex-direction: column;
  }

  .timeline-marker-fixed {
    width: 100%;
    padding-right: 20px;
    padding-top: 5px;
    background: var(--el-bg-color);
    border-bottom: 1px solid var(--el-border-color-light);
    z-index: 1;
  }

  .timeline-content {
    flex: 1;
    width: 100%;
    overflow: auto;
  }
</style>
