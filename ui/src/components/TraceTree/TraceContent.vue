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
  <div class="detail-section-timeline" style="display: flex; flex-direction: column">
    <MinTimeline
      v-show="minTimelineVisible"
      :spanList="spanList"
      :minTimestamp="minTimestamp"
      :maxTimestamp="maxTimestamp"
      @updateSelectedMaxTimestamp="handleSelectedMaxTimestamp"
      @updateSelectedMinTimestamp="handleSelectedMinTimestamp"
    />
    <div class="timeline-tool">
      <el-button :icon="DCaret" size="small" @click="toggleMinTimeline" />
    </div>
    <TableGraph
      :data="traceData"
      :selectedMaxTimestamp="selectedMaxTimestamp"
      :selectedMinTimestamp="selectedMinTimestamp"
      :minTimestamp="minTimestamp"
      :maxTimestamp="maxTimestamp"
    />
  </div>
</template>

<script setup>
  import { ref, computed } from 'vue';
  import { DCaret } from '@element-plus/icons-vue';
  import MinTimeline from './MinTimeline.vue';
  import TableGraph from './Table/Index.vue';

  const props = defineProps({
    trace: Object,
  });
  const traceData = computed(() => {
    return props.trace.spans.map((span) => convertTree(span));
  });
  const spanList = computed(() => {
    return getAllNodes({ label: 'TRACE_ROOT', children: traceData.value });
  });
  // Time range like xScale domain [0, max]
  const minTimestamp = computed(() => {
    if (!traceData.value.length) return 0;
    return Math.min(...spanList.value.filter((s) => s.startTime > 0).map((s) => s.startTime));
  });

  const maxTimestamp = computed(() => {
    const timestamps = spanList.value.map((span) => span.endTime || 0);
    if (timestamps.length === 0) return 0;

    return Math.max(...timestamps);
  });
  const selectedMaxTimestamp = ref(maxTimestamp.value);
  const selectedMinTimestamp = ref(minTimestamp.value);
  const minTimelineVisible = ref(true);

  function handleSelectedMaxTimestamp(value) {
    selectedMaxTimestamp.value = value;
  }

  function handleSelectedMinTimestamp(value) {
    selectedMinTimestamp.value = value;
  }

  function toggleMinTimeline() {
    minTimelineVisible.value = !minTimelineVisible.value;
  }

  function convertTree(d, spans) {
    d.endTime = new Date(d.endTime).getTime();
    d.startTime = new Date(d.startTime).getTime();
    d.duration = Number(d.duration);
    d.label = d.message;

    if (d.children && d.children.length > 0) {
      for (const i of d.children) {
        i.endTime = new Date(i.endTime).getTime();
        i.startTime = new Date(i.startTime).getTime();
      }
      for (const i of d.children) {
        convertTree(i, spans);
      }
    }

    return d;
  }
  function getAllNodes(tree) {
    const nodes = [];
    const stack = [tree];

    while (stack.length > 0) {
      const node = stack.pop();
      nodes.push(node);

      if (node?.children && node.children.length > 0) {
        for (let i = node.children.length - 1; i >= 0; i--) {
          stack.push(node.children[i]);
        }
      }
    }

    return nodes;
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

  .timeline-tool {
    justify-content: end;
    padding: 10px 5px;
    border-bottom: 1px solid var(--el-border-color-light);
    display: flex;
    flex-direction: row;
  }
</style>
