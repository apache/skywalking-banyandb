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
  <div class="trace-query-content">
    <div class="trace-info">
      <div class="flex-h" style="justify-content: space-between">
        <h3>{{ trace.label }}</h3>
        <div class="flex-h">
          <el-dropdown @command="handleDownload" trigger="click">
            <el-button size="small">
              Download
              <el-icon class="el-icon--right"><arrow-down /></el-icon>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="json">Download JSON</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </div>
      <div class="trace-meta flex-h">
        <div>
          <span class="grey mr-5">Duration</span>
          <span class="value">{{ trace.duration }}ms</span>
        </div>
        <div>
          <span class="grey mr-5">Total Spans</span>
          <span class="value">{{ trace.children?.length || 0 }}</span>
        </div>
        <div class="trace-id-container flex-h" style="align-items: center">
          <span class="grey mr-5">Trace ID</span>
          <span class="value">{{ trace.traceId }}</span>
          <span class="value ml-5 cp" @click="handleCopyTraceId">
            <el-icon><CopyDocument /></el-icon>
          </span>
        </div>
      </div>
    </div>
    <div class="flex-h">
      <div class="detail-section-timeline flex-v">
        <MinTimeline
          v-show="minTimelineVisible"
          :trace="trace"
          :minTimestamp="minTimestamp"
          :maxTimestamp="maxTimestamp"
          @updateSelectedMaxTimestamp="handleSelectedMaxTimestamp"
          @updateSelectedMinTimestamp="handleSelectedMinTimestamp"
        />
        <TimelineTool @toggleMinTimeline="toggleMinTimeline" @updateSpansGraphType="handleSpansGraphTypeUpdate" />
        <component
          :is="graphs[spansGraphType]"
          :data="trace.spans"
          :traceId="trace?.traceId"
          :showBtnDetail="false"
          headerType="Trace"
          :selectedMaxTimestamp="selectedMaxTimestamp"
          :selectedMinTimestamp="selectedMinTimestamp"
          :minTimestamp="minTimestamp"
          :maxTimestamp="maxTimestamp"
        />
      </div>
    </div>
  </div>
</template>

<script setup>
  import { ref, computed } from "vue";
  import { ElMessage } from "element-plus";
  import { CopyDocument } from "@element-plus/icons-vue";
  import MinTimeline from "./MinTimeline.vue";
  import { saveFileAsJSON } from "@/utils/file.js";
  import TimelineTool from "./TimelineTool.vue";
  import graphs from "./VisGraph/index";
  import { GraphTypeOptions } from "./VisGraph/constant";
  import copy from "@/utils/copy.js";

  const props = defineProps({
    trace: Object,
  });
  // Time range like xScale domain [0, max]
  const minTimestamp = computed(() => {
    if (!props.trace.spans.length) return 0;
    return Math.min(...props.trace.spans.map((s) => s.startTime || 0));
  });

  const maxTimestamp = computed(() => {
    const timestamps = props.trace.spans.map((span) => span.endTime || 0);
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

  function handleDownload() {
    const trace = props.trace;
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const baseFilename = `trace-${trace.traceId}-${timestamp}`;
    const spans = trace.spans.map((span) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { duration, label, ...newSpan } = span;
      return newSpan;
    });
    try {
      saveFileAsJSON(spans, `${baseFilename}.json`);
      ElMessage.success("Trace data downloaded as JSON");
    } catch (error) {
      console.error("Download error:", error);
      ElMessage.error("Failed to download file");
    }
  }
  function handleCopyTraceId() {
    if (!props.trace?.traceId) return;
    copy(props.trace?.traceId);
  }
</script>

<style lang="scss" scoped>
  .trace-info {
    padding-bottom: 15px;
    border-bottom: 1px solid var(--el-border-color-light);
  }

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
