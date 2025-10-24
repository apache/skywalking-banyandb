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
  <rect :x="`${startPct}%`" :y="0" :width="`${widthPct}%`" :height="barHeight" fill="#ccc" rx="2" ry="2" />
  <!-- Label and Duration Text -->
  <text
    v-if="showLabel"
    :x="0"
    :y="barHeight - 7"
    font-size="10"
    fill="var(--sw-font-grey-color)"
    text-anchor="start"
    class="span-label"
  >
    {{ span.label || "Unknown" }}
  </text>
  <text
    v-if="showDuration"
    :x="`${100}%`"
    :y="barHeight - 7"
    font-size="10"
    fill="var(--sw-font-grey-color)"
    text-anchor="end"
    class="span-duration"
  >
    {{ span.duration }}ms
  </text>
</template>

<script setup>
  import { computed } from "vue";

  const props = defineProps({
    span: Object,
    minTimestamp: Number,
    maxTimestamp: Number,
    depth: Number,
    showDuration: Boolean,
    showLabel: Boolean,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const barHeight = 3;

  const widthScale = computed(() => {
    const { selectedMinTimestamp, selectedMaxTimestamp, minTimestamp, maxTimestamp } = props;
    let max = maxTimestamp - minTimestamp;
    if (selectedMaxTimestamp !== undefined && selectedMinTimestamp !== undefined) {
      max = selectedMaxTimestamp - selectedMinTimestamp;
    }
    return (duration) => {
      const d = Math.max(0, duration || 0);
      return (d / max) * 100;
    };
  });
  const startPct = computed(() => {
    const { span, selectedMinTimestamp, minTimestamp } = props;
    const end = span.endTime;
    let start = span.startTime;
    if (selectedMinTimestamp !== undefined) {
      start = selectedMinTimestamp > start ? (end < selectedMinTimestamp ? 0 : selectedMinTimestamp) : start;
    }
    const dur = start - (selectedMinTimestamp || minTimestamp);

    return Math.max(0, widthScale.value(dur));
  });

  const widthPct = computed(() => {
    const { span, selectedMinTimestamp, selectedMaxTimestamp } = props;
    let start = span.startTime;
    let end = span.endTime;
    if (selectedMinTimestamp !== undefined) {
      start = selectedMinTimestamp > start ? selectedMinTimestamp : start;
      if (end < selectedMinTimestamp) {
        return 0;
      }
    }
    if (selectedMaxTimestamp !== undefined) {
      end = selectedMaxTimestamp < end ? selectedMaxTimestamp : end;
      if (span.startTime > selectedMaxTimestamp) {
        return 0;
      }
    }
    const dur = end - start;
    return Math.max(0, widthScale.value(dur));
  });
</script>

<style lang="scss" scoped>
  .span-label {
    font-weight: 500;
  }

  .span-duration {
    font-weight: 400;
  }
</style>
