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
  <g v-for="(marker, index) in markers" :key="marker.duration">
    <line
      :x1="`${marker.position}%`"
      :y1="0"
      :x2="`${marker.position}%`"
      :y2="lineHeight ? `${lineHeight}` : '100%'"
      stroke="var(--el-border-color-light)"
    />
    <text
      :key="`label-${marker.duration}`"
      :x="`${marker.position}%`"
      :y="12"
      font-size="10"
      fill="#999"
      text-anchor="right"
      :transform="`translate(${index === markers.length - 1 ? -50 : 5}, 0)`"
    >
      {{ marker.duration }}ms
    </text>
  </g>
</template>
<script setup>
  import { computed } from "vue";

  const props = defineProps({
    minTimestamp: Number,
    maxTimestamp: Number,
    lineHeight: [Number, String],
  });
  const markers = computed(() => {
    const maxDuration = props.maxTimestamp - props.minTimestamp;
    const markerDurations = [0, (maxDuration * 1) / 3, (maxDuration * 2) / 3, maxDuration];

    return markerDurations.map((duration) => ({
      duration: duration.toFixed(2),
      position: maxDuration > 0 ? (duration / maxDuration) * 100 : 0,
    }));
  });
</script>
<style scoped></style>
