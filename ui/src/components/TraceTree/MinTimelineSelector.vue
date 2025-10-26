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
  <g>
    <rect
      x="0"
      y="0"
      :width="`${boundaryLeft}%`"
      height="100%"
      fill="var(--el-color-primary-light-8)"
      fill-opacity="0.6"
      pointer-events="none"
    />
    <rect
      :x="`${boundaryRight}%`"
      y="0"
      :width="`${100 - boundaryRight}%`"
      height="100%"
      fill="var(--el-color-primary-light-8)"
      fill-opacity="0.6"
      pointer-events="none"
    />
    <rect
      :x="`${boundaryLeft}%`"
      y="0"
      width="3"
      height="100%"
      fill="var(--el-color-primary-light-5)"
      transform="translate(-1)"
      pointer-events="none"
    />
    <rect
      :x="`${boundaryRight}%`"
      y="0"
      width="3"
      height="100%"
      fill="var(--el-color-primary-light-5)"
      transform="translate(-1)"
      pointer-events="none"
    />

    <rect
      v-if="minMouseDownX !== undefined && minCurrentX !== undefined"
      :x="`${Math.min(minMouseDownX, minCurrentX)}%`"
      y="0"
      :width="`${Math.abs(minMouseDownX - minCurrentX)}%`"
      height="100%"
      fill="var(--el-color-primary-light-6)"
      fill-opacity="0.4"
      pointer-events="none"
    />
    <rect
      v-if="maxMouseDownX !== undefined && maxCurrentX !== undefined"
      :x="`${Math.min(maxMouseDownX, maxCurrentX)}%`"
      y="0"
      :width="`${Math.abs(maxMouseDownX - maxCurrentX)}%`"
      height="100%"
      fill="var(--el-color-primary-light-6)"
      fill-opacity="0.4"
      pointer-events="none"
    />
    <rect
      :x="`${boundaryLeft}%`"
      y="0"
      width="6"
      height="40%"
      fill="var(--el-color-primary)"
      @mousedown="minRangeHandler.onMouseDown"
      cursor="pointer"
      transform="translate(-3)"
    />
    <rect
      :x="`${boundaryRight}%`"
      y="0"
      width="6"
      height="40%"
      fill="var(--el-color-primary)"
      @mousedown="maxRangeHandler.onMouseDown"
      cursor="pointer"
      transform="translate(-3)"
    />
  </g>
</template>
<script setup>
  import { computed, ref, onMounted } from 'vue';
  import { useRangeTimestampHandler, adjustPercentValue } from './useHooks.js';

  const emit = defineEmits(['setSelectedMinTimestamp', 'setSelectedMaxTimestamp']);
  const props = defineProps({
    minTimestamp: Number,
    maxTimestamp: Number,
    selectedMinTimestamp: Number,
    selectedMaxTimestamp: Number,
  });
  const svgEle = ref(null);

  onMounted(() => {
    const element = document.querySelector('.trace-min-timeline svg');
    if (element) {
      svgEle.value = element;
    }
  });
  const maxOpositeX = computed(
    () => ((props.selectedMaxTimestamp - props.minTimestamp) / (props.maxTimestamp - props.minTimestamp)) * 100,
  );
  const minOpositeX = computed(
    () => ((props.selectedMinTimestamp - props.minTimestamp) / (props.maxTimestamp - props.minTimestamp)) * 100,
  );

  const minRangeHandler = computed(() => {
    return useRangeTimestampHandler({
      rootEl: svgEle.value,
      minTimestamp: props.minTimestamp,
      maxTimestamp: props.maxTimestamp,
      selectedTimestamp: props.selectedMaxTimestamp,
      isSmallerThanOpositeX: true,
      setTimestamp: (value) => emit('setSelectedMinTimestamp', value),
    });
  });
  const maxRangeHandler = computed(() =>
    useRangeTimestampHandler({
      rootEl: svgEle.value,
      minTimestamp: props.minTimestamp,
      maxTimestamp: props.maxTimestamp,
      selectedTimestamp: props.selectedMinTimestamp,
      isSmallerThanOpositeX: false,
      setTimestamp: (value) => emit('setSelectedMaxTimestamp', value),
    }),
  );

  const boundaryLeft = computed(() => {
    return adjustPercentValue(minOpositeX.value);
  });

  const boundaryRight = computed(() => adjustPercentValue(maxOpositeX.value) || 0);

  const minMouseDownX = computed(() => minRangeHandler.value.mouseDownX.value || 0);
  const minCurrentX = computed(() => minRangeHandler.value.currentX.value || 0);
  const maxMouseDownX = computed(() => maxRangeHandler.value.mouseDownX.value || 0);
  const maxCurrentX = computed(() => maxRangeHandler.value.currentX.value || 0);
</script>
