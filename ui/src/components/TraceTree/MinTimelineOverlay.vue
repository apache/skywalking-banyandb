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
      v-if="mouseDownX !== undefined && currentX !== undefined"
      :x="`${Math.min(mouseDownX, currentX)}%`"
      y="0"
      :width="`${Math.abs(mouseDownX - currentX)}%`"
      height="100%"
      fill="var(--el-color-primary-light-5)"
      fill-opacity="0.2"
      pointer-events="none"
    />
    <rect
      ref="rootEl"
      x="0"
      y="0"
      width="100%"
      height="100%"
      @mousedown="handleMouseDown"
      @mousemove="handleMouseHoverMove"
      @mouseleave="handleMouseHoverLeave"
      fill-opacity="0"
      cursor="col-resize"
    />
    <line
      v-if="hoverX"
      :x1="`${hoverX}%`"
      :y1="0"
      :x2="`${hoverX}%`"
      y2="100%"
      stroke="var(--el-color-primary-light-5)"
      stroke-width="1"
      pointer-events="none"
    />
  </g>
</template>
<script setup>
  import { onBeforeUnmount, ref } from "vue";

  const props = defineProps({
    minTimestamp: Number,
    maxTimestamp: Number,
  });
  const emit = defineEmits(["setSelectedMaxTimestamp", "setSelectedMinTimestamp"]);
  const rootEl = ref(null);
  const mouseDownX = ref(undefined);
  const currentX = ref(undefined);
  const hoverX = ref(undefined);
  const mouseDownXRef = ref(undefined);
  const isDragging = ref(false);

  function handleMouseMove(e) {
    if (!rootEl.value) {
      return;
    }
    const x = calculateX(rootEl.value.getBoundingClientRect(), e.pageX);
    currentX.value = x;
  }
  function handleMouseUp(e) {
    if (!isDragging.value || !rootEl.value || mouseDownXRef.value === undefined) {
      return;
    }

    const x = calculateX(rootEl.value.getBoundingClientRect(), e.pageX);
    const adjustedX = Math.abs(x - mouseDownXRef.value) < 1 ? x + 1 : x;

    const t1 = (mouseDownXRef.value / 100) * (props.maxTimestamp - props.minTimestamp) + props.minTimestamp;
    const t2 = (adjustedX / 100) * (props.maxTimestamp - props.minTimestamp) + props.minTimestamp;
    const newMinTimestmap = Math.min(t1, t2);
    const newMaxTimestamp = Math.max(t1, t2);

    emit("setSelectedMinTimestamp", newMinTimestmap);
    emit("setSelectedMaxTimestamp", newMaxTimestamp);

    currentX.value = undefined;
    mouseDownX.value = undefined;
    mouseDownXRef.value = undefined;
    isDragging.value = false;

    document.removeEventListener("mousemove", handleMouseMove);
    document.removeEventListener("mouseup", handleMouseUp);
  }

  const calculateX = (parentRect, x) => {
    const value = ((x - parentRect.left) / (parentRect.right - parentRect.left)) * 100;
    if (value <= 0) {
      return 0;
    }
    if (value >= 100) {
      return 100;
    }
    return value;
  };

  function handleMouseDown(e) {
    if (!rootEl.value) {
      return;
    }
    const x = calculateX(rootEl.value.getBoundingClientRect(), e.pageX);
    currentX.value = x;
    mouseDownX.value = x;
    mouseDownXRef.value = x;
    isDragging.value = true;

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
  }

  function handleMouseHoverMove(e) {
    if (e.buttons !== 0 || !rootEl.value) {
      return;
    }
    const x = calculateX(rootEl.value.getBoundingClientRect(), e.pageX);
    hoverX.value = x;
  }

  function handleMouseHoverLeave() {
    hoverX.value = undefined;
  }

  onBeforeUnmount(() => {
    document.removeEventListener("mousemove", handleMouseMove);
    document.removeEventListener("mouseup", handleMouseUp);
    isDragging.value = false;
  });
</script>
