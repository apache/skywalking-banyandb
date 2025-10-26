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
  <div class="trace-table-charts">
    <TableContainer
      :tableData="segmentId"
      :selectedMaxTimestamp="selectedMaxTimestamp"
      :selectedMinTimestamp="selectedMinTimestamp"

      @select="handleSelectSpan"
    >
      <div class="trace-tips" v-if="!segmentId.length">No data</div>
    </TableContainer>
  </div>
</template>
<script setup>
  import { ref, onMounted } from "vue";
  import TableContainer from "../Table/TableContainer.vue";

  const props = defineProps({
    data: Array,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const emits = defineEmits(["select"]);
  const segmentId = ref([]);
  onMounted(() => {
    segmentId.value = setLevel(props.data);
    console.log(segmentId.value);
  });

  function setLevel(arr, level = 1, totalExec) {
    for (const item of arr) {
      item.level = level;
      totalExec = totalExec || item.endTime - item.startTime;
      item.totalExec = totalExec;
      if (item.children && item.children.length > 0) {
        setLevel(item.children, level + 1, totalExec);
      }
    }
    return arr;
  }

  function handleSelectSpan(span) {
    emits("select", span);
  }
</script>
<style lang="scss" scoped>
  .trace-table-charts {
    overflow: auto;
    padding: 10px;
    height: 100%;
    width: 100%;
    position: relative;
  }
</style>
