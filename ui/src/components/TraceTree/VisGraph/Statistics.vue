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
  <div class="trace-statistics">
    <TableContainer
      :tableData="tableData"
      :type="TraceGraphType.STATISTICS"
      :headerType="headerType"
      :traceId="traceId"
    >
      <div class="trace-tips" v-if="!tableData.length">No data</div>
    </TableContainer>
  </div>
</template>
<script setup>
  import { ref, watch, onMounted } from "vue";
  import TableContainer from "../Table/TableContainer.vue"
  import traceTable from "./D3Graph/utils/trace-table";
  import { TraceGraphType } from "./constant";

  const props = defineProps({
    data: Array,
    traceId: String,
    showBtnDetail: Boolean,
    headerType: String,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const emit = defineEmits(["load"]);
  const loading = ref(true);
  const tableData = ref([]);
  const list = ref([]);

  onMounted(() => {
    tableData.value = calculationDataforStatistics(props.data);
    loading.value = false;
    emit("load", () => {
      loading.value = true;
    });
  });

  function calculationDataforStatistics(data) {
    list.value = traceTable.buildTraceDataList(data);
    const result = [];
    const map = traceTable.changeStatisticsTree(data);
    map.forEach((nodes, nodeKey) => {
      const lastColonIndex = nodeKey.lastIndexOf(":");
      result.push(
        getSpanGroupData(nodes, {
          endpointName: nodeKey.slice(0, lastColonIndex),
          type: nodeKey.slice(lastColonIndex + 1),
        }),
      );
    });
    return result;
  }

  function getSpanGroupData(groupspans, groupRef) {
    let maxTime = 0;
    let minTime = Infinity;
    let sumTime = 0;
    const count = groupspans.length;
    groupspans.forEach((groupspan) => {
      const duration = groupspan.dur || 0;
      if (duration > maxTime) {
        maxTime = duration;
      }
      if (duration < minTime) {
        minTime = duration;
      }
      sumTime = sumTime + duration;
    });
    const avgTime = count === 0 ? 0 : sumTime / count;
    return {
      groupRef,
      maxTime,
      minTime,
      sumTime,
      avgTime,
      count,
    };
  }

  watch(
    () => props.data,
    () => {
      if (!props.data.length) {
        tableData.value = [];
        return;
      }
      tableData.value = calculationDataforStatistics(props.data);
      loading.value = false;
    },
  );
</script>
<style lang="scss" scoped>
  .trace-tips {
    width: 100%;
    text-align: center;
    margin-top: 10px;
  }

  .trace-statistics {
    padding: 10px;
    height: calc(100% - 95px);
    width: 100%;
  }
</style>
