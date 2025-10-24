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
  <div class="timeline-tool flex-h">
    <div class="flex-h trace-type item">
      <el-radio-group v-model="spansGraphType" size="small">
        <el-radio-button v-for="option in reorderedOptions" :key="option.value" :value="option.value">
          {{ option.label }}
        </el-radio-button>
      </el-radio-group>
    </div>
    <div>
      <el-button :icon="DCaret" size="small" @click="onToggleMinTimeline" />
    </div>
  </div>
</template>
<script setup>
  import { ref, watch } from "vue";
  import { GraphTypeOptions } from "./VisGraph/constant";
  import { DCaret } from "@element-plus/icons-vue";

  const emit = defineEmits(["toggleMinTimeline", "updateSpansGraphType"]);
  const reorderedOptions = [
    GraphTypeOptions[2], // Table
    GraphTypeOptions[0], // List
    GraphTypeOptions[1], // Tree
    GraphTypeOptions[3], // Statistics
  ];
  const spansGraphType = ref(reorderedOptions[0].value);

  function onToggleMinTimeline() {
    emit("toggleMinTimeline");
  }

  // Watch for changes in spansGraphType and emit to parent
  watch(spansGraphType, (newValue) => {
    emit("updateSpansGraphType", newValue);
  });
</script>
<style lang="scss" scoped>
  .timeline-tool {
    justify-content: space-between;
    padding: 10px 5px;
    border-bottom: 1px solid var(--el-border-color-light);
  }
</style>
