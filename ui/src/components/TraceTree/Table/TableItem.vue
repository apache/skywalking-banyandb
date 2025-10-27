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
  <div>
    <div
      :class="[
        'trace-item',
        'level' + ((data.level || 0) - 1),
        { 'trace-item-error': data.isError },
        { highlighted: inTimeRange },
      ]"
    >
      <div
        :class="['method', 'level' + ((data.level || 0) - 1)]"
        :style="{
          'text-indent': ((data.level || 0) - 1) * 10 + 'px',
          width: `${method}px`,
        }"
        @click.stop
      >
        <el-icon
          :style="!displayChildren ? 'transform: rotate(-90deg);' : ''"
          @click.stop="toggle"
          v-if="data.children && data.children.length"
          style="vertical-align: middle"
        >
          <ArrowDown />
        </el-icon>
        <el-icon v-if="tagError" style="color: red; margin-left: 3px; vertical-align: middle">
          <WarningFilled />
        </el-icon>
        <i style="padding-left: 3px; vertical-align: middle; font-style: normal">
          {{ data.message }}
        </i>
      </div>
      <div class="start-time">
        {{ new Date(data.startTime).toLocaleString() }}
      </div>
      <div class="exec-ms">
        {{ (data.duration / 1000 / 1000).toFixed(3) }}
      </div>
      <div class="exec-percent">
        {{ execPercent }}
      </div>
      <div class="exec-percent">
        {{ durationPercent }}
      </div>
      <div class="self">
        {{ (data.selfDuration / 1000 / 1000).toFixed(3) }}
      </div>
      <div class="tags" @click.stop="showTagsDialog" :class="{ clickable: data.tags && data.tags.length > 0 }">
        <div class="tag" v-for="(tag, index) in visibleTags" :key="index">
          {{ tag.key }}: {{ tag.value && tag.value.length > 20 ? tag.value.slice(0, 20) + '...' : tag.value }}
        </div>
        <span v-if="hasMoreTags" class="more-tags">+{{ data.tags.length - MAX_VISIBLE_TAGS }}</span>
      </div>
    </div>

    <el-dialog v-model="tagsDialogVisible" title="Tag Details" width="600px" :append-to-body="true">
      <div class="tags-details" style="max-height: 70vh; overflow-y: auto">
        <el-table :data="data.tags" style="width: 100%">
          <el-table-column prop="key" label="Key" width="200" />
          <el-table-column prop="value" label="Value">
            <template #default="scope">
              <div class="tag-value">{{ scope.row.value }}</div>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-dialog>
    <div v-show="data.children && data.children.length > 0 && displayChildren" class="children-trace">
      <table-item
        v-for="(child, index) in data.children"
        :method="method"
        :key="index"
        :data="child"
        :selectedMaxTimestamp="selectedMaxTimestamp"
        :selectedMinTimestamp="selectedMinTimestamp"
      />
    </div>
  </div>
</template>
<script setup>
  import { ref, computed } from 'vue';
  import { ArrowDown, WarningFilled } from '@element-plus/icons-vue';

  const props = defineProps({
    data: Object,
    method: Number,
    selectedMaxTimestamp: Number,
    selectedMinTimestamp: Number,
  });
  const displayChildren = ref(true);
  const tagsDialogVisible = ref(false);
  const MAX_VISIBLE_TAGS = 1;

  const tagError = computed(() => {
    return props.data.tags.find((tag) => tag.key === 'error_msg');
  });
  const execPercent = computed(() => {
    if (props.data.level === 1) {
      return '100%';
    }
    const exec = props.data.endTime - props.data.startTime;
    if (exec < 0) {
      return '-';
    }
    const result = (exec / props.data.totalExec) * 100;
    if (isNaN(result)) {
      return '-';
    }
    if (result <= 0) {
      return '0';
    }
    return `${result.toFixed(2)}%`;
  });
  const durationPercent = computed(() => {
    const result = (props.data.selfDuration / props.data.duration) * 100;
    if (isNaN(result)) {
      return '-';
    }
    if (result <= 0) {
      return '0';
    }
    return `${result.toFixed(2)}%`;
  });
  const inTimeRange = computed(() => {
    if (props.selectedMinTimestamp === undefined || props.selectedMaxTimestamp === undefined) {
      return true;
    }

    return props.data.startTime <= props.selectedMaxTimestamp && props.data.endTime >= props.selectedMinTimestamp;
  });

  const visibleTags = computed(() => {
    if (!props.data.tags || props.data.tags.length === 0) {
      return [];
    }
    return props.data.tags.slice(0, MAX_VISIBLE_TAGS);
  });

  const hasMoreTags = computed(() => {
    return props.data.tags && props.data.tags.length > MAX_VISIBLE_TAGS;
  });

  function toggle() {
    displayChildren.value = !displayChildren.value;
  }

  function showTagsDialog() {
    if (props.data.tags && props.data.tags.length > 0) {
      tagsDialogVisible.value = true;
    }
  }
</script>
<style lang="scss" scoped>
  @import url('./table.scss');

  .trace-item.level0 {
    &:hover {
      background: rgb(0 0 0 / 4%);
    }
  }

  .highlighted {
    color: var(--el-color-primary);
  }

  .trace-item-error {
    color: #e54c17;
  }

  .trace-item {
    white-space: nowrap;
    position: relative;
  }

  .trace-item.selected {
    background-color: var(--sw-list-selected);
  }

  .trace-item:not(.level0):hover {
    background-color: var(--sw-list-hover);
  }

  .trace-item > div {
    padding: 5px;
    display: inline-block;
    border: 1px solid transparent;
    border-right: 1px dotted silver;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .trace-item > div.method {
    padding-left: 10px;
    cursor: pointer;
    // display: inline-flex;
    // align-items: center;
  }

  .trace-item div.exec-percent {
    height: 30px;
    padding: 0 8px;
  }

  .link-span {
    text-decoration: underline;
  }

  .tags {
    display: flex;
    align-items: center;
    gap: 4px;

    &.clickable {
      cursor: pointer;

      &:hover {
        background-color: rgba(0, 0, 0, 0.05);
      }
    }

    .tag {
      display: inline-block;
      padding: 2px 6px;
      background-color: #f0f0f0;
      border-radius: 3px;
      font-size: 12px;
    }

    .more-tags {
      cursor: pointer;
      color: var(--el-color-primary);
      font-weight: bold;
      padding: 2px 6px;
      font-size: 11px;

      &:hover {
        text-decoration: underline;
      }
    }
  }

  .tags-details {
    :deep(.el-table) {
      font-size: 13px;
    }

    .tag-value {
      word-break: break-all;
      white-space: pre-wrap;
      padding: 4px 0;
    }
  }
</style>
