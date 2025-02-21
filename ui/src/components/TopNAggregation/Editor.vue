<!--
  ~ Licensed to Apache Software Foundation (ASF) under one or more contributor
  ~ license agreements. See the NOTICE file distributed with
  ~ this work for additional information regarding copyright
  ~ ownership. Apache Software Foundation (ASF) licenses this file to you under
  ~ the Apache License, Version 2.0 (the "License"); you may
  ~ not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

<script lang="ts" setup>
  import { reactive, ref } from 'vue';
  import { watch, getCurrentInstance } from '@vue/runtime-core';
  import { useRoute, useRouter } from 'vue-router';
  import type { FormInstance } from 'element-plus';
  import { createSecondaryDataModel, getSecondaryDataModel, updateSecondaryDataModel } from '@/api/index';
  import { ElMessage } from 'element-plus';
  import FormHeader from '../common/FormHeader.vue';

  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose;
  const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus;
  const ruleFormRef = ref<FormInstance>();

  const route = useRoute();
  const router = useRouter();
  const rules = {
    group: [
      {
        required: true,
        message: 'Please enter the group',
        trigger: 'blur',
      },
    ],
    name: [
      {
        required: true,
        message: 'Please enter the name',
        trigger: 'blur',
      },
    ],
    sourceMeasureGroup: [
      {
        required: true,
        message: 'Please enter the source measure group',
        trigger: 'blur',
      },
    ],
    sourceMeasureName: [
      {
        required: true,
        message: 'Please enter the source measure',
        trigger: 'blur',
      },
    ],
    fieldName: [
      {
        required: true,
        message: 'Please enter the field name',
        trigger: 'blur',
      },
    ],
    fieldValueSort: [
      {
        required: true,
        message: 'Please select the field value sort',
        trigger: 'blur',
      },
    ],
    groupByTagNames: [
      {
        required: true,
        message: 'Please enter the group by tag names',
        trigger: 'blur',
      },
    ],
    countersNumber: [
      {
        required: true,
        message: 'Please enter counters number',
        trigger: 'blur',
      },
    ],
    lruSize: [
      {
        required: true,
        message: 'Please enter LRU size',
        trigger: 'blur',
      },
    ],
  };
  const fieldValueSortList = [
    {
      label: 'Both',
      value: 'SORT_UNSPECIFIED',
    },
    {
      label: 'SORT_DESC',
      value: 'SORT_DESC',
    },
    {
      label: 'SORT_ASC',
      value: 'SORT_ASC',
    },
  ];
  const data = reactive({
    group: route.params.group,
    name: route.params.name,
    type: route.params.type,
    operator: route.params.operator,
    schema: route.params.schema,
    form: {
      group: route.params.group,
      name: route.params.name || '',
      sourceMeasureGroup: route.params.group,
      sourceMeasureName: '',
      fieldName: '',
      fieldValueSort: '',
      groupByTagNames: [],
      countersNumber: '',
      lruSize: '',
    },
  });

  watch(
    () => route,
    () => {
      data.form = {
        group: route.params.group,
        name: route.params.name || '',
        sourceMeasureGroup: route.params.group,
        sourceMeasureName: '',
        fieldName: '',
        fieldValueSort: '',
        groupByTagNames: [],
        countersNumber: '',
        lruSize: '',
      };
      data.group = route.params.group;
      data.name = route.params.name;
      data.type = route.params.type;
      data.operator = route.params.operator;
      data.schema = route.params.schema;
      initData();
    },
    {
      immediate: true,
      deep: true,
    },
  );
  const submit = async (formEl: FormInstance | undefined) => {
    if (!formEl) return;
    await formEl.validate((valid) => {
      if (valid) {
        const param = {
          top_n_aggregation: {
            metadata: {
              group: data.form.group,
              name: data.form.name,
            },
            sourceMeasure: {
              group: data.form.sourceMeasureGroup,
              name: data.form.sourceMeasureName,
            },
            fieldName: data.form.fieldName,
            fieldValueSort: data.form.fieldValueSort,
            groupByTagNames: data.form.groupByTagNames,
            countersNumber: data.form.countersNumber,
            lruSize: data.form.lruSize,
          },
        };
        $loadingCreate();
        if (data.operator === 'create') {
          return createSecondaryDataModel('topn-agg', param)
            .then((res) => {
              if (res.status === 200) {
                ElMessage({
                  message: 'Create successed',
                  type: 'success',
                  duration: 5000,
                });
                $bus.emit('refreshAside');
                $bus.emit('deleteGroup', data.form.group);
                openTopNAgg();
              }
            })
            .catch((err) => {
              ElMessage({
                message: 'Please refresh and try again. Error: ' + err,
                type: 'error',
                duration: 3000,
              });
            })
            .finally(() => {
              $loadingClose();
            });
        } else {
          return updateSecondaryDataModel('topn-agg', data.form.group, data.form.name, param)
            .then((res) => {
              if (res.status === 200) {
                ElMessage({
                  message: 'Edit successed',
                  type: 'success',
                  duration: 5000,
                });
                $bus.emit('refreshAside');
                $bus.emit('deleteResource', data.form.group);
                openTopNAgg();
              }
            })
            .catch((err) => {
              ElMessage({
                message: 'Please refresh and try again. Error: ' + err,
                type: 'error',
                duration: 3000,
              });
            })
            .finally(() => {
              $loadingClose();
            });
        }
      }
    });
  };

  function openTopNAgg() {
    const route = {
      name: data.schema + '-' + data.type,
      params: {
        group: data.form.group,
        name: data.form.name,
        operator: 'read',
        type: data.type + '',
      },
    };
    router.push(route);
    const add = {
      label: data.form.name,
      type: `Read-${data.type}`,
      route,
    };
    $bus.emit('changeAside', data.form);
    $bus.emit('AddTabs', add);
  }

  function initData() {
    if (data.operator === 'edit' && data.form.group && data.form.name) {
      $loadingCreate();
      getSecondaryDataModel('topn-agg', data.form.group, data.form.name)
        .then((res) => {
          if (res.status === 200) {
            const topNAggregation = res.data.topNAggregation;
            data.form = {
              group: topNAggregation.metadata.group,
              name: topNAggregation.metadata.name,
              sourceMeasureGroup: topNAggregation.sourceMeasure.group,
              sourceMeasureName: topNAggregation.sourceMeasure.name,
              fieldName: topNAggregation.fieldName,
              fieldValueSort: topNAggregation.fieldValueSort,
              groupByTagNames: topNAggregation.groupByTagNames,
              countersNumber: topNAggregation.countersNumber,
              lruSize: topNAggregation.lruSize,
            };
          }
        })
        .catch((err) => {
          ElMessage({
            message: 'Please refresh and try again. Error: ' + err,
            type: 'error',
            duration: 3000,
          });
        })
        .finally(() => {
          $loadingClose();
        });
    }
  }
</script>

<template>
  <div>
    <el-card>
      <template #header>
        <el-row>
          <el-col :span="20">
            <FormHeader :fields="data" />
          </el-col>
          <el-col :span="4">
            <div class="flex align-item-center justify-end" style="height: 30px">
              <el-button size="small" type="primary" @click="submit(ruleFormRef)" color="#6E38F7">Submit</el-button>
            </div>
          </el-col>
        </el-row>
      </template>
      <el-form
        ref="ruleFormRef"
        :rules="rules"
        label-position="left"
        :model="data.form"
        style="width: 100%"
        label-width="auto"
      >
        <el-form-item label="Group" prop="group">
          <el-input v-model="data.form.group" clearable :disabled="true"></el-input>
        </el-form-item>
        <el-form-item label="Name" prop="name">
          <el-input
            v-model="data.form.name"
            :disabled="data.operator === 'edit'"
            clearable
            placeholder="Input Name"
          ></el-input>
        </el-form-item>
        <el-form-item label="Measure Group" prop="sourceMeasureGroup">
          <el-input
            v-model="data.form.sourceMeasureGroup"
            :disabled="true"
            clearable
            placeholder="Input Source Measure Group"
          ></el-input>
        </el-form-item>
        <el-form-item label="Measure Name" prop="sourceMeasureName">
          <el-input
            v-model="data.form.sourceMeasureName"
            :disabled="data.operator === 'edit'"
            clearable
            placeholder="Input Source Measure Name"
          ></el-input>
        </el-form-item>
        <el-form-item label="Field Name" prop="fieldName">
          <el-input v-model="data.form.fieldName" clearable placeholder="Input Field Name"></el-input>
        </el-form-item>
        <el-form-item label="Field Value Sort" prop="fieldValueSort">
          <el-select
            v-model="data.form.fieldValueSort"
            placeholder="Choose Field Value Sort"
            style="width: 100%"
            clearable
          >
            <el-option v-for="item in fieldValueSortList" :key="item.value" :label="item.label" :value="item.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="Group By Tag Names" prop="groupByTagNames">
          <el-select
            class="tags-and-rules"
            v-model="data.form.groupByTagNames"
            allow-create
            filterable
            default-first-option
            placeholder="Input Group By Tag Names"
            style="width: 100%"
            clearable
            multiple
          ></el-select>
        </el-form-item>
        <el-form-item label="Counters Number" prop="countersNumber">
          <el-input v-model="data.form.countersNumber" clearable placeholder="Input Counters Number"></el-input>
        </el-form-item>
        <el-form-item label="LRU Size" prop="lruSize">
          <el-input v-model="data.form.lruSize" clearable placeholder="Input LRU Size"></el-input>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }
</style>
