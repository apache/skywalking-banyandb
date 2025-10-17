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
<script setup>
  import { reactive, ref, onMounted } from 'vue';
  import { ElMessage } from 'element-plus';
  import { getCurrentInstance } from '@vue/runtime-core';
  import { useRoute, useRouter } from 'vue-router';
  import { updateTrace, createTrace, getTrace } from '@/api';
  import FormHeader from '../common/FormHeader.vue';
  import TagEditor from './TagEditor.vue';
  import { rules, formConfig, traceFieldsConfig } from './data';

  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = getCurrentInstance().appContext.config.globalProperties.$loadingClose;
  const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus;
  const route = useRoute();
  const router = useRouter();
  const tagEditorRef = ref();
  const ruleForm = ref();
  const { operator, name, group, type } = route.params;
  const formData = reactive({
    group: group || '',
    operator,
    type,
    name: name || '',
    traceIdTagName: '',
    timestampTagName: '',
    spanIdTagName: '',
    tags: [],
  });

  function initTrace() {
    if (operator === 'edit') {
      $loadingCreate();
      getTrace(group, name)
        .then((res) => {
          if (res.status === 200) {
            const { trace } = res.data;

            formData.traceIdTagName = trace.traceIdTagName;
            formData.timestampTagName = trace.timestampTagName;
            formData.spanIdTagName = trace.spanIdTagName;
            formData.tags = trace.tags.map((d) => ({
              ...d,
              key: d.name,
              value: d.type,
            }));
          }
        })
        .finally(() => {
          $loadingClose();
        });
    }
  }
  const openEditTag = (index) => {
    tagEditorRef.value.openDialog(formData.tags[index]).then((res) => {
      formData.tags[index] = res;
    });
  };
  const deleteTag = (index) => {
    formData.tags.splice(index, 1);
  };
  const openAddTag = () => {
    tagEditorRef.value.openDialog().then((res) => {
      formData.tags.push(res);
    });
  };
  const submit = async () => {
    if (!ruleForm.value) return;
    await ruleForm.value.validate((valid) => {
      if (valid) {
        $loadingCreate();
        const param = {
          trace: {
            metadata: {
              group: formData.group,
              name: formData.name,
            },
            tags: formData.tags.map((d) => ({ name: d.key, type: d.value })),
            traceIdTagName: formData.traceIdTagName,
            timestampTagName: formData.timestampTagName,
            spanIdTagName: formData.spanIdTagName,
          },
        };
        if (operator === 'create') {
          createTrace(param)
            .then((res) => {
              if (res.status === 200) {
                ElMessage({
                  message: 'Created successfully',
                  type: 'success',
                  duration: 5000,
                });
                $bus.emit('refreshAside');
                $bus.emit('deleteGroup', formData.group);
                openResources();
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
          return;
        }
        updateTrace(formData.group, formData.name, param)
          .then((res) => {
            if (res.status === 200) {
              ElMessage({
                message: 'Updated successfully',
                type: 'success',
                duration: 5000,
              });
              $bus.emit('refreshAside');
              $bus.emit('deleteResource', formData.name);
              openResources();
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
    });
  };
  function openResources() {
    const routeConfig = {
      name: formData.type,
      params: {
        group: formData.group,
        name: formData.name,
        operator: 'read',
        type: formData.type + '',
      },
    };
    router.push(routeConfig);
    const add = {
      label: formData.name,
      type: 'Read',
      route: routeConfig,
    };
    $bus.emit('AddTabs', add);
  }
  onMounted(() => {
    initTrace();
  });
</script>
<template>
  <div>
    <el-card shadow="always">
      <template #header>
        <el-row>
          <el-col :span="20">
            <FormHeader :fields="{ ...formData, catalog: formData.type }" />
          </el-col>
          <el-col :span="4">
            <div class="flex align-item-center justify-end" style="height: 30px">
              <el-button size="small" type="primary" @click="submit(ruleFormRef)" color="#6E38F7">Submit</el-button>
            </div>
          </el-col>
        </el-row>
      </template>
      <el-form ref="ruleForm" :rules="rules" :model="formData" label-position="left">
        <el-form-item
          v-for="item in formConfig"
          :key="item.prop"
          :label="item.label"
          :prop="item.prop"
          label-width="200"
        >
          <el-select
            v-if="item.type === 'select'"
            v-model="formData[item.prop]"
            placeholder="please select"
            style="width: 100%"
          >
            <el-option
              v-for="option in item.selectGroup"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            ></el-option>
          </el-select>
          <el-input
            v-if="item.type === 'input'"
            v-model="formData[item.prop]"
            :disabled="item.disabled"
            autocomplete="off"
          ></el-input>
          <el-input-number v-if="item.type === 'number'" v-model="formData[item.prop]" :min="0"></el-input-number>
        </el-form-item>
        <el-form-item label="Tags" prop="tags" label-width="200">
          <el-button size="small" type="primary" color="#6E38F7" @click="openAddTag">Add Tag</el-button>
          <el-table style="margin-top: 10px" :data="formData.tags" border>
            <el-table-column label="Name" prop="key"></el-table-column>
            <el-table-column label="Type" prop="value"></el-table-column>
            <el-table-column label="Operator" width="150">
              <template #default="scope">
                <el-button
                  link
                  type="primary"
                  @click.prevent="openEditTag(scope.$index)"
                  style="color: var(--color-main); font-weight: bold"
                  >Edit</el-button
                >
                <el-popconfirm @confirm="deleteTag(scope.$index)" title="Are you sure to delete this?">
                  <template #reference>
                    <el-button link type="danger" style="color: red; font-weight: bold">Delete</el-button>
                  </template>
                </el-popconfirm>
              </template>
            </el-table-column>
          </el-table>
        </el-form-item>
        <el-form-item
          v-for="item in traceFieldsConfig"
          :key="item.prop"
          :label="item.label"
          :prop="item.prop"
          label-width="200"
        >
          <el-input
            v-if="item.type === 'input'"
            v-model="formData[item.prop]"
            :disabled="item.disabled"
            autocomplete="off"
          ></el-input>
        </el-form-item>
      </el-form>
    </el-card>
    <TagEditor ref="tagEditorRef" />
  </div>
</template>
<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }
</style>

