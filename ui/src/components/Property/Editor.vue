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
  import { reactive, ref, onMounted, computed, getCurrentInstance } from 'vue';
  import { ElMessage } from 'element-plus';
  import { useRoute, useRouter } from 'vue-router';
  import { updateProperty, createProperty, getResourceOfAllType } from '@/api/index';
  import TagEditor from '@/components/common/TagEditor.vue';
  import { rules, strategyGroup, formConfig } from './data';

  // Constants
  const OPERATOR_MODE = {
    CREATE: 'create',
    EDIT: 'edit',
  };

  // Composables
  const { appContext } = getCurrentInstance();
  const $loadingCreate = appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = appContext.config.globalProperties.$loadingClose;
  const $bus = appContext.config.globalProperties.mittBus;
  const route = useRoute();
  const router = useRouter();

  // Refs
  const tagEditorRef = ref();
  const ruleForm = ref();

  // Extract route params
  const { operator, name, group, type } = route.params;

  // Reactive form data
  const formData = reactive({
    strategy: strategyGroup[0].value,
    group: group || '',
    operator,
    type,
    name: name || '',
    tags: [],
  });

  // Computed properties
  const isCreateMode = computed(() => operator === OPERATOR_MODE.CREATE);
  const isEditMode = computed(() => operator === OPERATOR_MODE.EDIT);

  // Initialize property data for edit mode
  async function initProperty() {
    if (!isEditMode.value) return;

    try {
      $loadingCreate();
      const res = await getResourceOfAllType(type, group, name);

      if (res.status === 200 && res.data?.property) {
        const { property } = res.data;
        formData.tags = property.tags.map((tag) => ({
          ...tag,
          key: tag.name,
          value: tag.type,
        }));
      }
    } catch (error) {
      ElMessage.error({
        message: `Failed to load property: ${error.message || 'Unknown error'}`,
        type: 'error',
      });
    } finally {
      $loadingClose();
    }
  }

  // Tag management functions with error handling
  const openEditTag = async (index) => {
    try {
      const result = await tagEditorRef.value.openDialog(formData.tags[index]);
      formData.tags[index] = result;
    } catch (error) {
      // User cancelled, do nothing
    }
  };

  const deleteTag = (index) => {
    formData.tags.splice(index, 1);
  };

  const openAddTag = async () => {
    try {
      const result = await tagEditorRef.value.openDialog();
      formData.tags.push(result);
    } catch (error) {
      // User cancelled, do nothing
    }
  };

  // Build property payload
  const buildPropertyPayload = () => ({
    strategy: formData.strategy,
    property: {
      id: formData.id,
      metadata: {
        group: formData.group,
        name: formData.name,
      },
      tags: formData.tags.map(({ key, value }) => ({
        name: key,
        type: value,
      })),
    },
  });

  // Navigate to resource view
  function openResources() {
    const targetRoute = {
      name: formData.type,
      params: {
        group: formData.group,
        name: formData.name,
        operator: 'read',
        type: String(formData.type),
      },
    };

    router.push(targetRoute);

    $bus.emit('AddTabs', {
      label: formData.name,
      type: 'Read',
      route: targetRoute,
    });
  }

  // Submit form with improved error handling
  const submit = async () => {
    if (!ruleForm.value) return;

    try {
      const isValid = await ruleForm.value.validate();
      if (!isValid) return;

      $loadingCreate();

      const payload = buildPropertyPayload();
      let response;

      if (isCreateMode.value) {
        response = await createProperty(payload);

        if (response.error) {
          throw new Error(response.error.message);
        }

        ElMessage.success({
          message: 'Property created successfully',
          type: 'success',
        });
      } else {
        response = await updateProperty(formData.group, formData.name, payload);

        if (response.error) {
          throw new Error(response.error.message);
        }

        ElMessage.success({
          message: 'Property updated successfully',
          type: 'success',
        });
      }

      // Emit events and navigate
      $bus.emit('refreshAside');
      $bus.emit('deleteResource', formData.name);
      openResources();
    } catch (error) {
      const action = isCreateMode.value ? 'create' : 'update';
      ElMessage.error({
        message: `Failed to ${action} property: ${error.message || 'Unknown error'}`,
        type: 'error',
      });
    } finally {
      $loadingClose();
    }
  };

  // Initialize on mount
  onMounted(() => {
    initProperty();
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
      </el-form>
      <TagEditor ref="tagEditorRef"></TagEditor>
    </el-card>
  </div>
</template>
<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }
</style>
