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
  import {
    deleteSecondaryDataModel,
    getindexRuleList,
    getindexRuleBindingList,
    getGroupList,
    getTopNAggregationList,
    getStreamOrMeasureList,
    deleteStreamOrMeasure,
    deleteGroup,
    createGroup,
    editGroup,
  } from '@/api/index';
  import { ElMessage, ElMessageBox } from 'element-plus';
  import { watch, getCurrentInstance } from '@vue/runtime-core';
  import { useRouter, useRoute } from 'vue-router';
  import { ref, reactive } from 'vue';
  import { Search } from '@element-plus/icons-vue';

  const router = useRouter();
  const route = useRoute();
  const { proxy } = getCurrentInstance();

  // ref
  const ruleForm = ref();
  const loading = ref(false);
  const treeRef = ref();
  const filterText = ref('');
  const currentNode = ref({});
  const resizable = ref();
  // Data
  const data = reactive({
    groupLists: [],
    isCollapse: false,
    treeWidth: '100%',
    // right menu
    showOperationMenus: false,
    operationMenus: [],
    top: 0,
    left: 0,
    // create/edit group
    dialogGroupVisible: false,
    setGroup: 'create',
    groupForm: {
      name: null,
      catalog: 'CATALOG_STREAM',
      shardNum: 1,
      segmentIntervalUnit: 'UNIT_DAY',
      segmentIntervalNum: 1,
      ttlUnit: 'UNIT_DAY',
      ttlNum: 3,
    },
    activeNode: '',
    formLabelWidth: '170px',
  });

  const defaultProps = {
    children: 'children',
    label: 'name',
  };

  const TargetTypes = {
    Group: 'group',
    Resources: 'resources',
  };
  // catalog to group type
  const catalogToGroupType = {
    CATALOG_MEASURE: 'measure',
    CATALOG_STREAM: 'stream',
    CATALOG_UNSPECIFIED: 'property',
  };

  // group type to catalog
  const groupTypeToCatalog = {
    measure: 'CATALOG_MEASURE',
    stream: 'CATALOG_STREAM',
    property: 'CATALOG_UNSPECIFIED',
  };

  const TypeMap = {
    topNAggregation: 'topn-agg',
    indexRule: 'index-rule',
    indexRuleBinding: 'index-rule-binding',
    children: 'children',
  };

  // rules
  const rules = {
    name: [
      {
        required: true,
        message: 'Please enter the name of the group',
        trigger: 'blur',
      },
    ],
    catalog: [
      {
        required: true,
        message: 'Please select the type of the group',
        trigger: 'blur',
      },
    ],
    shardNum: [
      {
        required: true,
        message: 'Please select the shard num of the group',
        trigger: 'blur',
      },
    ],
    segmentIntervalUnit: [
      {
        required: true,
        message: 'Please select the segment interval unit of the group',
        trigger: 'blur',
      },
    ],
    segmentIntervalNum: [
      {
        required: true,
        message: 'Please select the segment Interval num of the group',
        trigger: 'blur',
      },
    ],
    ttlUnit: [
      {
        required: true,
        message: 'Please select the ttl unit of the group',
        trigger: 'blur',
      },
    ],
    ttlNum: [
      {
        required: true,
        message: 'Please select the ttl num of the group',
        trigger: 'blur',
      },
    ],
  };

  // Eventbus
  const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus;

  // props data
  const props = defineProps({
    type: {
      type: String,
      required: true,
      default: '',
    },
  });

  // emit event
  const emit = defineEmits(['setWidth']);

  // init data
  function getGroupLists() {
    filterText.value = '';
    loading.value = true;
    getGroupList().then((res) => {
      if (res.status === 200) {
        data.groupLists = res.data.group.filter((d) => catalogToGroupType[d.catalog] === props.type);
        if (props.type === 'property') {
          data.groupLists = data.groupLists.map((item) => ({
            ...item.metadata,
            type: TargetTypes.Resources,
            key: `property-${item.metadata.name}`,
            catalog: item.catalog,
            resourceOpts: item.resourceOpts,
          }));
          loading.value = false;
          const { group, type } = route.params;
          data.activeNode = `${type}-${group}`;
          return;
        }
        let promise = data.groupLists.map((item) => {
          const type = props.type;
          const name = item.metadata.name;
          return new Promise((resolve, reject) => {
            getStreamOrMeasureList(type, name)
              .then((res) => {
                if (res.status === 200) {
                  item.children = res.data[type];
                  resolve();
                }
              })
              .catch((err) => {
                reject(err);
              });
          });
        });
        if (props.type === 'stream' || props.type === 'measure') {
          const promiseIndexRule = data.groupLists.map((item) => {
            const name = item.metadata.name;
            return new Promise((resolve, reject) => {
              getindexRuleList(name)
                .then((res) => {
                  if (res.status === 200) {
                    item.indexRule = res.data.indexRule;
                    resolve();
                  }
                })
                .catch((err) => {
                  reject(err);
                });
            });
          });
          const promiseIndexRuleBinding = data.groupLists.map((item) => {
            const name = item.metadata.name;
            return new Promise((resolve, reject) => {
              getindexRuleBindingList(name)
                .then((res) => {
                  if (res.status === 200) {
                    item.indexRuleBinding = res.data.indexRuleBinding;
                    resolve();
                  }
                })
                .catch((err) => {
                  reject(err);
                });
            });
          });
          promise = promise.concat(promiseIndexRule);
          promise = promise.concat(promiseIndexRuleBinding);
        }
        if (props.type == 'measure') {
          const TopNAggregationRule = data.groupLists.map((item) => {
            const name = item.metadata.name;
            return new Promise((resolve, reject) => {
              getTopNAggregationList(name)
                .then((res) => {
                  if (res.status === 200) {
                    item.topNAggregation = res.data.topNAggregation;
                    resolve();
                  }
                })
                .catch((err) => {
                  reject(err);
                });
            });
          });
          promise = promise.concat(TopNAggregationRule);
        }
        Promise.all(promise)
          .then(() => {
            data.groupLists = processGroupTree();
            initActiveNode();
          })
          .catch((err) => {
            ElMessage({
              message: `An error occurred while obtaining group data. Please refresh and try again. Error: ${err}`,
              type: 'error',
              duration: 3000,
            });
          })
          .finally(() => {
            loading.value = false;
          });
      } else {
        loading.value = false;
      }
    });
  }

  function processGroupTree() {
    const trees = [];
    for (const group of data.groupLists) {
      const g = {
        ...group.metadata,
        children: [],
        catalog: group.catalog,
        type: TargetTypes.Group,
        key: group.metadata.name,
      };
      const keys = Object.keys(TypeMap);
      for (const key of keys) {
        if (group[key]) {
          const n = key === 'children' ? props.type : key;
          const list = {
            name: n.charAt(0).toUpperCase() + n.slice(1),
            children: [],
            type: n,
            key: `${group.metadata.name}_${TypeMap[n] || n}`,
            group: group.metadata.name,
          };
          for (const item of group[key]) {
            list.children.push({
              ...item.metadata,
              type: TargetTypes.Resources,
              key: `${group.metadata.name}_${TypeMap[n] || n}_${item.metadata.name}`,
              parent: TypeMap[n] || n,
              catalog: group.catalog,
              resourceOpts: group.resourceOpts,
            });
          }
          g.children.push(list);
        }
      }
      trees.push(g);
    }
    return trees;
  }

  // to resources
  function viewResources(node) {
    currentNode.value = node;
    if (node.type !== TargetTypes.Resources) {
      return;
    }
    const { name, group, parent } = node;
    const values = Object.values(TypeMap);

    if (values.includes(parent)) {
      const route = {
        name: `${props.type}-${parent}`,
        params: {
          group: group,
          name: name,
          operator: 'read',
          type: parent,
        },
      };
      router.push(route);
      const add = {
        label: name,
        type: `Read-${parent}`,
        route,
      };
      return $bus.emit('AddTabs', add);
    }
    if (props.type == 'property') {
      const route = {
        name: 'property',
        params: {
          group: name,
          operator: 'read',
          type: props.type,
        },
      };
      router.push(route);
      const add = {
        label: name,
        type: 'Read',
        route,
      };
      data.active = `${name}`;
      return $bus.emit('AddTabs', add);
    }
    const route = {
      name: props.type,
      params: {
        group: group,
        name: name,
        operator: 'read',
        type: props.type,
      },
    };
    router.push(route);
    const add = {
      label: name,
      type: 'Read',
      route,
    };
    $bus.emit('AddTabs', add);
  }

  function openOperationMenus(e, node) {
    currentNode.value = node;
    data.showOperationMenus = true;
    data.top = e.pageY;
    data.left = e.pageX;
    document.getElementById('app').onclick = closeRightMenu;
    e.stopPropagation();
    const AllMenus = [
      { label: 'Create', fn: createTarget },
      { label: 'Edit', fn: editTarget },
      { label: `Refresh`, fn: getGroupLists },
      { label: 'Delete', fn: openDeleteDialog },
    ];
    if (node.type === TargetTypes.Group) {
      data.operationMenus = AllMenus;
      return;
    }
    if (node.type === TargetTypes.Resources) {
      data.operationMenus = [AllMenus[1], AllMenus[3]];
      return;
    }
    data.operationMenus = [AllMenus[0]];
    if (Object.keys(TypeMap).includes(currentNode.value.type)) {
      data.operationMenus.push(AllMenus[3]);
    }
  }
  function closeRightMenu() {
    data.showOperationMenus = false;
    document.getElementById('app').onclick = null;
  }
  // CRUD operator
  function editTarget() {
    if (currentNode.value.type === TargetTypes.Group) {
      openEditGroup();
      return;
    }
    if (currentNode.value.type === TargetTypes.Resources && Object.values(TypeMap).includes(currentNode.value.parent)) {
      openEditSecondaryDataModel();
      return;
    }
    openEditResource();
  }
  function createTarget() {
    if (currentNode.value.type === TargetTypes.Group) {
      openCreateGroup();
      return;
    }
    if (currentNode.value.type.toLowerCase() === props.type) {
      openCreateResource();
      return;
    }
    openCreateSecondaryDataModel();
  }
  function openCreateSecondaryDataModel() {
    const type = TypeMap[currentNode.value.type];
    const route = {
      name: `${props.type}-create-${type}`,
      params: {
        operator: 'create',
        group: currentNode.value.group,
        type,
        schema: props.type,
      },
    };
    router.push(route);
    const add = {
      label: currentNode.value.group,
      type: `Create-${type}`,
      route,
    };
    $bus.emit('AddTabs', add);
  }

  function openEditSecondaryDataModel() {
    const type = currentNode.value.parent;
    const route = {
      name: `${props.type}-edit-${type}`,
      params: {
        operator: 'edit',
        group: currentNode.value.group,
        name: currentNode.value.name,
        type,
        schema: props.type,
      },
    };
    router.push(route);
    const add = {
      label: currentNode.value.name,
      type: `Edit-${type}`,
      route,
    };
    $bus.emit('AddTabs', add);
  }

  function openCreateGroup() {
    data.setGroup = 'create';
    data.groupForm.catalog = groupTypeToCatalog[props.type];
    data.dialogGroupVisible = true;
  }
  function openEditGroup() {
    data.groupForm.name = currentNode.value.name;
    data.groupForm.catalog = currentNode.value.catalog;
    data.groupForm.shardNum = currentNode.value.resourceOpts?.shardNum;
    data.groupForm.segmentIntervalUnit = currentNode.value.resourceOpts?.segmentInterval?.unit;
    data.groupForm.segmentIntervalNum = currentNode.value.resourceOpts?.segmentInterval?.num;
    data.groupForm.ttlUnit = currentNode.value.resourceOpts?.ttl?.unit;
    data.groupForm.ttlNum = currentNode.value.resourceOpts?.ttl?.num;
    data.dialogGroupVisible = true;
    data.setGroup = 'edit';
  }
  function openCreateResource() {
    const route = {
      name: `create-${props.type}`,
      params: {
        operator: 'create',
        group: currentNode.value.group,
        type: props.type,
      },
    };
    router.push(route);
    const add = {
      label: currentNode.value.group,
      type: 'Create',
      route,
    };
    $bus.emit('AddTabs', add);
  }
  function openEditResource() {
    const route = {
      name: `edit-${props.type}`,
      params: {
        operator: 'edit',
        group: currentNode.value.group,
        name: currentNode.value.name,
        type: props.type,
      },
    };
    router.push(route);
    const add = {
      label: currentNode.value.name,
      type: 'Edit',
      route,
    };
    $bus.emit('AddTabs', add);
  }

  function openDeleteDialog() {
    ElMessageBox.confirm('Are you sure to delete?').then(() => {
      if (Object.keys(TypeMap).includes(currentNode.value.type)) {
        return deleteSecondaryDataModelFunction(TypeMap[currentNode.value.type]);
      }
      if (type === TargetTypes.Group) {
        return deleteGroupFunction();
      }
      return deleteResource();
    });
  }
  function deleteSecondaryDataModelFunction(param) {
    deleteSecondaryDataModel(param, currentNode.value.group, currentNode.value.type).then((res) => {
      if (res.status == 200) {
        if (res.data.deleted) {
          ElMessage({
            message: 'Delete succeeded',
            type: 'success',
            duration: 5000,
          });
          getGroupLists();
          $bus.emit('deleteResource', currentNode.value.type);
        }
      }
    });
  }
  function deleteGroupFunction() {
    // delete group
    deleteGroup(currentNode.value.name).then((res) => {
      if (res.status == 200) {
        if (res.data.deleted) {
          ElMessage({
            message: 'Delete succeeded',
            type: 'success',
            duration: 5000,
          });
          getGroupLists();
        }
        $bus.emit('deleteGroup', currentNode.value.name);
      }
    });
  }
  function deleteResource() {
    // delete Resources
    deleteStreamOrMeasure(props.type, currentNode.value.group, currentNode.value.name).then((res) => {
      if (res.status == 200) {
        if (res.data.deleted) {
          ElMessage({
            message: 'Delete succeeded',
            type: 'success',
            duration: 5000,
          });
          getGroupLists();
        }
        $bus.emit('deleteResource', currentNode.value.name);
      }
    });
  }

  // create/edit group
  function confirmForm() {
    data.setGroup == 'create' ? createGroupFunction() : editGroupFunction();
  }
  function createGroupFunction() {
    ruleForm.value.validate((valid) => {
      if (valid) {
        const dataList = {
          group: {
            metadata: {
              group: '',
              name: data.groupForm.name,
            },
            catalog: data.groupForm.catalog,
            resourceOpts: {
              shardNum: data.groupForm.shardNum,
              segmentInterval: {
                unit: data.groupForm.segmentIntervalUnit,
                num: data.groupForm.segmentIntervalNum,
              },
              ttl: {
                unit: data.groupForm.ttlUnit,
                num: data.groupForm.ttlNum,
              },
            },
          },
        };
        createGroup(dataList)
          .then((res) => {
            if (res.status == 200) {
              getGroupLists();
              ElMessage({
                message: 'Created successfully',
                type: 'success',
                duration: 3000,
              });
            }
          })
          .finally(() => {
            data.dialogGroupVisible = false;
          });
      }
    });
  }
  function editGroupFunction() {
    const name = data.groupLists[data.clickIndex].metadata.name;
    ruleForm.value.validate((valid) => {
      if (valid) {
        const dataList = {
          group: {
            metadata: {
              group: '',
              name: data.groupForm.name,
            },
            catalog: data.groupForm.catalog,
            resourceOpts: {
              shardNum: data.groupForm.shardNum,
              segmentInterval: {
                unit: data.groupForm.segmentIntervalUnit,
                num: data.groupForm.segmentIntervalNum,
              },
              ttl: {
                unit: data.groupForm.ttlUnit,
                num: data.groupForm.ttlNum,
              },
            },
          },
        };
        editGroup(name, dataList)
          .then((res) => {
            if (res.status == 200) {
              getGroupLists();
              ElMessage({
                message: 'Update succeeded',
                type: 'success',
                duration: 3000,
              });
            }
          })
          .finally(() => {
            data.dialogGroupVisible = false;
          });
      }
    });
  }
  // init form data
  function clearGroupForm() {
    data.dialogGroupVisible = false;
    data.groupForm = {
      name: null,
      catalog: 'CATALOG_STREAM',
      shardNum: 1,
      segmentIntervalUnit: 'UNIT_DAY',
      segmentIntervalNum: 1,
      ttlUnit: 'UNIT_DAY',
      ttlNum: 3,
    };
  }
  function initActiveNode() {
    const { group, name, type } = route.params;

    if (group && name && type) {
      data.activeNode = `${group}_${type}_${name}`;
    }
  }
  const filterNode = (value, data) => {
    if (!value) return true;
    return data.name.includes(value);
  };
  function mouseDown(e) {
    e.preventDefault();
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }

  function onMouseMove(e) {
    e.preventDefault();
    const newWidth = e.clientX - resizable.value.getBoundingClientRect().left;
    if (newWidth < 60) {
      return;
    }
    if (newWidth > 700) {
      return;
    }
    data.treeWidth = `${newWidth}px`;
    emit('setWidth', String(newWidth));
  }

  function onMouseUp(e) {
    e.preventDefault();
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }

  getGroupLists();

  watch(filterText, (val) => {
    treeRef.value?.filter(val);
  });
</script>

<template>
  <div
    :style="{ display: 'flex', flexDirection: 'column', width: `${data.treeWidth}`, height: `100%` }"
    ref="resizable"
  >
    <div style="display: flex; flex-direction: row; width: 100%; height: 100%; justify-content: space-between">
      <div class="size flex" style="display: flex; flex-direction: column; width: calc(100% - 6px); overflow: auto">
        <el-input
          v-if="props.type !== 'stream'"
          class="group-search"
          v-model="filterText"
          placeholder="Search"
          :prefix-icon="Search"
          clearable
        />
        <el-tree
          ref="treeRef"
          v-loading="loading"
          :data="data.groupLists"
          :props="defaultProps"
          :filter-node-method="filterNode"
          @node-click="viewResources"
          @node-contextmenu="openOperationMenus"
          class="group-tree"
          node-key="key"
          :default-expanded-keys="[data.activeNode]"
          :current-node-key="data.activeNode"
          highlight-current
        >
          <template #default="{ _, data }">
            <div class="node-icon">
              <el-icon class="el-icon" v-if="data.type !== TargetTypes.Group">
                <Histogram v-if="data.type === `${Object.keys(TypeMap)[0]}`" />
                <Collection v-else-if="data.type === `${Object.keys(TypeMap)[1]}`" />
                <Management v-else-if="data.type === `${Object.keys(TypeMap)[2]}`" />
                <Tickets v-else-if="data.type === TargetTypes.Resources" />
                <DataAnalysis v-else />
              </el-icon>
              <div>{{ data.name }}</div>
            </div>
          </template>
        </el-tree>
      </div>
      <div class="resizer" @mousedown="mouseDown"></div>
    </div>
    <el-dialog
      width="25%"
      center
      :title="`${data.setGroup} group`"
      v-model="data.dialogGroupVisible"
      :show-close="false"
    >
      <el-form ref="ruleForm" :rules="rules" :model="data.groupForm" label-position="left">
        <el-form-item label="group name" :label-width="data.formLabelWidth" prop="name">
          <el-input :disabled="data.setGroup == 'edit'" v-model="data.groupForm.name" autocomplete="off"> </el-input>
        </el-form-item>
        <el-form-item label="group type" :label-width="data.formLabelWidth" prop="catalog">
          <el-select v-model="data.groupForm.catalog" placeholder="please select" style="width: 100%">
            <el-option label="Stream" value="CATALOG_STREAM"></el-option>
            <el-option label="Measure" value="CATALOG_MEASURE"></el-option>
            <el-option label="Unspecified(Property)" value="CATALOG_UNSPECIFIED"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="shard num" :label-width="data.formLabelWidth" prop="shardNum">
          <el-input-number v-model="data.groupForm.shardNum" :min="1" />
        </el-form-item>
        <el-form-item label="segment interval unit" :label-width="data.formLabelWidth" prop="segmentIntervalUnit">
          <el-select v-model="data.groupForm.segmentIntervalUnit" placeholder="please select" style="width: 100%">
            <el-option label="Hour" value="UNIT_HOUR"></el-option>
            <el-option label="Day" value="UNIT_DAY"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="segment interval num" :label-width="data.formLabelWidth" prop="segmentIntervalNum">
          <el-input-number v-model="data.groupForm.segmentIntervalNum" :min="1" />
        </el-form-item>
        <el-form-item label="ttl unit" :label-width="data.formLabelWidth" prop="ttlUnit">
          <el-select v-model="data.groupForm.ttlUnit" placeholder="please select" style="width: 100%">
            <el-option label="Hour" value="UNIT_HOUR"></el-option>
            <el-option label="Day" value="UNIT_DAY"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="ttl num" :label-width="data.formLabelWidth" prop="ttlNum">
          <el-input-number v-model="data.groupForm.ttlNum" :min="1" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer footer">
        <el-button @click="clearGroupForm">cancel</el-button>
        <el-button type="primary" @click="confirmForm">{{ data.setGroup }} </el-button>
      </div>
    </el-dialog>
    <div
      v-if="data.showOperationMenus"
      class="right-menu box-shadow"
      :style="{ top: `${data.top}px`, left: `${data.left}px` }"
    >
      <div v-for="m in data.operationMenus" @click="m.fn">{{ m.label }}</div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  .group-search {
    margin: 20px 0 0 10px;
    width: calc(100% - 15px);
  }

  .group-tree {
    padding-top: 10px;
  }

  .resizer {
    width: 5px;
    cursor: ew-resize;
    background-color: #bbb;
    height: 100%;
  }

  .right-menu {
    width: 120px;
    position: fixed;
    z-index: 9999 !important;
    padding: 10px;
    font-size: 14px;
    div {
      height: 30px;
      line-height: 30px;
      padding-left: 20px;
      cursor: pointer;
      &:hover {
        background-color: #eee;
      }
    }
  }

  .footer {
    width: 100%;
    display: flex;
    justify-content: center;
  }

  .node-icon {
    display: flex;
    flex-direction: row;
  }

  .el-icon {
    height: 20px;
    line-height: 20px;
    margin-right: 10px;
  }
</style>
