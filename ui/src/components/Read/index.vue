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
  import { reactive, ref } from 'vue';
  import { useRoute } from 'vue-router';
  import { watch, getCurrentInstance } from '@vue/runtime-core';
  import { getResourceOfAllType, getTableList } from '@/api/index';
  import { Search, RefreshRight } from '@element-plus/icons-vue';
  import { jsonToYaml, yamlToJson } from '@/utils/yaml';
  import CodeMirror from '@/components/CodeMirror/index.vue';
  import { ElMessage } from 'element-plus';
  import { computed } from '@vue/runtime-core';
  import FormHeader from '../common/FormHeader.vue';
  import { Shortcuts, Last15Minutes } from '../common/data';

  const route = useRoute();

  const yamlRef = ref();

  // Loading
  const { proxy } = getCurrentInstance();
  const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate;
  const $loadingClose = proxy.$loadingClose;
  const tagType = {
    TAG_TYPE_UNSPECIFIED: 'null',
    TAG_TYPE_STRING: 'str',
    TAG_TYPE_INT: 'int',
    TAG_TYPE_STRING_ARRAY: 'strArray',
    TAG_TYPE_INT_ARRAY: 'intArray',
    TAG_TYPE_DATA_BINARY: 'binaryData',
  };
  const fieldTypes = {
    FIELD_TYPE_UNSPECIFIED: 'null',
    FIELD_TYPE_STRING: 'str',
    FIELD_TYPE_INT: 'int',
    FIELD_TYPE_FLOAT: 'float',
    FIELD_TYPE_DATA_BINARY: 'binaryData',
  };
  const filterConfig = reactive({
    groups: [],
    name: '',
    offset: null,
    limit: null,
    stages: undefined,
    projection: {
      tagFamilies: [
        {
          name: '',
          tags: [],
        },
      ],
    },
  });
  const data = reactive({
    fields: [],
    tableFields: [],
    handleFields: '',
    group: route.params.group,
    name: route.params.name,
    type: route.params.type,
    tagFamily: 0,
    tagFamilies: [],
    options: [],
    resourceData: null,
    timeValue: null,
    loading: false,
    total: 100,
    /* queryInfo: {
        pagenum: 1,
        pagesize: 100
    }, */
    tableTags: [],
    tableData: [],
    code: null,
    codeStorage: [],
    byStages: false,
  });
  const tableHeader = computed(() => {
    return data.tableTags.concat(data.tableFields);
  });
  watch(
    () => data.handleFields,
    () => {
      if (data.handleFields.length > 0) {
        data.tableTags = data.tableTags.map((item) => {
          item.label = `${data.options[data.tagFamily].label}.${item.name}`;
          return item;
        });
      } else {
        data.tableTags = data.tableTags.map((item) => {
          item.label = item.name;
          return item;
        });
      }
    },
  );
  watch(
    () => route,
    () => {
      data.group = route.params.group;
      data.name = route.params.name;
      data.type = route.params.type;
      data.tableData = [];
      data.tableTags = [];
      data.tableFields = [];
      data.fields = [];
      data.handleFields = '';

      if (data.group && data.name && data.type) {
        initCode();
        initData();
      }
    },
    {
      immediate: true,
      deep: true,
    },
  );

  function initCode() {
    let index = data.codeStorage.findIndex((item) => {
      return item.params.group === route.params.group && item.params.name === route.params.name;
    });
    if (index >= 0) {
      data.code = data.codeStorage[index].params.code;
    } else {
      let timeRange = {
        timeRange: {
          begin: new Date(new Date() - Last15Minutes),
          end: new Date(),
        },
      };
      timeRange = jsonToYaml(timeRange).data;
      data.code = `${timeRange}offset: 0
limit: 10
orderBy:
  indexRuleName: ""
  sort: SORT_UNSPECIFIED
`;
    }
  }
  function setCode() {
    const json = yamlToJson(data.code);
    if (data.byStages) {
      json.data.stages = ['hot'];
    } else {
      delete json.data.stages;
    }
    if (!json.data.hasOwnProperty('timeRange')) {
      json.data.timeRange = {
        begin: '',
        end: '',
      };
    }
    json.data.timeRange.begin = data.timeValue ? new Date(data.timeValue[0]) : null;
    json.data.timeRange.end = data.timeValue ? new Date(data.timeValue[1]) : null;
    data.code = jsonToYaml(json.data).data;
  }
  async function initData() {
    $loadingCreate();
    const res = await getResourceOfAllType(data.type, data.group, data.name);
    $loadingClose();
    if (res.error) {
      ElMessage({
        message: `Get ${data.type} failed: ${res.error.message}`,
        type: 'error',
      });
      return;
    }
    data.resourceData = res[data.type];
    data.tableTags = res[data.type].tagFamilies[0].tags.map((item) => {
      item.label = item.name;
      return item;
    });
    data.options = res[data.type].tagFamilies.map((item, index) => {
      return { label: item.name, value: index };
    });
    data.tagFamily = 0;
    data.fields = res[data.type].fields ? res[data.type].fields : [];
    handleCodeData();
  }
  async function getTableData() {
    data.tableData = [];
    data.loading = true;
    setTableFilterConfig();
    let paramList = JSON.parse(JSON.stringify(filterConfig));
    if (data.type === 'measure') {
      paramList.tagProjection = paramList.projection;
      if (data.handleFields.length > 0) {
        paramList.fieldProjection = {
          names: data.handleFields,
        };
      }
      delete paramList.projection;
    }
    paramList.name = data.resourceData.metadata.name;
    paramList.groups = [data.resourceData.metadata.group];
    const res = await getTableList(paramList, data.type);
    data.loading = false;
    if (res.error) {
      ElMessage({
        message: `Get ${data.type} failed: ${res.error.message}`,
        type: 'error',
      });
      return;
    }
    if (data.type === 'stream') {
      setTableData(res.elements);
    } else {
      setTableData(res.dataPoints);
    }
  }
  function setTableData(elements) {
    const tags = data.resourceData.tagFamilies[data.tagFamily].tags;
    const tableFields = data.tableFields;
    for (const item of elements) {
      const dataItem = {};
      for (const tag of item.tagFamilies[0].tags) {
        const index = tags.findIndex((item) => item.name === tag.key);
        const type = tags[index].type;
        if (tag.value[tagType[type]] === null) {
          dataItem[tag.key] = 'Null';
        } else {
          dataItem[tag.key] = tag.value[tagType[type]]?.value || tag.value[tagType[type]];
        }
      }
      if (data.type === 'measure' && tableFields.length > 0) {
        item.fields.forEach((field) => {
          const name = field.name;
          const fieldType =
            tableFields.filter((tableField) => {
              return tableField.name === name;
            })[0].fieldType || '';
          if (field.value[fieldTypes[fieldType]] === null) {
            dataItem[name] = 'Null';
          } else {
            dataItem[name] = field.value[fieldTypes[fieldType]]?.value || field.value[fieldTypes[fieldType]];
          }
        });
      }
      dataItem.timestamp = item.timestamp;
      data.tableData.push(dataItem);
    }
    data.loading = false;
  }
  function setTableFilterConfig() {
    const tagFamily = data.resourceData.tagFamilies[data.tagFamily];
    const tagsList = [];
    tagFamily.tags.forEach((item) => {
      tagsList.push(item.name);
    });
    filterConfig.projection.tagFamilies[0].name = tagFamily.name;
    filterConfig.projection.tagFamilies[0].tags = tagsList;
    filterConfig.stages = data.byStages ? filterConfig.stages : undefined;
  }
  function changeTagFamilies() {
    data.tableTags = data.resourceData.tagFamilies[data.tagFamily].tags.map((item) => {
      item.label = item.name;
      if (data.handleFields.length > 0) {
        item.label = `${data.options[data.tagFamily].label}.${item.name}`;
      }
      return item;
    });
    getTableData();
  }
  function handleCodeData() {
    const json = yamlToJson(data.code).data;
    filterConfig.offset = json.offset !== undefined ? json.offset : 0;
    filterConfig.limit = json.limit !== undefined ? json.limit : 10;
    delete filterConfig.timeRange;
    if (json.timeRange && !isNaN(Date.parse(json.timeRange.begin)) && !isNaN(Date.parse(json.timeRange.end))) {
      data.timeValue = [json.timeRange.begin, json.timeRange.end];
      filterConfig.timeRange = json.timeRange;
    } else if (json.timeRange.begin || json.timeRange.end) {
      data.timeValue = [];
      ElMessage({
        dangerouslyUseHTMLString: true,
        showClose: true,
        message: 'Warning: Wrong time type',
        type: 'warning',
        duration: 5000,
      });
    } else {
      data.timeValue = [];
    }
    json.orderBy ? (filterConfig.orderBy = json.orderBy) : delete filterConfig.orderBy;

    // Add other fields from json to filterConfig
    Object.keys(json).forEach((key) => {
      if (!['offset', 'limit', 'timeRange', 'orderBy'].includes(key)) {
        filterConfig[key] = json[key];
      }
    });

    getTableData();
  }
  function searchTableData() {
    yamlRef.value
      .checkYaml(data.code)
      .then(() => {
        handleCodeData();
      })
      .catch((err) => {
        ElMessage({
          dangerouslyUseHTMLString: true,
          showClose: true,
          message: err.message,
          type: 'error',
          duration: 5000,
        });
      });
  }
  function changeDatePicker() {
    const json = yamlToJson(data.code);
    if (!json.data.hasOwnProperty('timeRange')) {
      json.data.timeRange = {
        begin: '',
        end: '',
      };
    }
    json.data.timeRange.begin = data.timeValue ? data.timeValue[0] : null;
    json.data.timeRange.end = data.timeValue ? data.timeValue[1] : null;
    data.code = jsonToYaml(json.data).data;
  }
  function changeFields() {
    data.tableFields = data.handleFields.map((fieldName) => {
      let item = data.fields.filter((field) => {
        return field.name === fieldName;
      })[0];
      item.label = item.name;
      return item;
    });
    getTableData();
  }
</script>

<template>
  <div>
    <el-card shadow="always">
      <template #header>
        <FormHeader :fields="data" />
      </template>
      <el-row>
        <el-col :span="16">
          <div class="flex align-item-center" style="height: 40px; width: 100%">
            <el-select
              v-model="data.tagFamily"
              @change="changeTagFamilies"
              filterable
              placeholder="Please select"
              style="width: 200px"
            >
              <el-option v-for="item in data.options" :key="item.value" :label="item.label" :value="item.value">
              </el-option>
            </el-select>
            <el-select
              v-if="data.type === 'measure'"
              v-model="data.handleFields"
              collapse-tags
              style="margin: 0 0 0 10px; flex: 0 0 300px"
              @change="changeFields"
              filterable
              multiple
              placeholder="Please select Fields"
            >
              <el-option v-for="item in data.fields" :key="item.name" :label="item.name" :value="item.name">
              </el-option>
            </el-select>
            <el-date-picker
              @change="changeDatePicker"
              style="margin: 0 10px; flex: 1 1 0"
              v-model="data.timeValue"
              type="datetimerange"
              :shortcuts="Shortcuts"
              range-separator="to"
              start-placeholder="begin"
              end-placeholder="end"
              :align="`right`"
            >
            </el-date-picker>
            <el-checkbox
              v-model="data.byStages"
              @change="setCode"
              label="By stages"
              size="large"
              style="margin-right: 10px"
            />
            <el-button :icon="Search" @click="searchTableData" style="flex: 0 0 auto" color="#6E38F7" plain></el-button>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="flex align-item-center justify-end" style="height: 30px">
            <el-button :icon="RefreshRight" @click="getTableData" plain></el-button>
          </div>
        </el-col>
      </el-row>
      <CodeMirror ref="yamlRef" v-model="data.code" mode="yaml" style="height: 200px" :lint="true" :readonly="false">
      </CodeMirror>
    </el-card>
    <el-card shadow="always">
      <el-table
        v-loading="data.loading"
        element-loading-text="loading"
        element-loading-spinner="el-icon-loading"
        element-loading-background="rgba(0, 0, 0, 0.8)"
        ref="multipleTable"
        stripe
        :border="true"
        highlight-current-row
        tooltip-effect="dark"
        empty-text="No data yet"
        :data="data.tableData"
      >
        <el-table-column type="selection" width="55"> </el-table-column>
        <el-table-column type="index" label="number" width="90"> </el-table-column>
        <el-table-column label="timestamp" width="260" key="timestamp" prop="timestamp"></el-table-column>
        <el-table-column
          v-for="item in tableHeader"
          sortable
          :key="item.name"
          :label="item.label"
          :prop="item.name"
          show-overflow-tooltip
        >
          <template #default="scope">
            <el-popover
              v-if="(item.type || item.fieldType)?.includes(`ARRAY`) && scope.row[item.name] !== `Null`"
              effect="dark"
              trigger="hover"
              placement="top"
              width="auto"
            >
              <template #default>
                <div>{{ scope.row[item.name].join('; ') }}</div>
              </template>
              <template #reference>
                <el-tag>View</el-tag>
              </template>
            </el-popover>
            <div v-else>{{ scope.row[item.name] }}</div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<style lang="scss" scoped>
  :deep(.el-card) {
    margin: 15px;
  }
</style>
