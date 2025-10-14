/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export const strategyGroup = [
  {
    value: 'STRATEGY_MERGE',
    label: 'STRATEGY_MERGE',
  },
  {
    value: 'STRATEGY_REPLACE',
    label: 'STRATEGY_REPLACE',
  },
];

export const tagTypeOptions = [
  { value: 'TAG_TYPE_STRING', label: 'TAG_TYPE_STRING' },
  { value: 'TAG_TYPE_INT', label: 'TAG_TYPE_INT' },
  { value: 'TAG_TYPE_STRING_ARRAY', label: 'TAG_TYPE_STRING_ARRAY' },
  { value: 'TAG_TYPE_INT_ARRAY', label: 'TAG_TYPE_INT_ARRAY' },
  { value: 'TAG_TYPE_DATA_BINARY', label: 'TAG_TYPE_DATA_BINARY' },
  { value: 'TAG_TYPE_TIMESTAMP', label: 'TAG_TYPE_TIMESTAMP' },
];

export const formConfig = [
  {
    label: 'Strategy',
    prop: 'strategy',
    type: 'select',
    selectGroup: strategyGroup,
  },
  {
    label: 'Group',
    prop: 'group',
    type: 'input',
    disabled: true,
  },
  {
    label: 'Name',
    prop: 'name',
    type: 'input',
    disabled: false,
  },
  {
    label: 'Trace ID Tag Name',
    prop: 'traceIdTagName',
    type: 'input',
    disabled: false,
  },
  {
    label: 'Timestamp Tag Name',
    prop: 'timestampTagName',
    type: 'input',
    disabled: false,
  },
  {
    label: 'Span ID Tag Name',
    prop: 'spanIdTagName',
    type: 'input',
    disabled: false,
  },
];

export const rules = {
  name: [{ required: true, message: 'Please input name', trigger: 'blur' }],
  group: [{ required: true, message: 'Please select group', trigger: 'blur' }],
  traceIdTagName: [{ required: true, message: 'Please input trace ID tag name', trigger: 'blur' }],
  timestampTagName: [{ required: true, message: 'Please input timestamp tag name', trigger: 'blur' }],
  spanIdTagName: [{ required: true, message: 'Please input span ID tag name', trigger: 'blur' }],
  tags: [{ required: true, message: 'Please add at least one tag', trigger: 'change' }],
};

