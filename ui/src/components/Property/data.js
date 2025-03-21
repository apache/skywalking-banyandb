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

const validateTags = (rule, value, callback) => {
  if (value.length <= 0) {
    callback(new Error('Please add the tags'));
  } else {
    callback();
  }
};

export const rules = {
  strategy: [{ required: true, message: 'please select the apply method.', trigger: 'blur' }],
  group: [{ required: true, message: 'Please enter the group.', trigger: 'blur' }],
  name: [{ required: true, message: 'Please enter the name.', trigger: 'blur' }],
  tags: [{ required: true, validator: validateTags, trigger: 'blur' }],
};
export const strategyGroup = [
  { label: 'STRATEGY_MERGE', value: 'STRATEGY_MERGE' },
  { label: 'STRATEGY_REPLACE', value: 'STRATEGY_REPLACE' },
];
export const formConfig = [
  { label: 'Strategy', prop: 'strategy', type: 'select', selectGroup: strategyGroup },
  { label: 'Group', prop: 'group', type: 'input', disabled: true },
  { label: 'Name', prop: 'name', type: 'input' },
];
