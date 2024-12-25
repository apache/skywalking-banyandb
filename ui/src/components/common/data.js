/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export const last15Minutes = 900 * 1000;

const lastWeek = 3600 * 1000 * 24 * 7;

const lastMonth = 3600 * 1000 * 24 * 30;

const last3Months = 3600 * 1000 * 24 * 90;

export const Shortcuts = [
  {
    text: 'Last 15 minutes',
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setTime(start.getTime() - last15Minutes);
      return [start, end];
    },
  },
  {
    text: 'Last week',
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setTime(start.getTime() - lastWeek);
      return [start, end];
    },
  },
  {
    text: 'Last month',
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setTime(start.getTime() - lastMonth);
      return [start, end];
    },
  },
  {
    text: 'Last 3 months',
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setTime(start.getTime() - last3Months);
      return [start, end];
    },
  },
];
