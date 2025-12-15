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

export const Last15Minutes = 900 * 1000;

export const Last30Minutes = 1800 * 1000;

export const LastHour = 3600 * 1000;

export const LastDay = 3600 * 1000 * 24;

export const LastWeek = 3600 * 1000 * 24 * 7;

export const LastMonth = 3600 * 1000 * 24 * 30;

export const Last3Months = 3600 * 1000 * 24 * 90;

export const Shortcuts = [
  {
    text: 'Last 15 minutes',
    value: () => createRange(Last15Minutes),
  },
  {
    text: 'Last 30 minutes',
    value: () => createRange(Last30Minutes),
  },
  {
    text: 'Last hour',
    value: () => createRange(LastHour),
  },
  {
    text: 'Last day',
    value: () => createRange(LastDay),
  },
  {
    text: 'Last week',
    value: () => createRange(LastWeek),
  },
  {
    text: 'Last month',
    value: () => createRange(LastMonth),
  },
  {
    text: 'Last 3 months',
    value: () => createRange(Last3Months),
  },
];

function createRange(duration) {
  const end = new Date();
  const start = new Date(end.getTime() - duration);
  return [start, end];
}

// catalog to group type
export const CatalogToGroupType = {
  CATALOG_MEASURE: 'measure',
  CATALOG_STREAM: 'stream',
  CATALOG_PROPERTY: 'property',
  CATALOG_TRACE: 'trace',
  CATALOG_TOPN: 'topn',
};

// group type to catalog
export const GroupTypeToCatalog = {
  measure: 'CATALOG_MEASURE',
  stream: 'CATALOG_STREAM',
  property: 'CATALOG_PROPERTY',
  trace: 'CATALOG_TRACE',
  topn: 'CATALOG_TOPN',
};

export const TypeMap = {
  topNAggregation: 'topn-agg',
  indexRule: 'index-rule',
  indexRuleBinding: 'index-rule-binding',
  children: 'children',
};

export const SupportedIndexRuleTypes = [
  CatalogToGroupType.CATALOG_STREAM,
  CatalogToGroupType.CATALOG_MEASURE,
  CatalogToGroupType.CATALOG_TRACE,
];
