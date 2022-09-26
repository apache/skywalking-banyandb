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
defineProps({
    keyName: {
        type: String,
    },
    value: {
        type: [String, Number]
    }
})
</script>
 
<template>
    <div style="width: 100%; height: 45px;" class="flex justify-between bd-bottom align-item-center">
        <div class="text-main-color text-tips text-family">{{keyName}}</div>
        <div style="width: 50%; text-align: right;"
            class="text-secondary-color text-tips text-family text-overflow-hidden">
            <span :title="value">{{value}}</span>
        </div>
    </div>
</template>