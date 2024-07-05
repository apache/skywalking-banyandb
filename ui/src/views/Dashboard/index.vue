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
import { ref, onMounted } from 'vue';
import { getTableList } from '@/api/index'

const utcTime = ref({
    now: '',
    thirtySecondsAgo: ''
});

const upTimeDataContent = ref(null);
const memoryDataContent = ref(null);
const diskDataContent = ref(null);

function getCurrentUTCTime() {
    const now = new Date();
    utcTime.value.now = now.toISOString();

    const FiftheenSecondsAgo = new Date(now.getTime() - 15000);
    utcTime.value.thirtySecondsAgo = FiftheenSecondsAgo.toISOString();
}

const commonParams = {
    groups: ["_monitoring"],
    offset: 0,
    limit: 0,
    orderBy: {
        indexRuleName: "",
        sort: "SORT_UNSPECIFIED"
    },
    tagProjection: {
        tagFamilies: [
            {
                name: "default",
                tags: ["node_type", "node_id", "grpc_address", "http_address"]
            }
        ]
    },
    fieldProjection: {
        names: [
            "value"
        ]
    }
};

function fetchData() {
    getCurrentUTCTime();
    fetchUptime();
    fetchMemory();
    fetchDisk();
}

function fetchUptime() {
    const params = JSON.parse(JSON.stringify(commonParams));
    params.name = "up_time"
    params.timeRange =  {
        begin: utcTime.value.thirtySecondsAgo,
        end: utcTime.value.now,
    },
    getTableList(params, "measure")
        .then((res) => {
            if (res.status === 200) {
                upTimeDataContent.value = getLatestDataPointsForEachNode(res.data.dataPoints)
            }
        })
}

function fetchMemory() {
    const params = JSON.parse(JSON.stringify(commonParams));
    params.name = "memory_state"
    params.timeRange =  {
        begin: utcTime.value.thirtySecondsAgo,
        end: utcTime.value.now,
    },
    getTableList(params, "measure")
        .then((res) => {
            if (res.status === 200) {
                memoryDataContent.value = getLatestDataPointsForEachNode(res.data.dataPoints)
            }
        })
}

function fetchDisk() {
    const params = JSON.parse(JSON.stringify(commonParams));
    params.name = "disk"
    params.timeRange =  {
        begin: utcTime.value.thirtySecondsAgo,
        end: utcTime.value.now,
    },
    getTableList(params, "measure")
        .then((res) => {
            if (res.status === 200) {
                diskDataContent.value = getLatestDataPointsForEachNode(res.data.dataPoints)
            }
        })
}


// get the latest data for each node id 
function getLatestDataPointsForEachNode(data) {
    const nodeDataMap = {};
    data.forEach(item => {
        const nodeIdTag = item.tagFamilies[0].tags.find(tag => tag.key === "node_id");
        const nodeId = nodeIdTag.value.str.value;
        const timestamp = new Date(item.timestamp).getTime();

        if (!nodeDataMap[nodeId] || timestamp > nodeDataMap[nodeId].timestamp) {
            nodeDataMap[nodeId] = { ...item, timestamp };
        }
    });

    const uniqueNodeData = Object.values(nodeDataMap).map(item => {
        delete item.timestamp; // Remove the timestamp property added for comparison
        return item;
    });
    return uniqueNodeData
}

onMounted(() => {
    fetchData();
    // Optional: Update the time every 15 seconds
    setInterval(fetchData, 15000);
});


function getTagValue(tags, key) {
    const tag = tags.find(tag => tag.key === key);
    return tag ? tag.value.str.value : null;
}

function getFieldValue(fields, name) {
    const field = fields.find(field => field.name === name);
    return field ? field.value.float.value : null;
}
</script>

<template>
    <div>
        <h1 class="home">
            This is the dashboard page
        </h1>
        <div>
            <h1>Current UTC Time</h1>
            <p>Now: {{ utcTime.now }}</p>
            <p>30 Seconds Ago: {{ utcTime.thirtySecondsAgo }}</p>

            <h2>Fetched Data</h2>
            <div v-if="upTimeDataContent && upTimeDataContent.length > 0">
                <div v-for="(item, index) in upTimeDataContent" :key="index" class="data-row">
                    <!-- Customize the display of each item as needed -->
                    <p><strong>Node ID:</strong> {{ getTagValue(item.tagFamilies[0].tags, 'node_id') }}</p>
                    <p><strong>Node Type:</strong> {{ getTagValue(item.tagFamilies[0].tags, 'node_type') }}</p>
                    <p><strong>gRPC Address:</strong> {{ getTagValue(item.tagFamilies[0].tags, 'grpc_address') }}</p>
                    <p><strong>HTTP Address:</strong> {{ getTagValue(item.tagFamilies[0].tags, 'http_address') }}</p>
                    <p><strong>Up Time:</strong> {{ getFieldValue(item.fields, 'value') }}</p>
                </div>
            </div>
            <div v-if="memoryDataContent && memoryDataContent.length > 0">
                <div v-for="(item, index) in memoryDataContent" :key="index" class="data-row">
                    <p><strong>Memory:</strong> {{ getFieldValue(item.fields, 'value') }}</p>
                </div>
            </div>
            <div v-if="diskDataContent && diskDataContent.length > 0">
                <div v-for="(item, index) in diskDataContent" :key="index" class="data-row">
                    <p><strong>Disk:</strong> {{ getFieldValue(item.fields, 'value') }}</p>
                </div>
            </div>
        </div>
    </div>
</template>

<style lang="scss" scoped></style>