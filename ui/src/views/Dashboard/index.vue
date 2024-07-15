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
import { ref, watchEffect } from 'vue';
import { getTableList } from '@/api/index'

const value = ref(15000);
const options = ref([
    { value: 15000, label: '15 seconds' },
    { value: 30000, label: '30 seconds' },
    { value: 60000, label: '1 minute' },
    { value: 300000, label: '5 minutes' },
]);

const utcTime = ref({
    now: '',
    fiftheenSecondsAgo: ''
});
const commonParams = {
    groups: ["_monitoring"],
    offset: 0,
    orderBy: {
        indexRuleName: "",
        sort: "SORT_UNSPECIFIED"
    },
    fieldProjection: {
        names: [
            "value"
        ]
    }
};
const tagProjectionUptime = {
    tagFamilies: [
        {
            name: "default",
            tags: ["node_type", "node_id", "grpc_address", "http_address"]
        }
    ]
}
const tagProjection = {
    tagFamilies: [
        {
            name: "default",
            tags: ["node_id", "kind"]
        }
    ]
}
const tagProjectionDisk = {
    tagFamilies: [
        {
            name: "default",
            tags: ["node_id", "kind", "path"]
        }
    ]
}
const nodes = ref([]);

function formatUptime(seconds) {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    return `${hrs > 0 ? `${hrs}h ` : ''}${mins}m ${secs}s`;
}

function extractAddress(fullAddress) {
    const parts = fullAddress.split(':');
    return parts[parts.length - 1];
}

function formatBytes(bytes) {
    if (bytes === 0 || bytes === undefined) return 'N/A';
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
}

const colors = [
    { color: '#5cb87a', percentage: 51 },
    { color: '#edc374', percentage: 81 },
    { color: '#f56c6c', percentage: 100 },
];

async function fetchNodes() {
    getCurrentUTCTime()
    const [upTimeDataPoints, cpuDataPoints, memoryDataPoints, diskDataPoints] = await Promise.all([
        fetchDataPoints("up_time", tagProjectionUptime),
        fetchDataPoints("cpu_state", tagProjection),
        fetchDataPoints("memory_state", tagProjection),
        fetchDataPoints("disk", tagProjectionDisk),
    ]);
    // create table rows using uptime datapoints 
    const rows = getLatestForEachNode(upTimeDataPoints).map(item => {
        const tags = item.tagFamilies[0].tags;
        const nodeType = tags.find(tag => tag.key === 'node_type').value.str.value;
        const nodeId = tags.find(tag => tag.key === 'node_id').value.str.value;
        const grpcAddress = extractAddress(tags.find(tag => tag.key === 'grpc_address').value.str.value);
        const httpAddress = extractAddress(tags.find(tag => tag.key === 'http_address').value.str.value);
        const value = item.fields.find(field => field.name === 'value').value.float.value;
        return {
            node_id: nodeId,
            node_type: nodeType,
            grpc_address: grpcAddress,
            http_address: httpAddress,
            uptime: value
        };
    });
    rows.sort((a, b) => {
        return a.node_id.localeCompare(b.node_id);
    });
    // group by other metrics 
    const cpuData = groupBy(cpuDataPoints, "kind");
    const memoryData = groupBy(memoryDataPoints, "kind")
    const paths = groupBy(diskDataPoints, "path")
    const diskData = Object.keys(paths).reduce((acc, path) => {
        acc[path] = groupBy(paths[path], 'kind');
        return acc;
    }, {});
    // attach metrics to table 
    rows.forEach(row => {
        row.cpu = getLatestField(cpuData.user, row.node_id);
        row.memory = {
            used: getLatestField(memoryData.used, row.node_id),
            total: getLatestField(memoryData.total, row.node_id),
            used_percent: getLatestField(memoryData.used_percent, row.node_id),
        };
        row.disk = {}
        for (const path in diskData) {
            row.disk[path] = {
                used: getLatestField(diskData[path].used, row.node_id),
                total: getLatestField(diskData[path].total, row.node_id),
                used_percent: getLatestField(diskData[path].used_percent, row.node_id)
            }
        }
    });

    // Post-process row data
    rows.forEach(row => {
        row.uptime = formatUptime(row.uptime);
    });
    nodes.value = rows
}

function getCurrentUTCTime() {
    const now = new Date();
    utcTime.value.now = now.toISOString();

    const fiftheenSecondsAgo = new Date(now.getTime() - 15000);
    utcTime.value.fiftheenSecondsAgo = fiftheenSecondsAgo.toISOString();
}

async function fetchDataPoints(type, tagProjection) {
    const params = JSON.parse(JSON.stringify(commonParams));
    params.name = type;
    params.timeRange = {
        begin: utcTime.value.fiftheenSecondsAgo,
        end: utcTime.value.now,
    };
    params.tagProjection = tagProjection
    const res = await getTableList(params, "measure");
    if (res.status === 200) {
        return res.data.dataPoints;
    }
    return null; // Handle the case when status is not 200
}

function groupBy(data, key) {
    return data.reduce((acc, obj) => {
        const keyValue = obj.tagFamilies[0].tags.find(tag => tag.key === key).value.str.value;
        if (!acc[keyValue]) {
            acc[keyValue] = [];
        }
        acc[keyValue].push(obj);
        return acc;
    }, {});
}

// depuplicate by getting the latest data for each node id 
function getLatestForEachNode(data) {
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

// get latest field value by nodeId 
function getLatestField(data, nodeId) {
    let latestItem = null;
    let latestTimestamp = 0;

    // Iterate through each item in the data array
    data.forEach(item => {
        const nodeIdTag = item.tagFamilies[0].tags.find(tag => tag.key === 'node_id');
        const currentNodeId = nodeIdTag.value.str.value;
        const timestamp = new Date(item.timestamp).getTime(); // Convert timestamp to milliseconds

        // Check if the current item matches the nodeId and is the latest
        if (currentNodeId === nodeId && timestamp > latestTimestamp) {
            latestTimestamp = timestamp;
            latestItem = item;
        }
    });

    // Return the first field value if a matching latest item is found
    if (latestItem && latestItem.fields.length > 0) {
        return latestItem.fields[0].value.float.value;
    }

    // Return null if no matching item is found or there are no fields
    return null;
}

// Define a function to format date and time
function formatDate(date) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}`;
}

// Function to get current UTC time and formatted time range
function getFormattedUTCTimeRange() {
    const endTime = new Date();
    const startTime = new Date(endTime.getTime() - 30 * 60 * 1000); // Adding 30 minutes to start time
    return {
        formattedStartTime: formatDate(startTime),
        formattedEndTime: formatDate(endTime),
    };
}

const { formattedStartTime, formattedEndTime } = getFormattedUTCTimeRange();

let intervalId;

watchEffect(() => {
  if (intervalId) clearInterval(intervalId);
  fetchNodes();
  intervalId = setInterval(fetchNodes, value.value);
});
</script>

<template>
    <div class="dashboard">
        <div class="header-container">
            <span class="timestamp">
                <span class="timestamp-item">{{ formattedStartTime }} ~ {{ formattedEndTime }}</span>
                <span class="timestamp-item">UTC+0:0</span>
                <span>Auto Fresh:</span>
            </span>
            <el-select v-model="value" placeholder="Select" class="auto-fresh-select">
                <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value" />
            </el-select>
        </div>

        <el-card shadow="always">
            <template #header>
                <div class="card-header">
                    <span>Nodes</span>
                </div>
            </template>
            <el-table v-loading="nodes.loading" element-loading-text="loading" element-loading-spinner="el-icon-loading"
                element-loading-background="rgba(0, 0, 0, 0.8)" stripe border highlight-current-row
                tooltip-effect="dark" empty-text="No data yet" :data="nodes" style="width: 100%;">
                <el-table-column prop="node_id" label="Node ID" width="150"></el-table-column>
                <el-table-column prop="node_type" label="Type" width="150"></el-table-column>
                <el-table-column prop="uptime" label="Uptime" width="150"></el-table-column>
                <el-table-column label="CPU" width="200">
                    <template #default="scope">
                        <el-progress type="dashboard" :percentage="parseFloat((scope.row.cpu * 100).toFixed(2))"
                            :color="colors" />
                    </template>
                </el-table-column>
                <el-table-column label="Memory" width="350">
                    <template #default="scope">
                        <div class="memory-detail">
                            <div class="progress-container">
                                <el-progress type="line"
                                    :percentage="parseFloat((scope.row.memory.used_percent * 100).toFixed(2))"
                                    color="#82b0fa" :stroke-width="6" :show-text="false" class="fixed-progress-bar" />
                            </div>
                            <div class="memory-stats">
                                <span>Used: {{ formatBytes(scope.row.memory.used) }}</span>
                                <span>Total: {{ formatBytes(scope.row.memory.total) }}</span>
                                <span>
                                    Free: {{
                                        scope.row.memory.total && scope.row.memory.used
                                            ? formatBytes(scope.row.memory.total - scope.row.memory.used)
                                            : 'N/A'
                                    }}
                                </span>
                            </div>
                        </div>
                    </template>
                </el-table-column>

                <el-table-column label="Disk Details" width="450">
                    <template #default="scope">
                        <div class="disk-detail" v-for="(value, key) in scope.row.disk" :key="key">
                            <div class="progress-container">
                                <span class="disk-key">{{ key }}:</span>
                                <el-progress type="line" :percentage="parseFloat((value.used_percent * 100).toFixed(2))"
                                    color="#82b0fa" :stroke-width="6" :show-text="false" class="fixed-progress-bar" />
                            </div>
                            <div class="disk-stats">
                                <span>Used: {{ formatBytes(value.used) }}</span>
                                <span>Total: {{ formatBytes(value.total) }}</span>
                                <span>
                                    Free: {{
                                        value.total && value.used
                                            ? formatBytes(value.total - value.used)
                                            : 'N/A'
                                    }}
                                </span>
                            </div>
                        </div>
                    </template>
                </el-table-column>

                <el-table-column label="Port" width="250">
                    <template #default="scope">
                        <div>
                            <div>gRPC: {{ scope.row.grpc_address }}</div>
                            <div>HTTP: {{ scope.row.http_address || 'N/A' }}</div>
                        </div>
                    </template>
                </el-table-column>
            </el-table>
        </el-card>
    </div>
</template>

<style lang="scss" scoped>
.header-container {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    /* Aligns items to the right */
    margin: 15px 15px 10px 0;
    /* Space between the header and the card */
}

.timestamp {
    font-size: 16px;
    /* Adjust the font size as needed */
    color: #666;
    /* Adjust the text color as needed */
    margin-right: 10px;
    /* Space between the timestamp and the select */
}

.timestamp-item {
    margin-right: 12px;
    /* Add space between each item */
}

.auto-fresh-select {
    width: 200px;
    /* Adjust the width as needed */
}

.dashboard .el-table {
    margin: 10px; // Adjust the padding value as needed
}

.dashboard .el-card {
    margin: 15px; // Adjust the margin value as needed
    padding: 0;
}

.card-header {
    font-size: 20px;
    /* Make the text bigger */
    height: 10px;


}

.header-text {
    padding: 0;
    margin: 0;

    hr {
        margin: 0;
        border-top: 1px solid grey; // Adjust color as needed
    }
}

.fixed-progress-bar {
    width: 220px; // Fixed length for the progress bar
}

.demo-progress .el-progress--line {
    margin-bottom: 15px;
    max-width: 100px;
}

.demo-progress .el-progress--circle {
    margin-right: 15px;
}

.memory-detail,
.disk-detail {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    margin-bottom: 20px;
}

.disk-key {
    margin-right: 10px; // Adjust the margin value as needed
    font-weight: bold;
    color: #606266; // Adjust the color as needed
}

.progress-container {
    display: flex;
    align-items: left;
    margin-bottom: 10px;
    width: 100%;
}

.memory-stats,
.disk-stats {
    display: flex;
    justify-content: flex-start;
    text-align: left;
    gap: 10px;
    text-align: left;
    width: 100%;
}
</style>