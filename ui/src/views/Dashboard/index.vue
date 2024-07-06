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
        const grpcAddress = tags.find(tag => tag.key === 'grpc_address').value.str.value;
        const httpAddress = tags.find(tag => tag.key === 'http_address').value.str.value;
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
        row.cpu = getLatestField(cpuData.system, row.node_id);
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

onMounted(() => {
    fetchNodes();
    // Optional: Update the time every 15 seconds
    setInterval(fetchNodes, 15000);
});
</script>

<template>
    <div>
        <h1 class="home">
            This is the dashboard page
        </h1>
        <div>
            <h1>Current UTC Time</h1>
            <p>Now: {{ utcTime.now }}</p>
            <p>15 Seconds Ago: {{ utcTime.fiftheenSecondsAgo }}</p>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Node ID</th>
                    <th>Type</th>
                    <th>gRPC Address</th>
                    <th>HTTP Address</th>
                    <th>Uptime</th>
                    <th>CPU</th>
                    <th>Memory Used</th>
                    <th>Memory Total</th>
                    <th>Memory Used %</th>
                    <th>Disk Details</th>
                </tr>
            </thead>
            <tbody>
                <tr v-for="node in nodes" :key="node.node_id">
                    <td>{{ node.node_id }}</td>
                    <td>{{ node.node_type }}</td>
                    <td>{{ node.grpc_address }}</td>
                    <td>{{ node.http_address || 'N/A' }}</td>
                    <td>{{ node.uptime.toFixed(2) }} s</td>
                    <td>{{ node.cpu }}</td>
                    <td>{{ node.memory.used }}</td>
                    <td>{{ node.memory.total }}</td>
                    <td>{{ (node.memory.used_percent * 100).toFixed(2) }}%</td>
                    <td>
                        <div v-for="(value, key) in node.disk" :key="key">
                            {{ key }}: Used: {{ value.used || 'N/A' }}, Total: {{ value.total || 'N/A' }}, Used %: {{
                                value.used_percent ? (value.used_percent * 100).toFixed(2) + '%' : 'N/A' }}
                        </div>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
</template>

<style lang="scss" scoped></style>