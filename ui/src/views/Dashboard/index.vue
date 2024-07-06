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

async function fetchTableData() {
    getCurrentUTCTime()
    const [upTimeDataPoints, cpuDataPoints, memoryDataPoints, diskDataPoints] = await Promise.all([
        fetchDataPoints("up_time", tagProjectionUptime),
        fetchDataPoints("cpu_state", tagProjection),
        fetchDataPoints("memory_state", tagProjection),
        fetchDataPoints("disk", tagProjectionDisk),
    ]);
    // create table rows using uptime datapoints 
    const rows = deduplicateByNodeId(upTimeDataPoints).map(item => {
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
    const diskDataByPath = groupBy(diskDataPoints, "path")
    const diskData = Object.keys(diskDataByPath).reduce((acc, path) => {
        acc[path] = groupBy(diskDataByPath[path], 'kind');
        return acc;
    }, {});
    // attach metrics to table 
    rows.forEach(node => {
        node.cpu = getFieldValueByNodeId(cpuData.system, node.node_id);
        node.memory = {
            used: getFieldValueByNodeId(memoryData.used, node.node_id),
            total: getFieldValueByNodeId(memoryData.total, node.node_id),
            used_percent: getFieldValueByNodeId(memoryData.used_percent, node.node_id),
        };
        node.disk = {}
        for (const path in diskData) {
            node.disk[path] = {
                used: getFieldValueByNodeId(diskData[path].used, node.node_id),
                total: getFieldValueByNodeId(diskData[path].total, node.node_id),
                used_percent: getFieldValueByNodeId(diskData[path].used_percent, node.node_id)
            }
        }
    });
    nodes.value = rows
    console.log(rows)
}

function getCurrentUTCTime() {
    const now = new Date();
    utcTime.value.now = now.toISOString();

    const FiftheenSecondsAgo = new Date(now.getTime() - 15000);
    utcTime.value.thirtySecondsAgo = FiftheenSecondsAgo.toISOString();
}

async function fetchDataPoints(type, tagProjection) {
    const params = JSON.parse(JSON.stringify(commonParams));
    params.name = type;
    params.timeRange = {
        begin: utcTime.value.thirtySecondsAgo,
        end: utcTime.value.now,
    };
    params.tagProjection = tagProjection
    const res = await getTableList(params, "measure");
    if (res.status === 200) {
        return res.data.dataPoints;
    }
    return null; // Handle the case when status is not 200
}

function deduplicateByNodeId(data) {
    const nodeDataMap = {};
    data.forEach(item => {
        const nodeIdTag = item.tagFamilies[0].tags.find(tag => tag.key === "node_id");
        const nodeId = nodeIdTag.value.str.value;

        // Only add the item if it hasn't been added before
        if (!nodeDataMap[nodeId]) {
            nodeDataMap[nodeId] = item;
        }
    });

    // The values in nodeDataMap are the first entries for each node ID
    return Object.values(nodeDataMap);
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

function getFieldValueByNodeId(data, nodeId) {
    // Find the object with the matching node_id
    const item = data.find(item => {
        const nodeIdTag = item.tagFamilies[0].tags.find(tag => tag.key === 'node_id');
        return nodeIdTag.value.str.value === nodeId;
    });

    // Return the first field value if the object is found
    if (item && item.fields.length > 0) {
        return item.fields[0].value.float.value;
    }

    // Return null if the object is not found or there are no fields
    return null;
}

onMounted(() => {
    fetchTableData();
    // Optional: Update the time every 15 seconds
    setInterval(fetchTableData, 15000);
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
            <p>30 Seconds Ago: {{ utcTime.thirtySecondsAgo }}</p>
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