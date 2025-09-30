#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Wait for containers to be healthy
timeout=60
elapsed=0

echo -n "Waiting for containers to be healthy"

while [ $elapsed -lt $timeout ]; do
    stream_health=$(docker inspect --format='{{.State.Health.Status}}' banyandb-stream 2>/dev/null || echo "not found")
    trace_health=$(docker inspect --format='{{.State.Health.Status}}' banyandb-trace 2>/dev/null || echo "not found")
    
    if [ "$stream_health" = "healthy" ] && [ "$trace_health" = "healthy" ]; then
        echo -e "\nBoth containers are healthy!"
        exit 0
    fi
    
    echo -n "."
    sleep 1
    elapsed=$((elapsed + 1))
done

echo -e "\nTimeout waiting for containers to be healthy."
docker compose ps
exit 1