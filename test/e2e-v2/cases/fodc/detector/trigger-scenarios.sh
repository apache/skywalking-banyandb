#!/bin/bash
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the License.

set -e

echo "Starting FODC detector e2e test scenarios..."

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Scenario 1: Create a death rattle file
echo "Scenario 1: Creating death rattle file..."
docker exec fodc-test-helper sh -c "echo 'E2E test death rattle - container failing' > /tmp/death-rattle"
sleep 5

# Scenario 2: Create common death rattle paths
echo "Scenario 2: Creating common death rattle paths..."
docker exec fodc-test-helper sh -c "touch /tmp/container-failing"
sleep 3

# Scenario 3: Verify BanyanDB metrics are accessible
echo "Scenario 3: Verifying BanyanDB metrics endpoint..."
docker exec fodc-test-helper wget --quiet -O- http://banyandb:2121/metrics | head -5

# Scenario 4: Verify FODC health endpoint
echo "Scenario 4: Verifying FODC health endpoint..."
docker exec fodc-test-helper wget --quiet -O- http://fodc-sidecar:17914/healthz | head -5

# Scenario 5: Wait for detector to process events
echo "Scenario 5: Waiting for detector to process events..."
sleep 10

echo "Test scenarios completed"

