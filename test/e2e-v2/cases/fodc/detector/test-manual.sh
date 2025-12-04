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

# Manual test script for FODC detector e2e tests
# This script can be used to manually verify the e2e test setup

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== FODC Detector E2E Manual Test ==="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Check if services are running
echo "1. Checking if services are running..."
if ! docker-compose ps | grep -q "fodc-sidecar"; then
    echo "   Services are not running. Starting them..."
    docker-compose up -d
    echo "   Waiting for services to be ready..."
    sleep 30
else
    echo "   Services are already running"
fi

# Test 1: Check FODC health endpoint
echo ""
echo "2. Testing FODC health endpoint..."
if docker exec fodc-test-helper wget --quiet --tries=1 --spider http://fodc-sidecar:17914/healthz 2>/dev/null; then
    echo "   ✓ FODC health endpoint is accessible"
else
    echo "   ✗ FODC health endpoint is not accessible"
    exit 1
fi

# Test 2: Check health endpoint returns JSON
echo ""
echo "3. Testing health endpoint response..."
RESPONSE=$(docker exec fodc-test-helper wget --quiet -O- http://fodc-sidecar:17914/healthz 2>/dev/null | head -1)
if echo "$RESPONSE" | grep -q "{"; then
    echo "   ✓ Health endpoint returns JSON"
    echo "   Response preview: ${RESPONSE:0:50}..."
else
    echo "   ✗ Health endpoint does not return JSON"
    exit 1
fi

# Test 3: Check metadata
echo ""
echo "4. Testing metadata..."
if docker exec fodc-test-helper sh -c "wget --quiet -O- http://fodc-sidecar:17914/healthz | grep -q 'mode'" 2>/dev/null; then
    echo "   ✓ Metadata contains 'mode' field"
else
    echo "   ✗ Metadata does not contain 'mode' field"
    exit 1
fi

# Test 4: Test death rattle detection
echo ""
echo "5. Testing death rattle detection..."
docker exec fodc-test-helper sh -c "echo 'Manual test death rattle' > /tmp/death-rattle" 2>/dev/null
sleep 5
if docker exec fodc-test-helper sh -c "wget --quiet -O- http://fodc-sidecar:17914/healthz | grep -q 'death_rattle'" 2>/dev/null; then
    echo "   ✓ Death rattle detection is working"
else
    echo "   ⚠ Death rattle detection may not be working (check logs)"
fi

# Test 5: Check metrics polling
echo ""
echo "6. Testing metrics polling..."
sleep 5
if docker exec fodc-test-helper sh -c "wget --quiet -O- http://fodc-sidecar:17914/healthz | grep -q 'total_snapshots'" 2>/dev/null; then
    echo "   ✓ Metrics polling is working"
else
    echo "   ⚠ Metrics polling may not be working (check logs)"
fi

# Test 6: Check flight recorder
echo ""
echo "7. Testing flight recorder..."
if docker exec fodc-sidecar ls -la /data/fodc-flight-recorder.bin >/dev/null 2>&1; then
    echo "   ✓ Flight recorder file exists"
else
    echo "   ⚠ Flight recorder file may not exist yet (check logs)"
fi

echo ""
echo "=== Manual Test Complete ==="
echo ""
echo "To view logs:"
echo "  docker-compose logs -f fodc-sidecar"
echo ""
echo "To stop services:"
echo "  docker-compose down"

