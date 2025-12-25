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
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

echo "=== Verifying eBPF Sidecar Attachment ==="
echo ""

# Find the BanyanDB container
CONTAINER_NAME=$(docker ps --filter "name=classic.*banyandb" --format "{{.Names}}" | head -1)
if [ -z "$CONTAINER_NAME" ]; then
    echo "✗ BanyanDB container is NOT running"
    exit 1
fi

echo "Found BanyanDB container: $CONTAINER_NAME"
echo ""

# Check if BanyanDB container is running
echo "1. Checking if BanyanDB container is running..."
if docker ps | grep -q "$CONTAINER_NAME"; then
    echo "✓ BanyanDB container is running"
else
    echo "✗ BanyanDB container is NOT running"
    exit 1
fi
echo ""

# Check if ebpf-sidecar binary exists in container
echo "2. Checking if eBPF sidecar binary is present..."
if docker exec "$CONTAINER_NAME" test -f /usr/local/bin/ebpf-sidecar 2>/dev/null; then
    echo "✓ eBPF sidecar binary is present in container"
else
    echo "✗ eBPF sidecar binary NOT found"
    echo "  Build it first: cd ../../ebpf-sidecar && make build"
    exit 1
fi
echo ""

# Check container logs for eBPF messages
echo "3. Checking container logs for eBPF activity..."
if docker logs "$CONTAINER_NAME" 2>&1 | grep -q -E "\[eBPF\]|ebpf-sidecar|eBPF sidecar"; then
    echo "✓ eBPF sidecar appears to be running"
    echo ""
    echo "Recent eBPF logs:"
    docker logs "$CONTAINER_NAME" 2>&1 | grep -E "\[eBPF\]" | tail -10
else
    echo "⚠ Could not find eBPF messages in logs"
    echo ""
    echo "Recent logs:"
    docker logs --tail 20 "$CONTAINER_NAME" 2>&1
fi
echo ""

# Check if eBPF process is running inside container
echo "4. Checking if eBPF sidecar process is running..."
if docker exec "$CONTAINER_NAME" ps aux 2>/dev/null | grep -q "[e]bpf-sidecar"; then
    echo "✓ eBPF sidecar process is running"
    echo ""
    docker exec "$CONTAINER_NAME" ps aux | grep -E "(PID|ebpf-sidecar|banyand)" | grep -v grep
else
    echo "⚠ eBPF sidecar process not found"
fi
echo ""

# Check output directory
echo "5. Checking output directory..."
if [ -d "/tmp/ebpf-sidecar-classic" ]; then
    echo "✓ Output directory exists"
    echo "Files in output directory:"
    ls -lh /tmp/ebpf-sidecar-classic/ 2>/dev/null || echo "  (empty)"
else
    echo "⚠ Output directory does not exist yet"
fi
echo ""

# Try to access metrics endpoint
echo "6. Checking metrics endpoint..."
if curl -s -f http://localhost:18080/metrics > /dev/null 2>&1; then
    echo "✓ Metrics endpoint is accessible at http://localhost:18080/metrics"
    echo ""
    echo "Sample metrics:"
    curl -s http://localhost:18080/metrics | head -20
else
    echo "⚠ Metrics endpoint is not accessible (this is normal if eBPF sidecar is not running)"
fi
echo ""

echo "=== Verification Complete ==="
echo ""
echo "To monitor live I/O operations, run:"
echo "  docker logs -f $CONTAINER_NAME"
echo ""
echo "To see only eBPF logs:"
echo "  docker logs -f $CONTAINER_NAME 2>&1 | grep '\[eBPF\]'"
echo ""
echo "To access metrics:"
echo "  curl http://localhost:18080/metrics"
