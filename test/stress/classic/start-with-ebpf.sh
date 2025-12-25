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

echo "Starting BanyanDB with eBPF sidecar..."

# Start BanyanDB in the background
/banyand "$@" &
BANYAND_PID=$!
echo "BanyanDB started with PID: $BANYAND_PID"

# Wait a bit for BanyanDB to initialize
sleep 5

# Check if ebpf-sidecar binary exists
if [ ! -f "/usr/local/bin/ebpf-sidecar" ]; then
    echo "WARNING: ebpf-sidecar binary not found at /usr/local/bin/ebpf-sidecar"
    echo "eBPF monitoring will not be available"
    echo "To enable it, build ebpf-sidecar first: cd ebpf-sidecar && make build"
    # Just wait for BanyanDB
    wait $BANYAND_PID
    exit $?
fi

# Start eBPF sidecar
echo "Starting eBPF sidecar to monitor BanyanDB (PID: $BANYAND_PID)..."
EXPORT_MODE="${EXPORT_MODE:-prometheus}"
EXPORT_INTERVAL="${EXPORT_INTERVAL:-10s}"
OUTPUT_DIR="${OUTPUT_DIR:-/ebpf-output}"
HTTP_PORT="${HTTP_PORT:-18080}"
GRPC_PORT="${GRPC_PORT:-19090}"
LOG_LEVEL="${LOG_LEVEL:-info}"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Start eBPF sidecar in the background
/usr/local/bin/ebpf-sidecar \
    --export-mode="$EXPORT_MODE" \
    --export-interval="$EXPORT_INTERVAL" \
    --output-dir="$OUTPUT_DIR" \
    --http-port="$HTTP_PORT" \
    --grpc-port="$GRPC_PORT" \
    --log-level="$LOG_LEVEL" \
    --target-pid="$BANYAND_PID" \
    2>&1 | sed 's/^/[eBPF] /' &

EBPF_PID=$!
echo "eBPF sidecar started with PID: $EBPF_PID"
echo "eBPF HTTP metrics available at: http://localhost:$HTTP_PORT/metrics"
echo "eBPF output directory: $OUTPUT_DIR"

# Function to handle shutdown
shutdown() {
    echo "Shutting down..."
    kill -TERM $EBPF_PID 2>/dev/null || true
    kill -TERM $BANYAND_PID 2>/dev/null || true
    wait $EBPF_PID 2>/dev/null || true
    wait $BANYAND_PID 2>/dev/null || true
    exit 0
}

# Trap signals
trap shutdown SIGTERM SIGINT

# Wait for either process to exit
wait -n $BANYAND_PID $EBPF_PID
EXIT_CODE=$?

# If one exits, kill the other
kill -TERM $EBPF_PID 2>/dev/null || true
kill -TERM $BANYAND_PID 2>/dev/null || true

exit $EXIT_CODE
