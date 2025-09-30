#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file to
# you under the Apache License, Version 2.0 (the
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

# Container stats collection script
# Collects performance metrics during test execution

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
STATS_FILE="$SCRIPT_DIR/container-stats.json"
SUMMARY_FILE="$SCRIPT_DIR/performance-summary.txt"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Container names
STREAM_CONTAINER="banyandb-stream"
TRACE_CONTAINER="banyandb-trace"

# Function to check if containers exist
check_containers() {
    if ! docker ps --format "table {{.Names}}" | grep -q "$STREAM_CONTAINER"; then
        echo -e "${YELLOW}Warning: Stream container ($STREAM_CONTAINER) not found${NC}"
        return 1
    fi
    
    if ! docker ps --format "table {{.Names}}" | grep -q "$TRACE_CONTAINER"; then
        echo -e "${YELLOW}Warning: Trace container ($TRACE_CONTAINER) not found${NC}"
        return 1
    fi
    
    return 0
}

# Function to collect stats for a single container
collect_container_stats() {
    local container_name=$1
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Get container stats in JSON format
    local stats=$(docker stats --no-stream --format "json" "$container_name" 2>/dev/null || echo "{}")
    
    # Add timestamp and container name
    echo "$stats" | jq --arg timestamp "$timestamp" --arg container "$container_name" \
        '. + {timestamp: $timestamp, container: $container}' 2>/dev/null || echo "{}"
}

# Function to start continuous stats collection
start_stats_collection() {
    echo -e "${GREEN}Starting container stats collection...${NC}"
    
    # Initialize stats file
    echo "[]" > "$STATS_FILE"
    
    # Check if containers exist
    if ! check_containers; then
        echo -e "${YELLOW}Some containers not found, stats collection may be incomplete${NC}"
    fi
    
    # Start background collection process
    (
        while true; do
            # Collect stats for both containers
            local stream_stats=$(collect_container_stats "$STREAM_CONTAINER")
            local trace_stats=$(collect_container_stats "$TRACE_CONTAINER")
            
            # Add to stats file
            if [ "$stream_stats" != "{}" ] || [ "$trace_stats" != "{}" ]; then
                local temp_file=$(mktemp)
                jq --argjson stream "$stream_stats" --argjson trace "$trace_stats" \
                    '. + [$stream, $trace]' "$STATS_FILE" > "$temp_file" 2>/dev/null || echo "[]" > "$temp_file"
                mv "$temp_file" "$STATS_FILE"
            fi
            
            sleep 5  # Collect every 5 seconds
        done
    ) &
    
    echo $! > "$SCRIPT_DIR/stats_pid"
    echo -e "${GREEN}Stats collection started (PID: $(cat $SCRIPT_DIR/stats_pid))${NC}"
}

# Function to stop stats collection
stop_stats_collection() {
    if [ -f "$SCRIPT_DIR/stats_pid" ]; then
        local pid=$(cat "$SCRIPT_DIR/stats_pid")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo -e "${GREEN}Stats collection stopped${NC}"
        fi
        rm -f "$SCRIPT_DIR/stats_pid"
    fi
}

# Function to generate performance summary
generate_summary() {
    echo -e "${GREEN}Generating performance summary...${NC}"
    
    if [ ! -f "$STATS_FILE" ] || [ ! -s "$STATS_FILE" ]; then
        echo -e "${YELLOW}No stats data found${NC}"
        return 1
    fi
    
    # Create summary file
    cat > "$SUMMARY_FILE" << EOF
========================================
Container Performance Summary
========================================
Generated: $(date)
Test Duration: $(jq -r '.[0].timestamp + " to " + .[-1].timestamp' "$STATS_FILE" 2>/dev/null || echo "Unknown")

EOF

    # Process stats for each container
    for container in "$STREAM_CONTAINER" "$TRACE_CONTAINER"; do
        echo "Processing stats for $container..."
        
        # Filter stats for this container
        local container_stats=$(jq --arg container "$container" '.[] | select(.container == $container)' "$STATS_FILE")
        
        if [ -z "$container_stats" ] || [ "$container_stats" = "" ]; then
            echo -e "${YELLOW}No stats found for $container${NC}"
            continue
        fi
        
        # Calculate metrics
        local cpu_avg=$(echo "$container_stats" | jq -r '.CPUPerc' | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local cpu_max=$(echo "$container_stats" | jq -r '.CPUPerc' | sed 's/%//' | awk 'BEGIN{max=0} {if($1>max) max=$1} END {print max}')
        local mem_avg=$(echo "$container_stats" | jq -r '.MemUsage' | awk -F'/' '{print $1}' | sed 's/[^0-9.]//g' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        local mem_max=$(echo "$container_stats" | jq -r '.MemUsage' | awk -F'/' '{print $1}' | sed 's/[^0-9.]//g' | awk 'BEGIN{max=0} {if($1>max) max=$1} END {print max}')
        local mem_limit=$(echo "$container_stats" | jq -r '.MemUsage' | awk -F'/' '{print $2}' | head -1 | sed 's/[^0-9.]//g')
        local net_rx=$(echo "$container_stats" | jq -r '.NetIO' | awk -F'/' '{print $1}' | head -1)
        local net_tx=$(echo "$container_stats" | jq -r '.NetIO' | awk -F'/' '{print $2}' | head -1)
        local block_read=$(echo "$container_stats" | jq -r '.BlockIO' | awk -F'/' '{print $1}' | head -1)
        local block_write=$(echo "$container_stats" | jq -r '.BlockIO' | awk -F'/' '{print $2}' | head -1)
        local sample_count=$(echo "$container_stats" | jq -s 'length')
        
        # Add to summary
        cat >> "$SUMMARY_FILE" << EOF

----------------------------------------
$container Performance Metrics
----------------------------------------
Sample Count: $sample_count

CPU Usage:
  Average: ${cpu_avg}%
  Maximum: ${cpu_max}%

Memory Usage:
  Average: ${mem_avg}MB
  Maximum: ${mem_max}MB
  Limit: ${mem_limit}MB
  Usage %: $(echo "scale=2; $mem_avg * 100 / $mem_limit" | bc 2>/dev/null || echo "N/A")%

Network I/O:
  Received: $net_rx
  Transmitted: $net_tx

Block I/O:
  Read: $block_read
  Write: $block_write

EOF
    done
    
    # Add comparison section
    cat >> "$SUMMARY_FILE" << EOF

----------------------------------------
Performance Comparison
----------------------------------------
EOF

    # Compare CPU usage
    local stream_cpu_avg=$(jq --arg container "$STREAM_CONTAINER" '.[] | select(.container == $container) | .CPUPerc' "$STATS_FILE" | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
    local trace_cpu_avg=$(jq --arg container "$TRACE_CONTAINER" '.[] | select(.container == $container) | .CPUPerc' "$STATS_FILE" | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
    
    if [ "$stream_cpu_avg" != "0" ] && [ "$trace_cpu_avg" != "0" ]; then
        local cpu_ratio=$(echo "scale=2; $stream_cpu_avg / $trace_cpu_avg" | bc 2>/dev/null || echo "N/A")
        echo "CPU Usage Ratio (Stream/Trace): $cpu_ratio" >> "$SUMMARY_FILE"
    fi
    
    # Compare Memory usage
    local stream_mem_avg=$(jq --arg container "$STREAM_CONTAINER" '.[] | select(.container == $container) | .MemUsage' "$STATS_FILE" | awk -F'/' '{print $1}' | sed 's/[^0-9.]//g' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
    local trace_mem_avg=$(jq --arg container "$TRACE_CONTAINER" '.[] | select(.container == $container) | .MemUsage' "$STATS_FILE" | awk -F'/' '{print $1}' | sed 's/[^0-9.]//g' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
    
    if [ "$stream_mem_avg" != "0" ] && [ "$trace_mem_avg" != "0" ]; then
        local mem_ratio=$(echo "scale=2; $stream_mem_avg / $trace_mem_avg" | bc 2>/dev/null || echo "N/A")
        echo "Memory Usage Ratio (Stream/Trace): $mem_ratio" >> "$SUMMARY_FILE"
    fi
    
    echo -e "${GREEN}Performance summary saved to: $SUMMARY_FILE${NC}"
    echo -e "${GREEN}Raw stats data saved to: $STATS_FILE${NC}"
}

# Function to cleanup
cleanup() {
    stop_stats_collection
    rm -f "$SCRIPT_DIR/stats_pid"
}

# Main execution
case "${1:-start}" in
    start)
        start_stats_collection
        ;;
    stop)
        stop_stats_collection
        ;;
    summary)
        generate_summary
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 {start|stop|summary|cleanup}"
        exit 1
        ;;
esac
