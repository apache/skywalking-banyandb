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

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../.." && pwd )"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default command
COMMAND=""

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}Docker is not running. Please start Docker and try again.${NC}"
        exit 1
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build     Build Docker images"
    echo "  up        Start containers and wait for health"
    echo "  test      Run performance tests (containers must be running)"
    echo "  down      Stop and remove containers"
    echo "  clean     Clean up everything including volumes"
    echo "  logs      Show container logs"
    echo "  ps        Show container status"
    echo "  stats     Show container resource usage"
    echo "  all       Run complete test (build, up, test, down)"
    echo "  help      Show this help message"
    echo ""
    echo "Options:"
    echo "  --help    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all              # Run complete test workflow"
    echo "  $0 build            # Just build images"
    echo "  $0 up               # Start containers"
    echo "  $0 test             # Run tests"
    echo "  $0 down             # Stop containers"
}

# Function to build Docker images
do_build() {
    echo -e "${GREEN}Building Docker containers...${NC}"
    cd "$PROJECT_ROOT"
    make generate
    PLATFORMS=linux/amd64 make -C banyand release
    
    cd "$SCRIPT_DIR"
    docker compose build
    echo -e "${GREEN}Build complete.${NC}"
}

# Function to start containers
do_up() {
    echo -e "${GREEN}Starting Docker containers...${NC}"
    cd "$SCRIPT_DIR"
    docker compose up -d
    
    echo -e "${GREEN}Waiting for containers to be healthy...${NC}"
    ./wait-for-healthy.sh
    echo -e "${GREEN}Containers are ready.${NC}"
}

# Function to run tests
do_test() {
    echo -e "${GREEN}Running performance tests...${NC}"
    cd "$PROJECT_ROOT"
    
    # Set environment variable to enable Docker test
    export DOCKER_TEST=true
    
    # Run the test from the docker directory to ensure relative paths work
    cd "$SCRIPT_DIR"
    go test -v -timeout 30m ./... -run TestStreamVsTraceDocker
    
    echo -e "${GREEN}Test completed successfully!${NC}"
    
    # Optional: Show container resource usage
    echo -e "\n${YELLOW}Container Resource Usage:${NC}"
    docker stats --no-stream banyandb-stream banyandb-trace
}

# Function to stop containers
do_down() {
    echo -e "${YELLOW}Stopping containers...${NC}"
    cd "$SCRIPT_DIR"
    docker compose down
    echo -e "${GREEN}Containers stopped.${NC}"
}

# Function to clean everything
do_clean() {
    echo -e "${YELLOW}Cleaning up everything...${NC}"
    cd "$SCRIPT_DIR"
    docker compose down -v
    docker compose rm -f
    echo -e "${GREEN}Cleanup complete.${NC}"
}

# Function to show logs
do_logs() {
    cd "$SCRIPT_DIR"
    docker compose logs -f
}

# Function to show container status
do_ps() {
    cd "$SCRIPT_DIR"
    docker compose ps
}

# Function to show container stats
do_stats() {
    docker stats --no-stream banyandb-stream banyandb-trace
}

# Function to run all steps
do_all() {
    echo -e "${GREEN}Stream vs Trace Performance Test - Complete Workflow${NC}"
    echo -e "${GREEN}====================================================${NC}"
    
    check_docker
    do_build
    do_up
    do_test
    do_down
}

# Parse command
if [ $# -eq 0 ]; then
    show_usage
    exit 0
fi

COMMAND=$1
shift

# Parse remaining arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
    shift
done

# Execute the command
case $COMMAND in
    build)
        check_docker
        do_build
        ;;
    up)
        check_docker
        do_up
        ;;
    test)
        do_test
        ;;
    down)
        do_down
        ;;
    clean)
        do_clean
        ;;
    logs)
        do_logs
        ;;
    ps)
        do_ps
        ;;
    stats)
        do_stats
        ;;
    all)
        do_all
        ;;
    help)
        show_usage
        exit 0
        ;;
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        show_usage
        exit 1
        ;;
esac