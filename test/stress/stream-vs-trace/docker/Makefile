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

.PHONY: help build up down test clean logs ps stats all

# Default target
help:
	@echo "Stream vs Trace Docker Test - Make Targets"
	@echo "=========================================="
	@echo "  all      - Run complete test (build, up, test, down)"
	@echo "  build    - Build Docker images"
	@echo "  up       - Start containers and wait for health"
	@echo "  test     - Run tests (containers must be running)"
	@echo "  down     - Stop and remove containers"
	@echo "  clean    - Clean up everything including volumes"
	@echo "  logs     - Show container logs"
	@echo "  ps       - Show container status"
	@echo "  stats    - Show container resource usage"

# Run complete test workflow
all:
	./run-docker-test.sh all

# Build Docker images
build:
	./run-docker-test.sh build

# Start containers
up:
	./run-docker-test.sh up

# Stop containers
down:
	./run-docker-test.sh down

# Run the test (assumes containers are already running)
test:
	./run-docker-test.sh test

# Clean everything
clean:
	./run-docker-test.sh clean

# Show logs
logs:
	./run-docker-test.sh logs

# Show container status
ps:
	./run-docker-test.sh ps

# Show resource usage
stats:
	./run-docker-test.sh stats