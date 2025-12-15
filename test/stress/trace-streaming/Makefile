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

# Makefile for Trace Streaming Performance Tests

.PHONY: help build docker-build docker-up docker-down test-small test-all profiles clean plot-comparison

# Default target
help:
	@echo "Trace Streaming Performance Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build                 - Build BanyanDB binary for Docker"
	@echo "  docker-build          - Build Docker image"
	@echo "  docker-up             - Start BanyanDB container (4GB limit)"
	@echo "  docker-down           - Stop BanyanDB container"
	@echo "  test-small            - Run small scale test (5 min, 100K traces)"
	@echo "  test-all              - Run all tests (currently just small)"
	@echo "  plot-comparison       - Plot write vs query comparison (auto-detects CSV files)"
	@echo "  profiles              - Generate pprof reports from captured profiles"
	@echo "  clean                 - Clean up containers and volumes"

# Build BanyanDB binary
build:
	@echo "Building BanyanDB binary..."
	cd ../../.. && make release

# Build Docker image
docker-build: build
	@echo "Building Docker image..."
	docker-compose build

# Start BanyanDB container
docker-up:
	@echo "Starting BanyanDB container..."
	docker-compose up -d
	@echo "Waiting for BanyanDB to be healthy..."
	@sleep 5
	@docker-compose ps

# Stop BanyanDB container
docker-down:
	@echo "Stopping BanyanDB container..."
	docker-compose down

# Run small scale test
test-small:
	@echo "Running small scale test (100K traces, ~5 min)..."
	go test -v -timeout 30m -ginkgo.focus="Small Scale"

# Run all tests
test-all: test-small

# Generate pprof reports
profiles:
	@echo "Generating pprof reports..."
	@if [ -d "profiles" ]; then \
		for heap in profiles/heap_*.pprof; do \
			if [ -f "$$heap" ]; then \
				echo "Processing $$heap..."; \
				go tool pprof -text "$$heap" > "$${heap%.pprof}.txt"; \
				go tool pprof -svg "$$heap" > "$${heap%.pprof}.svg" 2>/dev/null || true; \
			fi; \
		done; \
		for cpu in profiles/cpu_*.pprof; do \
			if [ -f "$$cpu" ]; then \
				echo "Processing $$cpu..."; \
				go tool pprof -text "$$cpu" > "$${cpu%.pprof}.txt"; \
				go tool pprof -svg "$$cpu" > "$${cpu%.pprof}.svg" 2>/dev/null || true; \
			fi; \
		done; \
		echo "Reports generated in profiles/"; \
	else \
		echo "No profiles directory found"; \
	fi

# Plot write vs query comparison
plot-comparison:
	@if [ -f "memory_write_small.csv" ] && [ -f "memory_query_small.csv" ]; then \
		echo "Plotting write vs query comparison (small scale)..."; \
		python3 plot_memory_comparison.py memory_write_small.csv memory_query_small.csv --prefix memory_small; \
		echo "Generated: memory_small_write.png, memory_small_query.png, memory_small_comparison.png"; \
	else \
		echo "No write/query CSV pairs found. Run a test first."; \
		echo "Expected files: memory_write_small.csv and memory_query_small.csv"; \
		exit 1; \
	fi

# Clean up
clean:
	@echo "Cleaning up containers and volumes..."
	docker-compose down -v
	@echo "Removing generated files..."
	rm -rf profiles/ *.csv *.pprof *.png
	@echo "Cleanup complete"

# Docker-based test execution
docker-test-small: docker-up
	@echo "Running small scale test against Docker container..."
	@sleep 10  # Wait for container to be fully ready
	BANYANDB_ADDR=localhost:17912 go test -v -timeout 30m -ginkgo.focus="Small Scale"

# View container logs
logs:
	docker-compose logs -f banyandb-streaming

# View memory usage
memory:
	@docker stats banyandb-streaming --no-stream

# Access pprof heap profile
pprof-heap:
	@echo "Accessing heap profile..."
	go tool pprof http://localhost:6060/debug/pprof/heap

# Access pprof CPU profile (30 second sample)
pprof-cpu:
	@echo "Sampling CPU profile for 30 seconds..."
	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
