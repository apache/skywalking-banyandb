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

mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
mk_dir   := $(dir $(mk_path))
root_dir := $(mk_dir)../../..

# Test output directory
OUTPUT_DIR := /tmp/ebpf-sidecar-test

# Docker compose environment
cli_env := DOCKER_BUILDKIT=1 USER_ID=$(shell id -u) GROUP_ID=$(shell id -g)

.PHONY: all
all: up generate-load show-metrics

.PHONY: build-sidecar
build-sidecar:
	@echo "Building eBPF sidecar..."
	@(cd $(root_dir)/ebpf-sidecar && make build)

.PHONY: prepare
prepare: clean
	@echo "Preparing test directories..."
	@mkdir -p $(OUTPUT_DIR)
	@chmod 777 $(OUTPUT_DIR)

.PHONY: up
up: prepare build-sidecar
	@echo "Starting test environment..."
	$(cli_env) docker compose --env-file ./env up -d --build
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Test environment is ready!"

.PHONY: down
down:
	@echo "Stopping test environment..."
	docker compose down -v

.PHONY: clean
clean:
	@echo "Cleaning up test artifacts..."
	@rm -rf $(OUTPUT_DIR)
	@docker compose down -v 2>/dev/null || true

.PHONY: generate-load
generate-load:
	@echo "Generating I/O load patterns..."
	docker compose exec -T load-generator /scripts/generate_load.sh

.PHONY: test-basic
test-basic:
	@echo "Running basic I/O test..."
	docker compose exec -T load-generator /scripts/test_basic_io.sh

.PHONY: test-cache
test-cache:
	@echo "Running cache miss test..."
	docker compose exec -T load-generator /scripts/test_cache_miss.sh

.PHONY: test-memory
test-memory:
	@echo "Running memory pressure test..."
	docker compose exec -T load-generator /scripts/test_memory_pressure.sh

.PHONY: test-stress
test-stress:
	@echo "Running full stress test..."
	docker compose exec -T load-generator /scripts/test_stress.sh

.PHONY: show-metrics
show-metrics:
	@echo "=== Collected Metrics ==="
	@if [ -f $(OUTPUT_DIR)/metrics.json ]; then \
		cat $(OUTPUT_DIR)/metrics.json | jq .; \
	else \
		echo "No metrics file found. Checking container logs..."; \
		docker compose logs ebpf-sidecar | tail -20; \
	fi

.PHONY: query-db
query-db:
	@echo "Querying BanyanDB for metrics..."
	@docker compose exec -T banyandb bydbctl measure query \
		--name ebpf_metrics \
		--group system \
		--begin -10m \
		--end now

.PHONY: export-metrics
export-metrics:
	@echo "Exporting metrics to $(OUTPUT_DIR)/export.json..."
	@docker compose exec -T ebpf-sidecar curl -s http://localhost:8080/metrics > $(OUTPUT_DIR)/export.json
	@echo "Metrics exported to $(OUTPUT_DIR)/export.json"

.PHONY: logs
logs:
	@echo "=== eBPF Sidecar Logs ==="
	@docker compose logs ebpf-sidecar

.PHONY: logs-follow
logs-follow:
	@docker compose logs -f ebpf-sidecar

.PHONY: status
status:
	@echo "=== Container Status ==="
	@docker compose ps
	@echo ""
	@echo "=== Health Check ==="
	@curl -s http://localhost:8080/health 2>/dev/null && echo " - Sidecar is healthy" || echo " - Sidecar is not responding"
	@curl -s http://localhost:17912/api/healthz 2>/dev/null && echo " - BanyanDB is healthy" || echo " - BanyanDB is not responding"

.PHONY: shell-sidecar
shell-sidecar:
	@docker compose exec ebpf-sidecar /bin/sh

.PHONY: shell-load
shell-load:
	@docker compose exec load-generator /bin/bash

.PHONY: test-ci
test-ci: up test-basic show-metrics export-metrics
	@echo "CI test completed. Results in $(OUTPUT_DIR)"
	@$(MAKE) down

.PHONY: help
help:
	@echo "eBPF Sidecar Stress Testing"
	@echo ""
	@echo "Targets:"
	@echo "  up              - Start test environment"
	@echo "  down            - Stop test environment" 
	@echo "  clean           - Clean up all test artifacts"
	@echo "  generate-load   - Generate I/O load patterns"
	@echo "  test-basic      - Run basic I/O test"
	@echo "  test-cache      - Run cache miss test"
	@echo "  test-memory     - Run memory pressure test"
	@echo "  test-stress     - Run full stress test"
	@echo "  show-metrics    - Display collected metrics"
	@echo "  query-db        - Query BanyanDB for metrics"
	@echo "  export-metrics  - Export metrics to file"
	@echo "  logs            - Show sidecar logs"
	@echo "  logs-follow     - Follow sidecar logs"
	@echo "  status          - Show container and health status"
	@echo "  shell-sidecar   - Open shell in sidecar container"
	@echo "  shell-load      - Open shell in load generator"
	@echo "  test-ci         - Run CI test suite"
	@echo "  help            - Show this help message"