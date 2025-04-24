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

SHELL := /bin/bash
ROOTDIR := $(shell cd ../../../ && pwd)
PWD := $(shell pwd)
REPORTS_DIR := $(PWD)/reports

.PHONY: all-benchmarks benchmark-write benchmark-read benchmark-merge benchmark-sequential-merge benchmark-multiple-reads benchmark-mixed benchmark-concurrent benchmark-threshold benchmark-memory clean help

all-benchmarks: clean
	@mkdir -p $(REPORTS_DIR)
	@echo "Running all fadvis benchmarks..."
	@go test -bench=. -benchmem -count=3 | tee $(REPORTS_DIR)/all_benchmarks.txt
	@echo "Benchmark results saved to $(REPORTS_DIR)/all_benchmarks.txt"

benchmark-write:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running write performance benchmarks..."
	@go test -bench=BenchmarkWritePerformance -benchmem -count=3 | tee $(REPORTS_DIR)/write_benchmarks.txt
	@echo "Write benchmark results saved to $(REPORTS_DIR)/write_benchmarks.txt"

benchmark-read:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running read performance benchmarks..."
	@go test -v -bench=BenchmarkReadPerformance -benchtime=5s -run=^$ ./test/stress/fadvis/ -benchmem | tee $(REPORTS_DIR)/read_benchmarks.txt
	@echo "Read benchmark results saved to $(REPORTS_DIR)/read_benchmarks.txt"

benchmark-merge:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running merge operation benchmarks..."
	@go test -bench=BenchmarkMergeOperations -benchmem -count=3 | tee $(REPORTS_DIR)/merge_benchmarks.txt
	@echo "Merge benchmark results saved to $(REPORTS_DIR)/merge_benchmarks.txt"

benchmark-sequential-merge:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running sequential merge operation benchmarks..."
	@go test -bench=BenchmarkSequentialMergeOperations -benchmem -count=3 | tee $(REPORTS_DIR)/sequential_merge_benchmarks.txt
	@echo "Sequential merge benchmark results saved to $(REPORTS_DIR)/sequential_merge_benchmarks.txt"

benchmark-multiple-reads:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running multiple reads benchmarks..."
	@go test -bench=BenchmarkMultipleReads -benchmem -count=3 | tee $(REPORTS_DIR)/multiple_reads_benchmarks.txt
	@echo "Multiple reads benchmark results saved to $(REPORTS_DIR)/multiple_reads_benchmarks.txt"

benchmark-mixed:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running mixed workload benchmarks..."
	@go test -bench=BenchmarkMixedWorkload -benchmem -count=3 | tee $(REPORTS_DIR)/mixed_benchmarks.txt
	@echo "Mixed workload benchmark results saved to $(REPORTS_DIR)/mixed_benchmarks.txt"

benchmark-concurrent:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running concurrent operations benchmarks..."
	@go test -v -bench=BenchmarkConcurrent -benchtime=5s -run=^$ ./test/stress/fadvis/ -benchmem | tee $(REPORTS_DIR)/concurrent_benchmarks.txt
	@echo "Concurrent operations benchmark results saved to $(REPORTS_DIR)/concurrent_benchmarks.txt"

benchmark-threshold:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running threshold adaptation benchmarks..."
	@go test -bench=BenchmarkThresholdAdaptation -benchmem -count=3 | tee $(REPORTS_DIR)/threshold_benchmarks.txt
	@echo "Threshold adaptation benchmark results saved to $(REPORTS_DIR)/threshold_benchmarks.txt"

benchmark-memory:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running memory usage benchmarks with monitoring..."
	@bash -c "\
		echo 'Starting cache monitoring...' && \
		(while true; do grep -E 'Cached|Buffers|MemFree' /proc/meminfo >> $(REPORTS_DIR)/memory_stats.txt; sleep 1; done) & \
		monitoring_pid=\$$! && \
		echo 'Running benchmarks...' && \
		go test -bench=. -benchmem -count=1 | tee $(REPORTS_DIR)/memory_benchmarks.txt && \
		kill \$$monitoring_pid && \
		echo 'Memory usage benchmark results saved to $(REPORTS_DIR)/memory_benchmarks.txt' && \
		echo 'Memory monitoring data saved to $(REPORTS_DIR)/memory_stats.txt'"

clean:
	@echo "Cleaning up reports directory..."
	@rm -rf $(REPORTS_DIR)
	@mkdir -p $(REPORTS_DIR)
	@echo "Done."

help:
	@echo "Available targets:"
	@echo "  all-benchmarks          - Run all benchmarks with multiple iterations"
	@echo "  benchmark-write         - Run write performance benchmarks"
	@echo "  benchmark-read          - Run read performance benchmarks"
	@echo "  benchmark-merge         - Run merge operation benchmarks"
	@echo "  benchmark-sequential-merge - Run sequential merge operation benchmarks"
	@echo "  benchmark-multiple-reads - Run multiple reads benchmarks"
	@echo "  benchmark-mixed         - Run mixed workload benchmarks"
	@echo "  benchmark-concurrent    - Run concurrent operations benchmarks"
	@echo "  benchmark-threshold     - Run threshold adaptation benchmarks"
	@echo "  benchmark-memory        - Run benchmarks with memory usage monitoring"
	@echo "  clean                   - Clean up reports directory"
	@echo "  help                    - Show this help message"