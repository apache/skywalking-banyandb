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


# ========= General Variable Definitions =========
SHELL := /bin/bash
ROOTDIR := $(shell cd ../../../ && pwd)
PWD := $(shell pwd)

REPORTS_DIR := $(PWD)/reports
BPF_DIR := $(PWD)/bpf
BPF_HEADERS_DIR := $(BPF_DIR)
BPF_SRC := $(PWD)/bpfsrc/fadvise.c

# ========= Build Entries =========
.PHONY: bpf clean generate-bpf prepare-bpf-headers \
	bpf-benchmark docker-bpf-benchmark \
	all-benchmarks benchmark-write benchmark-seqread benchmark-merge \
	benchmark-sequential-merge benchmark-multiple-reads benchmark-mixed \
	benchmark-concurrent benchmark-threshold benchmark-memory

bpf: prepare-bpf-headers generate-bpf

# ========= Auto Generate vmlinux.h =========
prepare-bpf-headers:
	@echo "Preparing eBPF headers..."
	@if [ ! -f $(BPF_HEADERS_DIR)/vmlinux.h ]; then \
		echo "Generating vmlinux.h (requires bpftool and Linux environment)"; \
		bpftool btf dump file /sys/kernel/btf/vmlinux format c > $(BPF_HEADERS_DIR)/vmlinux.h; \
	else \
		echo "vmlinux.h already exists."; \
	fi

# ========= Multi-Platform eBPF Loader Generation =========
generate-bpf:
	@echo "Generating eBPF code for amd64..."
	@cd $(BPF_DIR) && \
	GOARCH=amd64 GOOS=linux bpf2go -cc clang -target bpfel \
		-cflags "-O2 -g -Wall -I$(BPF_HEADERS_DIR)" \
		-output-stem bpf_x86 -go-package bpf \
		Bpf ../bpfsrc/fadvise.c && \
	sed -i.bak '/^\/\/go:build/c\//go:build (386 || amd64) \&\& linux' bpf_x86_bpfel.go && \
	rm -f bpf_x86_bpfel.go.bak

	@echo "Generating eBPF code for arm64..."
	@cd $(BPF_DIR) && \
	GOARCH=arm64 GOOS=linux bpf2go -cc clang -target bpfel \
		-cflags "-O2 -g -Wall -I$(BPF_HEADERS_DIR)" \
		-output-stem bpf_arm64 -go-package bpf \
		Bpf ../bpfsrc/fadvise.c && \
	sed -i.bak '/^\/\/go:build/c\//go:build arm64 \&\& linux' bpf_arm64_bpfel.go && \
	rm -f bpf_arm64_bpfel.go.bak

	@echo "eBPF code generation completed for amd64 and arm64."

# ========= BPF Benchmarking =========
bpf-benchmark: prepare-bpf-headers generate-bpf
	@mkdir -p $(REPORTS_DIR)
	@echo "Running fadvis benchmarks with eBPF tracing..."
	@go test -v -tags linux -bench=BenchmarkWithBPF -benchtime=5s -run=^$ ./... -benchmem | tee $(REPORTS_DIR)/bpf_benchmarks.txt
	@echo "BPF benchmark results saved to $(REPORTS_DIR)/bpf_benchmarks.txt"

# ========= Running BPF Benchmark in Docker =========
docker-bpf-benchmark:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running eBPF benchmarks in Docker container..."
	@docker run --rm -it \
		--cap-add=SYS_ADMIN \
		--privileged \
		-v $(ROOTDIR):/skywalking-banyandb \
		-w /skywalking-banyandb/test/stress/fadvis \
		golang:1.21-bullseye \
		/bin/bash -c " \
			apt-get update && \
			apt-get install -y clang llvm bpftool && \
			make prepare-bpf-headers generate-bpf && \
			go test -v -tags linux -bench=BenchmarkWithBPF -benchtime=5s -run=^$ ./... -benchmem \
		" | tee $(REPORTS_DIR)/docker_bpf_benchmarks.txt
	@echo "Docker BPF benchmark results saved to $(REPORTS_DIR)/docker_bpf_benchmarks.txt"

# ========= General Benchmark Process (Updated) =========
all-benchmarks: benchmark-write benchmark-seqread benchmark-multiple-reads benchmark-mixed \
	benchmark-concurrent-ops benchmark-concurrent-merges benchmark-threshold benchmark-memory

benchmark-write:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running write performance benchmarks (with/without fadvis)..."
	@go test -v -run=^$$ -bench=BenchmarkWritePerformance -benchmem -count=3 ./benchmark | tee $(REPORTS_DIR)/write_benchmarks.txt

# Removed benchmark-read target as the corresponding test has been removed

benchmark-seqread:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running sequential read benchmarks (with/without fadvis)..."
	@go test -v -run=^$$ -bench=BenchmarkSequentialRead -benchmem -count=3 ./benchmark | tee $(REPORTS_DIR)/sequential_read_benchmarks.txt

benchmark-multiple-reads:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running multiple reads benchmarks (with/without fadvis)..."
	@go test -v -run=^$$ -bench=BenchmarkMultipleReads -benchmem -count=3 ./benchmark | tee $(REPORTS_DIR)/multiple_reads_benchmarks.txt

benchmark-mixed:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running mixed workload benchmarks (with/without fadvis)..."
	@go test -v -run=^$$ -bench=BenchmarkMixedWorkload -benchmem -count=3 ./benchmark | tee $(REPORTS_DIR)/mixed_benchmarks.txt

benchmark-concurrent-ops:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running concurrent operations benchmarks..."
	@go test -v -run=^$$ -bench=BenchmarkConcurrentOperations -benchmem -benchtime=5s ./benchmark | tee $(REPORTS_DIR)/concurrent_ops_benchmarks.txt

benchmark-concurrent-merges:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running concurrent merge benchmarks..."
	@go test -v -run=^$$ -bench=BenchmarkConcurrentMerges -benchmem -benchtime=5s ./benchmark | tee $(REPORTS_DIR)/concurrent_merges_benchmarks.txt

benchmark-threshold:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running threshold adaptation benchmarks..."
	@go test -v -run=^$$ -bench=BenchmarkThresholdAdaptation -benchmem -count=3 ./benchmark | tee $(REPORTS_DIR)/threshold_benchmarks.txt

benchmark-memory:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running memory usage benchmarks with monitoring..."
	@bash -c "\
		echo 'Starting cache monitoring...' && \
		(while true; do grep -E 'Cached|Buffers|MemFree' /proc/meminfo >> $(REPORTS_DIR)/memory_stats.txt; sleep 1; done) & \
		monitoring_pid=\$$! && \
		echo 'Running benchmarks...' && \
		go test -bench=. -benchmem -count=1 ./benchmark | tee $(REPORTS_DIR)/memory_benchmarks.txt && \
		kill \$$monitoring_pid && \
		echo 'Memory usage benchmark results saved to $(REPORTS_DIR)/memory_benchmarks.txt' && \
		echo 'Memory monitoring data saved to $(REPORTS_DIR)/memory_stats.txt'"

# ========= Cleanup =========
clean:
	@echo "Cleaning up reports and BPF artifacts..."
	@rm -rf $(REPORTS_DIR)
	@rm -f $(BPF_DIR)/*_bpfel.*
	@rm -f $(BPF_DIR)/vmlinux.h
	@echo "Done."
