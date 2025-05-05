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
	all-benchmarks benchmark-write benchmark-seqread \
	benchmark-multiple-reads benchmark-mixed \

bpf: prepare-bpf-headers generate-bpf
generate: bpf

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
	@echo ">>> Building Docker image for eBPF benchmarks…"
	@docker build --pull -t fs-fadvis-bench -f pkg/fs/fadvisemonitor/Dockerfile .
	@echo ">>> Running benchmarks inside container…"
	@docker run --rm \
		--cap-add=SYS_ADMIN \
		--privileged \
		-v $(ROOTDIR):/workspace \
		-w /workspace/pkg/fs/fadvisemonitor \
		fs-fadvis-bench \
		make bpf-benchmark | tee $(REPORTS_DIR)/docker_bpf_benchmarks.txt
	@echo "Docker BPF benchmark results saved to $(REPORTS_DIR)/docker_bpf_benchmarks.txt"

# ========= General Benchmark Process =========
all-benchmarks: benchmark-write benchmark-seqread benchmark-multiple-reads benchmark-mixed 

benchmark-write:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running write performance benchmarks (with/without fadvis)..."
	@ulimit -l unlimited && \
	go test -v -run=^$$ -bench=BenchmarkWritePerformance -benchtime=3x -benchmem ./benchmark | tee $(REPORTS_DIR)/write_benchmarks.txt

# Removed benchmark-read target as the corresponding test has been removed

benchmark-seqread:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running sequential read benchmarks (with/without fadvis)..."
	@ulimit -l unlimited && \
	go test -v -run=^$$ -bench=BenchmarkSequentialRead -benchtime=3x -benchmem ./benchmark | tee $(REPORTS_DIR)/sequential_read_benchmarks.txt

benchmark-multiple-reads:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running multiple reads benchmarks (with/without fadvis)..."
	@ulimit -l unlimited && \
	go test -v -run=^$$ -bench=BenchmarkMultipleReads -benchtime=3x -benchmem ./benchmark | tee $(REPORTS_DIR)/multiple_reads_benchmarks.txt

benchmark-mixed:
	@mkdir -p $(REPORTS_DIR)
	@echo "Running mixed workload benchmarks (with/without fadvis)..."
	@ulimit -l unlimited && \
	go test -v -run=^$$ -bench=BenchmarkMixedWorkload -benchtime=3x -benchmem ./benchmark | tee $(REPORTS_DIR)/mixed_benchmarks.txt

# ========= Cleanup =========
clean:
	@echo "Cleaning up reports and BPF artifacts..."
	@rm -rf $(REPORTS_DIR)
	@rm -f $(BPF_DIR)/*_bpfel.*
	@rm -f $(BPF_DIR)/vmlinux.h
	@echo "Done."
