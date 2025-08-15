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

# Build variables
GO := go
NAME := ebpf-sidecar
BUILD_DIR := build/bin

.PHONY: all
all: generate build

.PHONY: generate
generate: vmlinux ebpf-bindings

.PHONY: ebpf-bindings
ebpf-bindings:
	@echo "Generating eBPF Go bindings..."
	@mkdir -p internal/ebpf/generated
	@cd internal/ebpf/generated && \
		go run github.com/cilium/ebpf/cmd/bpf2go \
			-cc clang \
			-cflags "-O2 -Wall -Werror" \
			-target amd64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type lru_shrink_info_t \
			-type reclaim_info_t \
			-type cache_stats_t \
			-type cache_event_t \
			Iomonitor ../programs/iomonitor.c -- -I.
	@cd internal/ebpf/generated && \
		go run github.com/cilium/ebpf/cmd/bpf2go \
			-cc clang \
			-cflags "-O2 -Wall -Werror" \
			-target arm64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type lru_shrink_info_t \
			-type reclaim_info_t \
			-type cache_stats_t \
			-type cache_event_t \
			Iomonitor ../programs/iomonitor.c -- -I.

.PHONY: vmlinux
vmlinux: install-bpftool
	@echo "Generating vmlinux.h..."
	@mkdir -p internal/ebpf/generated
	@bpftool btf dump file /sys/kernel/btf/vmlinux format c > internal/ebpf/generated/vmlinux.h

.PHONY: install-bpftool
install-bpftool:
	@echo "Checking for bpftool..."
	@if command -v bpftool >/dev/null 2>&1; then \
		echo "bpftool found: $$(command -v bpftool)"; \
	else \
		echo "bpftool not found, installing..."; \
		$(MAKE) install-bpftool-auto; \
	fi

.PHONY: install-bpftool-auto
install-bpftool-auto:
	@echo "Detecting Linux distribution..."
	@if [ -f /etc/debian_version ]; then \
		echo "Debian/Ubuntu detected, installing bpftools..."; \
		if command -v apt-get >/dev/null 2>&1; then \
			apt-get update && apt-get install -y linux-tools-common linux-tools-generic bpftools || \
			apt-get install -y linux-tools-$$(uname -r) || \
			apt-get install -y bpftools; \
		else \
			echo "Error: apt-get not available"; \
			exit 1; \
		fi; \
	elif [ -f /etc/redhat-release ] || [ -f /etc/centos-release ] || [ -f /etc/fedora-release ]; then \
		echo "RedHat/CentOS/Fedora detected, installing bpftool..."; \
		if command -v dnf >/dev/null 2>&1; then \
			dnf install -y bpftool kernel-devel; \
		elif command -v yum >/dev/null 2>&1; then \
			yum install -y bpftool kernel-devel; \
		else \
			echo "Error: Neither dnf nor yum available"; \
			exit 1; \
		fi; \
	elif [ -f /etc/alpine-release ]; then \
		echo "Alpine Linux detected, installing bpftool..."; \
		if command -v apk >/dev/null 2>&1; then \
			apk add --no-cache bpftool linux-headers; \
		else \
			echo "Error: apk not available"; \
			exit 1; \
		fi; \
	else \
		echo "Unknown Linux distribution, trying to install from source..."; \
		$(MAKE) install-bpftool-source; \
	fi
	@echo "Verifying bpftool installation..."
	@if command -v bpftool >/dev/null 2>&1; then \
		echo "bpftool successfully installed: $$(command -v bpftool)"; \
		bpftool version; \
	else \
		echo "Error: bpftool installation failed"; \
		exit 1; \
	fi

.PHONY: install-bpftool-source
install-bpftool-source:
	@echo "Installing bpftool from kernel source..."
	@mkdir -p /tmp/bpftool-build
	@cd /tmp/bpftool-build && \
	if [ ! -d "linux" ]; then \
		echo "Downloading kernel source..."; \
		KERNEL_VERSION=$$(uname -r | cut -d- -f1); \
		wget -q "https://github.com/torvalds/linux/archive/v$$KERNEL_VERSION.tar.gz" -O kernel.tar.gz || \
		wget -q "https://cdn.kernel.org/pub/linux/kernel/v$${KERNEL_VERSION%%.*}.x/linux-$$KERNEL_VERSION.tar.gz" -O kernel.tar.gz || \
		{ echo "Failed to download kernel source, trying latest stable..."; \
		  git clone --depth=1 https://github.com/torvalds/linux.git || exit 1; }; \
		if [ -f kernel.tar.gz ]; then \
			tar -xf kernel.tar.gz && mv linux-* linux; \
		fi; \
	fi && \
	cd linux/tools/bpf/bpftool && \
	make && \
	cp bpftool /usr/local/bin/ && \
	echo "bpftool installed to /usr/local/bin/bpftool"
	@rm -rf /tmp/bpftool-build

.PHONY: install-deps
install-deps: install-bpftool
	@echo "Installing additional eBPF dependencies..."
	@if [ -f /etc/debian_version ]; then \
		apt-get install -y clang llvm libbpf-dev linux-headers-$$(uname -r) || \
		apt-get install -y clang llvm linux-headers-generic; \
	elif [ -f /etc/redhat-release ] || [ -f /etc/centos-release ] || [ -f /etc/fedora-release ]; then \
		if command -v dnf >/dev/null 2>&1; then \
			dnf install -y clang llvm libbpf-devel kernel-headers kernel-devel; \
		else \
			yum install -y clang llvm libbpf-devel kernel-headers kernel-devel; \
		fi; \
	elif [ -f /etc/alpine-release ]; then \
		apk add --no-cache clang llvm libbpf-dev linux-headers; \
	fi

.PHONY: build
build: generate
	@echo "Building $(NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build -o $(BUILD_DIR)/$(NAME) ./cmd/sidecar

.PHONY: clean
clean:
	@echo "Cleaning $(NAME)..."
	@rm -rf $(BUILD_DIR)
	@rm -f internal/ebpf/generated/*

.PHONY: test
test:
	@echo "Running tests for $(NAME)..."
	$(GO) test -v -race ./...

.PHONY: test-ebpf
test-ebpf:
	@echo "Running eBPF tests (requires root)..."
	@if [ "$$(id -u)" != "0" ]; then \
		echo "eBPF tests require root privileges. Run with sudo."; \
		exit 1; \
	fi
	$(GO) test -v -tags=ebpf ./internal/ebpf/...

.PHONY: docker
docker:
	@echo "Building Docker image for $(NAME)..."
	docker build -t skywalking-banyandb/$(NAME):latest .

.PHONY: run
run: build
	@echo "Running $(NAME)..."
	./$(BUILD_DIR)/$(NAME)

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all             - Generate and build"
	@echo "  install-deps    - Install all eBPF dependencies (bpftool, clang, etc)"
	@echo "  install-bpftool - Check and install bpftool if needed"
	@echo "  vmlinux         - Generate vmlinux.h header"
	@echo "  ebpf-bindings   - Generate eBPF Go bindings"
	@echo "  generate        - Generate vmlinux.h and eBPF bindings"
	@echo "  build           - Build the sidecar binary"
	@echo "  clean           - Clean build artifacts"
	@echo "  test            - Run unit tests"
	@echo "  test-ebpf       - Run eBPF tests (requires root)"
	@echo "  docker          - Build Docker image"
	@echo "  run             - Build and run the sidecar"
	@echo "  help            - Show this help message"