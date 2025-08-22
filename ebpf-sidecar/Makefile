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

# ---------- Safety & Environment ----------
# Enable strict error handling
SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

# Ensure /usr/local/bin has priority (for custom-built bpftool)
export PATH := /usr/local/bin:$(PATH)

# Build variables
GO := go
NAME := ebpf-sidecar
BUILD_DIR := build/bin
UNAME_R := $(shell uname -r)
ARCH := $(shell uname -m)
CLANG ?= clang

.PHONY: all
all: generate build

.PHONY: generate
generate: vmlinux ebpf-bindings

# eBPF compilation flags
BPF_CFLAGS := -O2 -g -Wall -Werror -D__TARGET_ARCH_$(ARCH)

.PHONY: ebpf-bindings
ebpf-bindings:
	@echo "Generating eBPF Go bindings..."
	@mkdir -p internal/ebpf/generated
	@echo "Building for amd64..."
	@cd internal/ebpf/generated && \
		$(GO) run github.com/cilium/ebpf/cmd/bpf2go \
			-cc $(CLANG) \
			-cflags "$(BPF_CFLAGS)" \
			-target amd64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type lru_shrink_info_t \
			-type reclaim_info_t \
			-type cache_stats_t \
			-type cache_event_t \
			Iomonitor ../programs/iomonitor.c -- -I.
	@echo "Building for arm64..."
	@cd internal/ebpf/generated && \
		$(GO) run github.com/cilium/ebpf/cmd/bpf2go \
			-cc $(CLANG) \
			-cflags "$(BPF_CFLAGS)" \
			-target arm64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type lru_shrink_info_t \
			-type reclaim_info_t \
			-type cache_stats_t \
			-type cache_event_t \
			Iomonitor ../programs/iomonitor.c -- -I.
	@echo "eBPF bindings generated in internal/ebpf/generated"
	@# Verify build tags are present
	@grep -l "//go:build" internal/ebpf/generated/*.go 2>/dev/null | head -5 || true

.PHONY: vmlinux
vmlinux: install-bpftool
	@echo "Generating vmlinux.h..."
	@# Check if BTF is available
	@if [ ! -r "/sys/kernel/btf/vmlinux" ]; then \
		echo "ERROR: /sys/kernel/btf/vmlinux not readable. Your kernel may not have CONFIG_DEBUG_INFO_BTF=y."; \
		echo "Try: sudo apt install linux-image-$(UNAME_R) linux-modules-$(UNAME_R) linux-modules-extra-$(UNAME_R)"; \
		exit 1; \
	fi
	@mkdir -p internal/ebpf/generated
	@echo "Generating vmlinux.h from /sys/kernel/btf/vmlinux..."
	@bpftool btf dump file /sys/kernel/btf/vmlinux format c > internal/ebpf/generated/vmlinux.h
	@# Verify the file was generated and is not empty
	@test -s internal/ebpf/generated/vmlinux.h || { echo "ERROR: vmlinux.h generation failed"; exit 1; }

.PHONY: check-bpftool
check-bpftool:
	@echo "Checking for bpftool..."
	@if ! command -v bpftool >/dev/null 2>&1; then \
		echo "bpftool not found in PATH. Installing..."; \
		$(MAKE) install-bpftool-auto; \
	fi
	@# Check for Ubuntu wrapper script issues
	@ver="$$(bpftool version 2>&1 || true)"; \
	if echo "$$ver" | grep -q "not found for kernel"; then \
		echo "WARNING: bpftool wrapper detected (mismatched linux-tools for kernel $(UNAME_R))."; \
		echo "Either install matching linux-tools-$(UNAME_R) or use custom-built bpftool in /usr/local/bin"; \
		if [ -x /usr/local/bin/bpftool ]; then \
			echo "Using /usr/local/bin/bpftool instead"; \
		else \
			echo "Building bpftool from source..."; \
			$(MAKE) install-bpftool-source; \
		fi; \
	fi
	@if ! bpftool version 2>&1 | grep -q "bpftool v"; then \
		echo "ERROR: bpftool not working properly"; \
		exit 1; \
	fi
	@echo "bpftool OK: $$(command -v bpftool)"; bpftool version

.PHONY: install-bpftool
install-bpftool: check-bpftool

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
	@rm -f internal/ebpf/generated/*.go internal/ebpf/generated/*.o 2>/dev/null || true
	@# Keep vmlinux.h during clean (expensive to regenerate)
	@echo "Note: vmlinux.h preserved. Use 'make distclean' to remove everything."

.PHONY: distclean
distclean: clean
	@echo "Deep cleaning (including vmlinux.h)..."
	@rm -f internal/ebpf/generated/vmlinux.h
	@rm -rf internal/ebpf/generated

.PHONY: lint
lint:
	@echo "Running lint for $(NAME)..."
	@../bin/golangci-lint run -v --config ../.golangci.yml --timeout 10m ./... && \
	  ../bin/revive -config ../revive.toml -formatter friendly ./...

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
	@echo "  check-bpftool   - Verify bpftool is working correctly"
	@echo "  install-bpftool - Check and install bpftool if needed"
	@echo "  vmlinux         - Generate vmlinux.h header (checks BTF support)"
	@echo "  ebpf-bindings   - Generate eBPF Go bindings for multiple architectures"
	@echo "  generate        - Generate vmlinux.h and eBPF bindings"
	@echo "  build           - Build the sidecar binary"
	@echo "  clean           - Clean build artifacts (preserves vmlinux.h)"
	@echo "  distclean       - Deep clean including vmlinux.h"
	@echo "  lint            - Run linting checks"
	@echo "  test            - Run unit tests"
	@echo "  test-ebpf       - Run eBPF tests (requires root)"
	@echo "  docker          - Build Docker image"
	@echo "  run             - Build and run the sidecar"
	@echo "  help            - Show this help message"
	@echo ""
	@echo "Environment:"
	@echo "  Kernel: $(UNAME_R)"
	@echo "  Arch:   $(ARCH)"
	@echo "  PATH:   $(PATH)"