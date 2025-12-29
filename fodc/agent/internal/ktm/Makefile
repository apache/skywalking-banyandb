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
UNAME_R := $(shell uname -r)
ARCH := $(shell uname -m)
CLANG ?= clang

.PHONY: all
all: generate

.PHONY: generate
generate: check-deps vmlinux ebpf-bindings

# Quick dependency check - fails fast if critical tools are missing
.PHONY: check-deps
check-deps:
	@echo "Checking build dependencies..."
	@# Check clang (required for eBPF compilation)
	@if ! command -v clang >/dev/null 2>&1; then \
		echo "ERROR: clang not found. Install it first:"; \
		echo "  Ubuntu/Debian: sudo apt-get install -y clang llvm"; \
		echo "  RedHat/Fedora: sudo dnf install -y clang llvm"; \
		exit 1; \
	fi
	@# Check llvm-strip (required for eBPF)
	@if ! command -v llvm-strip >/dev/null 2>&1; then \
		echo "ERROR: llvm-strip not found. Install it with clang/llvm package."; \
		exit 1; \
	fi
	@# Check BTF support (required for vmlinux.h)
	@if [ ! -r "/sys/kernel/btf/vmlinux" ]; then \
		echo "ERROR: /sys/kernel/btf/vmlinux not readable."; \
		echo "Your kernel may not have CONFIG_DEBUG_INFO_BTF=y."; \
		echo "Try: sudo apt install linux-image-$$(uname -r) linux-modules-extra-$$(uname -r)"; \
		exit 1; \
	fi
	@echo "✓ All critical dependencies available"

# eBPF compilation flags
BPF_CFLAGS := -O2 -g -Wall -Werror -D__TARGET_ARCH_$(ARCH)

# Check for required tools
LLVM_STRIP := $(shell command -v llvm-strip 2> /dev/null)
BPF2GO := $(shell command -v bpf2go 2> /dev/null)

.PHONY: ebpf-bindings
ebpf-bindings:
	@echo "Generating eBPF Go bindings..."
	@mkdir -p iomonitor/ebpf/generated
	@echo "Building for amd64..."
	@(cd iomonitor/ebpf/generated && \
		$(GO) run github.com/cilium/ebpf/cmd/bpf2go \
			-cc $(CLANG) \
			-cflags "$(BPF_CFLAGS)" \
			-target amd64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type shrink_counters_t \
			-type reclaim_counters_t \
			-type cache_stats_t \
			-type read_latency_stats_t \
			Iomonitor ../programs/iomonitor.c -- -I.)
	@echo "Building for arm64..."
	@(cd iomonitor/ebpf/generated && \
		$(GO) run github.com/cilium/ebpf/cmd/bpf2go \
			-cc $(CLANG) \
			-cflags "$(BPF_CFLAGS)" \
			-target arm64 \
			-go-package generated \
			-type fadvise_stats_t \
			-type fadvise_args_t \
			-type shrink_counters_t \
			-type reclaim_counters_t \
			-type cache_stats_t \
			-type read_latency_stats_t \
			Iomonitor ../programs/iomonitor.c -- -I.)
	@echo "✓ eBPF bindings generated successfully"

.PHONY: iomonitor-ebpf
iomonitor-ebpf: vmlinux ebpf-bindings
	@echo "iomonitor eBPF objects and bindings generated"

.PHONY: vmlinux
vmlinux: ensure-bpftool
	@echo "Generating vmlinux.h..."
	@mkdir -p iomonitor/ebpf/generated
	@bpftool btf dump file /sys/kernel/btf/vmlinux format c > iomonitor/ebpf/generated/vmlinux.h
	@test -s iomonitor/ebpf/generated/vmlinux.h || { echo "ERROR: vmlinux.h generation failed"; exit 1; }
	@echo "✓ vmlinux.h generated successfully"

# Ensure bpftool is available (install if needed)
.PHONY: ensure-bpftool
ensure-bpftool:
	@if ! command -v bpftool >/dev/null 2>&1; then \
		echo "bpftool not found, attempting to install..."; \
		$(MAKE) install-bpftool; \
	fi
	@# Verify bpftool works
	@if ! bpftool version >/dev/null 2>&1; then \
		echo "ERROR: bpftool not working properly"; \
		exit 1; \
	fi

# Install bpftool (tries package manager first, then source)
.PHONY: install-bpftool
install-bpftool:
	@echo "Installing bpftool..."
	@if [ -f /etc/debian_version ]; then \
		echo "Debian/Ubuntu detected"; \
		sudo apt-get update && sudo apt-get install -y linux-tools-common linux-tools-generic || \
		sudo apt-get install -y linux-tools-$$(uname -r) || \
		$(MAKE) install-bpftool-from-source; \
	elif [ -f /etc/redhat-release ]; then \
		echo "RedHat/Fedora detected"; \
		sudo dnf install -y bpftool || sudo yum install -y bpftool || \
		$(MAKE) install-bpftool-from-source; \
	else \
		echo "Unknown distro, building from source..."; \
		$(MAKE) install-bpftool-from-source; \
	fi
	@echo "✓ bpftool installed: $$(command -v bpftool)"

.PHONY: install-bpftool-from-source
install-bpftool-from-source:
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

# Install all dependencies (for first-time setup)
.PHONY: install-deps
install-deps:
	@echo "Installing all eBPF build dependencies..."
	@if [ -f /etc/debian_version ]; then \
		sudo apt-get update && \
		sudo apt-get install -y clang llvm linux-tools-common linux-tools-generic; \
	elif [ -f /etc/redhat-release ]; then \
		sudo dnf install -y clang llvm bpftool kernel-devel || \
		sudo yum install -y clang llvm bpftool kernel-devel; \
	else \
		echo "Unknown distro. Please install: clang, llvm, bpftool manually."; \
		exit 1; \
	fi
	@echo "✓ All dependencies installed"

# Note: Binary building is handled by fodc/agent/Makefile
# This Makefile only handles eBPF generation

.PHONY: clean
clean:
	@echo "Cleaning KTM eBPF artifacts..."
	@rm -f iomonitor/ebpf/generated/*.go iomonitor/ebpf/generated/*.o 2>/dev/null || true
	@# Keep vmlinux.h during clean (expensive to regenerate)
	@echo "Note: vmlinux.h preserved. Use 'make distclean' to remove everything."

.PHONY: distclean
distclean: clean
	@echo "Deep cleaning (including vmlinux.h)..."
	@rm -f iomonitor/ebpf/generated/vmlinux.h
	@rm -rf iomonitor/ebpf/generated

.PHONY: lint
lint:
	@echo "Running lint for $(NAME)..."
	@../bin/golangci-lint run -v --config ../.golangci.yml --timeout 10m ./... && \
	  ../bin/revive -config ../revive.toml -formatter friendly ./...

.PHONY: format
format:
	@echo "Running format for $(NAME)..."
	@../bin/golangci-lint run --fix --timeout 10m -c ../.golangci-format.yml ./...

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
	$(GO) test -v -tags=ebpf ./iomonitor/ebpf/...

# Docker and run targets are handled by fodc/agent/Makefile


.PHONY: help
help:
	@echo "KTM eBPF Generation Makefile"
	@echo ""
	@echo "Quick Start:"
	@echo "  make install-deps  # First time setup (installs clang, llvm, bpftool)"
	@echo "  make generate      # Generate eBPF bindings"
	@echo ""
	@echo "Available targets:"
	@echo "  all              - Generate eBPF bindings (default)"
	@echo "  generate         - Generate vmlinux.h and eBPF bindings"
	@echo "  check-deps       - Verify all required tools are available"
	@echo "  install-deps     - Install all eBPF dependencies"
	@echo "  clean            - Clean eBPF artifacts (preserves vmlinux.h)"
	@echo "  distclean        - Deep clean including vmlinux.h"
	@echo "  test             - Run unit tests"
	@echo "  test-ebpf        - Run eBPF tests (requires root)"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Dependencies:"
	@echo "  Required: clang, llvm-strip, bpftool, BTF-enabled kernel"
	@echo "  Auto-installed: bpf2go (via 'go run')"
	@echo ""
	@echo "Environment:"
	@echo "  Kernel: $(UNAME_R)"
	@echo "  Arch:   $(ARCH)"
