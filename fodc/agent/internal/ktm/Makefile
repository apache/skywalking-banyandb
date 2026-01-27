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

# Quick dependency check - auto-install in CI, fail fast locally
.PHONY: check-deps
check-deps:
	@echo "Checking build dependencies..."
	@# Check if running in CI
	@if [ -n "$${CI:-}" ] || [ -n "$${GITHUB_ACTIONS:-}" ]; then \
		echo "CI environment detected, auto-installing dependencies..."; \
		$(MAKE) install-deps; \
	fi
	@# Check clang (required for eBPF compilation)
	@if ! command -v clang >/dev/null 2>&1; then \
		echo "ERROR: clang not found. Install it first:"; \
		echo "  Run: make install-deps"; \
		echo "  Or manually: sudo apt-get install -y clang llvm"; \
		exit 1; \
	fi
	@# Check llvm-strip (required for eBPF)
	@if ! command -v llvm-strip >/dev/null 2>&1; then \
		echo "ERROR: llvm-strip not found."; \
		echo "  Run: make install-deps"; \
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
	@$(MAKE) add-license
	@echo "✓ eBPF bindings generated successfully"

.PHONY: add-license
add-license:
	@echo "Ensuring ASF license headers in generated Go files..."
	@shopt -s nullglob
	@files=(iomonitor/ebpf/generated/*.go)
	@if [ "$${#files[@]}" -eq 0 ]; then \
		echo "No generated Go files found; skipping license header injection."; \
		exit 0; \
	fi
	@for file in "$${files[@]}"; do \
		if ! grep -q "Licensed to Apache Software Foundation" "$$file"; then \
			tmp_file="$${file}.tmp"; \
			printf '%s\n' \
				'// Licensed to Apache Software Foundation (ASF) under one or more contributor' \
				'// license agreements. See the NOTICE file distributed with' \
				'// this work for additional information regarding copyright' \
				'// ownership. Apache Software Foundation (ASF) licenses this file to you under' \
				'// the Apache License, Version 2.0 (the "License"); you may' \
				'// not use this file except in compliance with the License.' \
				'// You may obtain a copy of the License at' \
				'//' \
				'//     http://www.apache.org/licenses/LICENSE-2.0' \
				'//' \
				'// Unless required by applicable law or agreed to in writing,' \
				'// software distributed under the License is distributed on an' \
				'// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY' \
				'// KIND, either express or implied.  See the License for the' \
				'// specific language governing permissions and limitations' \
				'// under the License.' \
				> "$$tmp_file"; \
			cat "$$file" >> "$$tmp_file"; \
			mv "$$tmp_file" "$$file"; \
		fi; \
	done
	@echo "✓ License headers ensured"

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

# Ensure bpftool is available
.PHONY: ensure-bpftool
ensure-bpftool:
	@if ! command -v bpftool >/dev/null 2>&1; then \
		echo "ERROR: bpftool not found. Please install it:"; \
		echo "  Ubuntu/Debian: sudo apt-get install -y linux-tools-common linux-tools-generic"; \
		echo "  RedHat/Fedora: sudo dnf install -y bpftool"; \
		echo "  Or run: make install-deps"; \
		exit 1; \
	fi
	@bpftool version >/dev/null 2>&1 || { echo "ERROR: bpftool not working"; exit 1; }

# Install all dependencies (first-time setup)
.PHONY: install-deps
install-deps:
	@echo "Installing eBPF build dependencies..."
	@if [ -f /etc/debian_version ]; then \
		sudo apt-get update && \
		sudo apt-get install -y clang llvm libbpf-dev linux-tools-common linux-tools-generic; \
	elif [ -f /etc/redhat-release ]; then \
		sudo dnf install -y clang llvm libbpf-devel bpftool kernel-devel || \
		sudo yum install -y clang llvm libbpf-devel bpftool kernel-devel; \
	else \
		echo "ERROR: Unknown distro. Install manually: clang, llvm, libbpf-dev, bpftool"; \
		exit 1; \
	fi
	@echo "✓ Dependencies installed"

# Note: Binary building is handled by fodc/agent/Makefile
# This Makefile only handles eBPF generation

.PHONY: clean
clean:
	@echo "Cleaning eBPF artifacts..."
	@rm -f iomonitor/ebpf/generated/*.go iomonitor/ebpf/generated/*.o 2>/dev/null || true
	@echo "Note: vmlinux.h preserved (use 'make distclean' to remove)"

.PHONY: distclean
distclean: clean
	@echo "Deep clean (including vmlinux.h)..."
	@rm -rf iomonitor/ebpf/generated

.PHONY: lint
lint:
	$(GO) vet ./...

.PHONY: test
test:
	$(GO) test -v ./...

.PHONY: test-ebpf
test-ebpf:
	@if [ "$$(id -u)" != "0" ]; then \
		echo "ERROR: eBPF tests require root. Run: sudo make test-ebpf"; \
		exit 1; \
	fi
	$(GO) test -v -tags=ebpf ./iomonitor/ebpf/...

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
