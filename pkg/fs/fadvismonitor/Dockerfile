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

# This Dockerfile is only for local development and testing purposes.

# Dockerfile: Multi-stage builder for eBPF object compilation and benchmark execution

# Dockerfile for eBPF benchmark (multi-arch, makefile-driven)

# syntax=docker/dockerfile:1
ARG BASE_IMAGE=golang:1.24-bullseye
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS builder

# === Install eBPF compilation and generation tools ===
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang llvm libelf-dev bpftool make git && \
    rm -rf /var/lib/apt/lists/*

# === Install bpf2go ===
ARG BPF2GO_VERSION=v0.13.0
RUN go install github.com/cilium/ebpf/cmd/bpf2go@${BPF2GO_VERSION}

WORKDIR /workspace

# Copy source code and pre-download modules to speed up subsequent steps
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Execute eBPF generation: vmlinux.h + eBPF objects + Go loader
WORKDIR /workspace/pkg/fs/fadvisemonitor
RUN make prepare-bpf-headers generate-bpf

# === Benchmark stage ===
FROM builder AS bench
WORKDIR /workspace/pkg/fs/fadvisemonitor

# Default entrypoint: run benchmark report
CMD ["make", "bpf-benchmark"]